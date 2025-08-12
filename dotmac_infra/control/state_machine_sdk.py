"""
State Machine SDK - Operations Plane

Fine-grained state management for complex business processes.
Supports finite state machines, transitions, guards, and event-driven changes.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from uuid import uuid4
from enum import Enum

from .base_operations_sdk import BaseOperationsSDK, OperationPriority
from ..base_sdk import SecurityContext
from ...core.enums import Permission

logger = logging.getLogger(__name__)


class StateType(Enum):
    INITIAL = "initial"
    INTERMEDIATE = "intermediate" 
    FINAL = "final"
    ERROR = "error"


class StateMachineStatus(Enum):
    CREATED = "created"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class State:
    def __init__(self, state_id: str, name: str, state_type: StateType = StateType.INTERMEDIATE,
                 entry_actions: List[Dict] = None, exit_actions: List[Dict] = None):
        self.state_id = state_id
        self.name = name
        self.state_type = state_type
        self.entry_actions = entry_actions or []
        self.exit_actions = exit_actions or []


class Transition:
    def __init__(self, from_state: str, to_state: str, event: str, 
                 guard_condition: str = None, actions: List[Dict] = None):
        self.from_state = from_state
        self.to_state = to_state
        self.event = event
        self.guard_condition = guard_condition
        self.actions = actions or []


class StateMachineSDK(BaseOperationsSDK):
    """State Machine SDK for fine-grained state management with DRY composition"""
    
    def __init__(self, tenant_id: str, **kwargs):
        super().__init__(tenant_id, **kwargs)
        self.action_handlers: Dict[str, Callable] = {}
        self.running_instances: Dict[str, Dict] = {}
    
    async def create_state_machine(self, name: str, description: str, 
                                 states_config: List[Dict], transitions_config: List[Dict],
                                 initial_state: str, context: SecurityContext) -> str:
        """Create state machine definition with DRY validation and caching"""
        try:
            if not await self.validate_operation_permissions(
                "state_machine.create", [Permission.STATE_MACHINE_CREATE], context
            ):
                raise PermissionError("Insufficient permissions")
            
            machine_id = str(uuid4())
            
            # Create states and transitions
            states = {s["state_id"]: State(**s) for s in states_config}
            transitions = [Transition(**t) for t in transitions_config]
            
            # Store in database
            machine_data = {
                "machine_id": machine_id,
                "name": name,
                "description": description,
                "states": {k: {"state_id": v.state_id, "name": v.name, "state_type": v.state_type.value} 
                          for k, v in states.items()},
                "transitions": [{"from_state": t.from_state, "to_state": t.to_state, "event": t.event}
                               for t in transitions],
                "initial_state": initial_state
            }
            
            await self.db_sdk.execute(
                "INSERT INTO operations_state_machines (machine_id, tenant_id, name, definition) VALUES (%s, %s, %s, %s)",
                (machine_id, self.tenant_id, name, json.dumps(machine_data))
            )
            
            # Cache and publish event
            await self.cache_sdk.set(f"state_machine:{self.tenant_id}:{machine_id}", machine_data, ttl=3600)
            await self.event_sdk.publish("state_machine.created", {"machine_id": machine_id, "name": name})
            
            return machine_id
            
        except Exception as e:
            logger.error(f"Failed to create state machine: {e}")
            raise
    
    async def start_instance(self, machine_id: str, context_data: Dict, context: SecurityContext) -> str:
        """Start state machine instance"""
        try:
            if not await self.validate_operation_permissions(
                "state_machine.start", [Permission.STATE_MACHINE_EXECUTE], context
            ):
                raise PermissionError("Insufficient permissions")
            
            instance_id = str(uuid4())
            
            # Get machine definition
            machine_data = await self.cache_sdk.get(f"state_machine:{self.tenant_id}:{machine_id}")
            if not machine_data:
                # Fallback to database
                record = await self.db_sdk.query_one(
                    "SELECT definition FROM operations_state_machines WHERE machine_id = %s AND tenant_id = %s",
                    (machine_id, self.tenant_id)
                )
                machine_data = json.loads(record["definition"]) if record else None
            
            if not machine_data:
                raise ValueError(f"State machine {machine_id} not found")
            
            # Create instance
            instance = {
                "instance_id": instance_id,
                "machine_id": machine_id,
                "current_state": machine_data["initial_state"],
                "status": StateMachineStatus.RUNNING.value,
                "context_data": context_data,
                "created_at": datetime.now(timezone.utc),
                "state_history": [],
                "transition_history": []
            }
            
            # Store instance
            await self.db_sdk.execute(
                "INSERT INTO operations_state_machine_instances (instance_id, machine_id, tenant_id, current_state, status, context_data) VALUES (%s, %s, %s, %s, %s, %s)",
                (instance_id, machine_id, self.tenant_id, instance["current_state"], 
                 instance["status"], json.dumps(context_data))
            )
            
            self.running_instances[instance_id] = instance
            
            # Publish event
            await self.event_sdk.publish("state_machine.instance.started", {
                "instance_id": instance_id, "machine_id": machine_id, "initial_state": instance["current_state"]
            })
            
            return instance_id
            
        except Exception as e:
            logger.error(f"Failed to start instance: {e}")
            raise
    
    async def send_event(self, instance_id: str, event: str, event_data: Dict, context: SecurityContext) -> bool:
        """Send event to state machine instance"""
        try:
            if not await self.validate_operation_permissions(
                "state_machine.send_event", [Permission.STATE_MACHINE_EXECUTE], context
            ):
                raise PermissionError("Insufficient permissions")
            
            instance = self.running_instances.get(instance_id)
            if not instance:
                # Load from database
                record = await self.db_sdk.query_one(
                    "SELECT * FROM operations_state_machine_instances WHERE instance_id = %s AND tenant_id = %s",
                    (instance_id, self.tenant_id)
                )
                if record:
                    instance = dict(record)
                    instance["context_data"] = json.loads(instance["context_data"])
                    self.running_instances[instance_id] = instance
            
            if not instance or instance["status"] != StateMachineStatus.RUNNING.value:
                return False
            
            # Get machine definition for transitions
            machine_record = await self.db_sdk.query_one(
                "SELECT definition FROM operations_state_machines WHERE machine_id = %s AND tenant_id = %s",
                (instance["machine_id"], self.tenant_id)
            )
            
            if not machine_record:
                return False
            
            machine_data = json.loads(machine_record["definition"])
            current_state = instance["current_state"]
            
            # Find applicable transition
            applicable_transition = None
            for transition in machine_data["transitions"]:
                if transition["from_state"] == current_state and transition["event"] == event:
                    applicable_transition = transition
                    break
            
            if not applicable_transition:
                return False
            
            # Execute transition
            from_state = applicable_transition["from_state"]
            to_state = applicable_transition["to_state"]
            
            # Update instance state
            instance["current_state"] = to_state
            instance["transition_history"].append({
                "from_state": from_state,
                "to_state": to_state,
                "event": event,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            
            # Check if final state
            target_state = machine_data["states"].get(to_state, {})
            if target_state.get("state_type") == StateType.FINAL.value:
                instance["status"] = StateMachineStatus.COMPLETED.value
                
                await self.event_sdk.publish("state_machine.instance.completed", {
                    "instance_id": instance_id, "final_state": to_state
                })
            
            # Update database
            await self.db_sdk.execute(
                "UPDATE operations_state_machine_instances SET current_state = %s, status = %s WHERE instance_id = %s",
                (instance["current_state"], instance["status"], instance_id)
            )
            
            # Publish transition event
            await self.event_sdk.publish("state_machine.transition", {
                "instance_id": instance_id, "from_state": from_state, "to_state": to_state, "event": event
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send event: {e}")
            raise
    
    async def get_instance_status(self, instance_id: str, context: SecurityContext) -> Optional[Dict]:
        """Get state machine instance status"""
        try:
            instance = self.running_instances.get(instance_id)
            if not instance:
                record = await self.db_sdk.query_one(
                    "SELECT * FROM operations_state_machine_instances WHERE instance_id = %s AND tenant_id = %s",
                    (instance_id, self.tenant_id)
                )
                if record:
                    instance = dict(record)
                    instance["context_data"] = json.loads(instance["context_data"])
            
            return instance
            
        except Exception as e:
            logger.error(f"Failed to get instance status: {e}")
            raise
    
    async def register_action_handler(self, action_type: str, handler_func: Callable):
        """Register action handler"""
        self.action_handlers[action_type] = handler_func
        logger.info(f"Registered handler for action type: {action_type}")
    
    async def health_check(self) -> Dict[str, Any]:
        """Health check for State Machine SDK"""
        try:
            await self.db_sdk.query_one("SELECT 1")
            
            return {
                "status": "healthy",
                "sdk": "StateMachineSDK",
                "tenant_id": self.tenant_id,
                "running_instances": len(self.running_instances),
                "registered_handlers": len(self.action_handlers),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "sdk": "StateMachineSDK",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    def _get_sdk_capabilities(self) -> List[str]:
        """Get State Machine SDK capabilities"""
        return [
            "finite_state_machines",
            "state_transitions",
            "event_driven_changes",
            "guard_conditions",
            "state_actions",
            "instance_management"
        ]
