"""
Automation Engine SDK - Operations Plane

Intelligent automation and orchestration for business processes.
Supports rule-based automation, triggers, conditions, and cross-plane integration.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable
from uuid import uuid4
from enum import Enum

from .base_operations_sdk import BaseOperationsSDK, OperationPriority
from .workflow_engine_sdk import WorkflowEngineSDK
from .task_scheduler_sdk import TaskSchedulerSDK
from ..base_sdk import SecurityContext
from ...core.enums import Permission

logger = logging.getLogger(__name__)


class TriggerType(Enum):
    EVENT = "event"
    SCHEDULE = "schedule"
    CONDITION = "condition"
    WEBHOOK = "webhook"
    MANUAL = "manual"


class ActionType(Enum):
    WORKFLOW = "workflow"
    TASK = "task"
    NOTIFICATION = "notification"
    API_CALL = "api_call"
    SCRIPT = "script"


class AutomationStatus(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PAUSED = "paused"
    ERROR = "error"


class ExecutionStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AutomationRule:
    """Automation rule definition"""

    def __init__(
        self,
        rule_id: str,
        name: str,
        description: str,
        trigger_config: Dict,
        condition_config: Dict,
        actions_config: List[Dict],
        priority: OperationPriority = OperationPriority.NORMAL,
    ):
        self.rule_id = rule_id
        self.name = name
        self.description = description
        self.trigger_config = trigger_config
        self.condition_config = condition_config
        self.actions_config = actions_config
        self.priority = priority
        self.status = AutomationStatus.ACTIVE
        self.created_at = datetime.now(timezone.utc)
        self.last_executed = None
        self.execution_count = 0


class AutomationEngineSDK(BaseOperationsSDK):
    """Automation Engine SDK for intelligent business process automation"""

    def __init__(self, tenant_id: str, **kwargs):
        super().__init__(tenant_id, **kwargs)

        # Compose with other Operations SDKs
        self.workflow_engine = WorkflowEngineSDK(tenant_id)
        self.task_scheduler = TaskSchedulerSDK(tenant_id)

        # Automation configuration
        self.max_concurrent_executions = kwargs.get("max_concurrent_executions", 10)
        self.default_timeout = kwargs.get("default_timeout", 300)

        # Runtime state
        self.active_rules: Dict[str, AutomationRule] = {}
        self.running_executions: Dict[str, Dict] = {}
        self.trigger_handlers: Dict[str, Callable] = {}
        self.condition_evaluators: Dict[str, Callable] = {}
        self.action_executors: Dict[str, Callable] = {}

        # Register default handlers
        self._register_default_handlers()

    def _register_default_handlers(self):
        """Register default trigger handlers and action executors"""
        self.trigger_handlers.update(
            {
                "event": self._handle_event_trigger,
                "schedule": self._handle_schedule_trigger,
                "condition": self._handle_condition_trigger,
                "webhook": self._handle_webhook_trigger,
            }
        )

        self.action_executors.update(
            {
                "workflow": self._execute_workflow_action,
                "task": self._execute_task_action,
                "notification": self._execute_notification_action,
                "api_call": self._execute_api_call_action,
            }
        )

    async def create_automation_rule(
        self,
        name: str,
        description: str,
        trigger_config: Dict,
        condition_config: Dict,
        actions_config: List[Dict],
        context: SecurityContext,
        priority: OperationPriority = OperationPriority.NORMAL,
    ) -> str:
        """Create automation rule with DRY validation and caching"""
        try:
            if not await self.validate_operation_permissions(
                "automation.create", [Permission.AUTOMATION_CREATE], context
            ):
                raise PermissionError("Insufficient permissions")

            rule_id = str(uuid4())

            # Create automation rule
            rule = AutomationRule(
                rule_id=rule_id,
                name=name,
                description=description,
                trigger_config=trigger_config,
                condition_config=condition_config,
                actions_config=actions_config,
                priority=priority,
            )

            # Store in database
            await self.db_sdk.execute(
                """INSERT INTO operations_automation_rules 
                   (rule_id, tenant_id, name, description, trigger_config, condition_config, 
                    actions_config, priority, status, created_at) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    rule_id,
                    self.tenant_id,
                    name,
                    description,
                    json.dumps(trigger_config),
                    json.dumps(condition_config),
                    json.dumps(actions_config),
                    priority.value,
                    rule.status.value,
                    rule.created_at,
                ),
            )

            # Cache and activate rule
            self.active_rules[rule_id] = rule
            await self.cache_sdk.set(
                f"automation_rule:{self.tenant_id}:{rule_id}", rule.__dict__, ttl=3600
            )

            # Set up trigger if needed
            await self._setup_trigger(rule)

            # Publish event
            await self.event_sdk.publish(
                "automation.rule.created",
                {
                    "rule_id": rule_id,
                    "name": name,
                    "trigger_type": trigger_config.get("type"),
                },
            )

            await self.observability_sdk.log_info(
                f"Automation rule {rule_id} ({name}) created"
            )

            return rule_id

        except Exception as e:
            logger.error(f"Failed to create automation rule: {e}")
            raise

    async def _setup_trigger(self, rule: AutomationRule):
        """Set up trigger for automation rule"""
        trigger_type = rule.trigger_config.get("type")

        if trigger_type == "schedule":
            # Schedule recurring execution
            schedule_config = rule.trigger_config.get("schedule", {})
            await self.task_scheduler.schedule_recurring_task(
                task_id=f"automation_trigger_{rule.rule_id}",
                task_type="automation_trigger",
                payload={"rule_id": rule.rule_id},
                schedule_config=schedule_config,
                context=SecurityContext(user_id="system", permissions=[], roles=[]),
            )

        elif trigger_type == "event":
            # Subscribe to event
            event_pattern = rule.trigger_config.get("event_pattern")
            if event_pattern:
                await self.event_sdk.subscribe(
                    event_pattern,
                    lambda event_data, rule_id=rule.rule_id: self._handle_event_trigger(
                        rule_id, event_data
                    ),
                )

    async def execute_automation_rule(
        self, rule_id: str, trigger_data: Dict, context: SecurityContext
    ) -> str:
        """Execute automation rule"""
        try:
            if not await self.validate_operation_permissions(
                "automation.execute", [Permission.AUTOMATION_EXECUTE], context
            ):
                raise PermissionError("Insufficient permissions")

            # Get rule
            rule = await self._get_automation_rule(rule_id)
            if not rule or rule.status != AutomationStatus.ACTIVE:
                raise ValueError(f"Automation rule {rule_id} not found or inactive")

            # Check concurrent execution limit
            if len(self.running_executions) >= self.max_concurrent_executions:
                logger.warning(
                    f"Max concurrent executions reached, queuing rule {rule_id}"
                )
                # Queue for later execution
                await self.job_queue_sdk.enqueue_job(
                    job_type="automation_execution",
                    payload={"rule_id": rule_id, "trigger_data": trigger_data},
                    context=context,
                    priority=rule.priority,
                )
                return "queued"

            # Create execution
            execution_id = str(uuid4())
            execution = {
                "execution_id": execution_id,
                "rule_id": rule_id,
                "status": ExecutionStatus.PENDING.value,
                "trigger_data": trigger_data,
                "started_at": datetime.now(timezone.utc),
                "actions_completed": 0,
                "total_actions": len(rule.actions_config),
            }

            # Store execution
            await self.db_sdk.execute(
                """INSERT INTO operations_automation_executions 
                   (execution_id, rule_id, tenant_id, status, trigger_data, started_at) 
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (
                    execution_id,
                    rule_id,
                    self.tenant_id,
                    execution["status"],
                    json.dumps(trigger_data),
                    execution["started_at"],
                ),
            )

            self.running_executions[execution_id] = execution

            # Execute rule asynchronously
            asyncio.create_task(self._execute_rule_async(rule, execution, context))

            return execution_id

        except Exception as e:
            logger.error(f"Failed to execute automation rule {rule_id}: {e}")
            raise

    async def _execute_rule_async(
        self, rule: AutomationRule, execution: Dict, context: SecurityContext
    ):
        """Execute automation rule asynchronously"""
        try:
            execution["status"] = ExecutionStatus.RUNNING.value

            # Evaluate conditions
            if not await self._evaluate_conditions(
                rule.condition_config, execution["trigger_data"]
            ):
                execution["status"] = ExecutionStatus.COMPLETED.value
                execution["result"] = "Conditions not met, skipping execution"
                await self._complete_execution(execution)
                return

            # Execute actions sequentially
            action_results = []
            for i, action_config in enumerate(rule.actions_config):
                try:
                    result = await self._execute_action(
                        action_config, execution["trigger_data"], context
                    )
                    action_results.append(
                        {"action_index": i, "result": result, "status": "completed"}
                    )
                    execution["actions_completed"] += 1

                except Exception as e:
                    action_results.append(
                        {"action_index": i, "error": str(e), "status": "failed"}
                    )
                    logger.error(
                        f"Action {i} failed in execution {execution['execution_id']}: {e}"
                    )

                    # Check if should continue on error
                    if not action_config.get("continue_on_error", False):
                        raise

            # Complete execution
            execution["status"] = ExecutionStatus.COMPLETED.value
            execution["result"] = action_results
            execution["completed_at"] = datetime.now(timezone.utc)

            # Update rule statistics
            rule.last_executed = execution["completed_at"]
            rule.execution_count += 1

            await self._complete_execution(execution)

            # Publish completion event
            await self.event_sdk.publish(
                "automation.execution.completed",
                {
                    "execution_id": execution["execution_id"],
                    "rule_id": rule.rule_id,
                    "actions_completed": execution["actions_completed"],
                },
            )

        except Exception as e:
            execution["status"] = ExecutionStatus.FAILED.value
            execution["error"] = str(e)
            execution["completed_at"] = datetime.now(timezone.utc)

            await self._complete_execution(execution)

            # Publish failure event
            await self.event_sdk.publish(
                "automation.execution.failed",
                {
                    "execution_id": execution["execution_id"],
                    "rule_id": rule.rule_id,
                    "error": str(e),
                },
            )

        finally:
            # Clean up
            self.running_executions.pop(execution["execution_id"], None)

    async def _evaluate_conditions(
        self, condition_config: Dict, trigger_data: Dict
    ) -> bool:
        """Evaluate automation conditions"""
        if not condition_config:
            return True

        condition_type = condition_config.get("type", "always")

        if condition_type == "always":
            return True
        elif condition_type == "expression":
            # Simple expression evaluation
            expression = condition_config.get("expression", "true")
            try:
                # Replace variables with trigger data
                for key, value in trigger_data.items():
                    expression = expression.replace(f"${key}", str(value))
                return eval(expression)  # Use proper evaluator in production
            except Exception:
                return False
        elif condition_type == "custom":
            # Use custom evaluator
            evaluator_name = condition_config.get("evaluator")
            if evaluator_name in self.condition_evaluators:
                return await self.condition_evaluators[evaluator_name](
                    condition_config, trigger_data
                )

        return False

    async def _execute_action(
        self, action_config: Dict, trigger_data: Dict, context: SecurityContext
    ) -> Any:
        """Execute automation action"""
        action_type = action_config.get("type")

        if action_type in self.action_executors:
            executor = self.action_executors[action_type]
            return await executor(action_config, trigger_data, context)
        else:
            raise ValueError(f"Unknown action type: {action_type}")

    async def _execute_workflow_action(
        self, action_config: Dict, trigger_data: Dict, context: SecurityContext
    ):
        """Execute workflow action"""
        workflow_id = action_config.get("workflow_id")
        input_data = action_config.get("input_data", {})

        # Merge trigger data into input
        merged_input = {**input_data, **trigger_data}

        execution_id = await self.workflow_engine.execute_workflow(
            workflow_id=workflow_id, input_data=merged_input, context=context
        )

        return {"workflow_execution_id": execution_id}

    async def _execute_task_action(
        self, action_config: Dict, trigger_data: Dict, context: SecurityContext
    ):
        """Execute task action"""
        task_config = action_config.get("task_config", {})

        # Merge trigger data into task payload
        task_config["payload"] = {**task_config.get("payload", {}), **trigger_data}

        task_id = await self.task_scheduler.schedule_task(
            task_type=task_config.get("task_type", "generic"),
            payload=task_config["payload"],
            context=context,
            schedule_time=datetime.now(timezone.utc) + timedelta(seconds=1),
        )

        return {"task_id": task_id}

    async def _execute_notification_action(
        self, action_config: Dict, trigger_data: Dict, context: SecurityContext
    ):
        """Execute notification action"""
        # Use Communications SDK for notifications
        notification_config = action_config.get("notification_config", {})

        # Template message with trigger data
        message = notification_config.get("message", "")
        for key, value in trigger_data.items():
            message = message.replace(f"${key}", str(value))

        # Send notification (simplified)
        return {"notification_sent": True, "message": message}

    async def _execute_api_call_action(
        self, action_config: Dict, trigger_data: Dict, context: SecurityContext
    ):
        """Execute API call action"""
        import aiohttp

        url = action_config.get("url")
        method = action_config.get("method", "POST")
        headers = action_config.get("headers", {})
        payload = action_config.get("payload", {})

        # Merge trigger data into payload
        merged_payload = {**payload, **trigger_data}

        async with aiohttp.ClientSession() as session:
            async with session.request(
                method, url, json=merged_payload, headers=headers
            ) as response:
                result = await response.text()
                return {"status_code": response.status, "response": result}

    async def _handle_event_trigger(self, rule_id: str, event_data: Dict):
        """Handle event trigger"""
        try:
            await self.execute_automation_rule(
                rule_id=rule_id,
                trigger_data={"event_data": event_data, "trigger_type": "event"},
                context=SecurityContext(user_id="system", permissions=[], roles=[]),
            )
        except Exception as e:
            logger.error(f"Failed to handle event trigger for rule {rule_id}: {e}")

    async def _get_automation_rule(self, rule_id: str) -> Optional[AutomationRule]:
        """Get automation rule with caching"""
        # Check active rules first
        if rule_id in self.active_rules:
            return self.active_rules[rule_id]

        # Check cache
        cached_rule = await self.cache_sdk.get(
            f"automation_rule:{self.tenant_id}:{rule_id}"
        )
        if cached_rule:
            rule = AutomationRule(**cached_rule)
            self.active_rules[rule_id] = rule
            return rule

        # Load from database
        record = await self.db_sdk.query_one(
            "SELECT * FROM operations_automation_rules WHERE rule_id = %s AND tenant_id = %s",
            (rule_id, self.tenant_id),
        )

        if record:
            rule = AutomationRule(
                rule_id=record["rule_id"],
                name=record["name"],
                description=record["description"],
                trigger_config=json.loads(record["trigger_config"]),
                condition_config=json.loads(record["condition_config"]),
                actions_config=json.loads(record["actions_config"]),
                priority=OperationPriority(record["priority"]),
            )
            rule.status = AutomationStatus(record["status"])
            rule.created_at = record["created_at"]
            rule.last_executed = record.get("last_executed")
            rule.execution_count = record.get("execution_count", 0)

            self.active_rules[rule_id] = rule
            return rule

        return None

    async def _complete_execution(self, execution: Dict):
        """Complete automation execution"""
        await self.db_sdk.execute(
            """UPDATE operations_automation_executions 
               SET status = %s, completed_at = %s, result = %s, error_message = %s 
               WHERE execution_id = %s""",
            (
                execution["status"],
                execution.get("completed_at"),
                json.dumps(execution.get("result")),
                execution.get("error"),
                execution["execution_id"],
            ),
        )

    async def get_execution_status(
        self, execution_id: str, context: SecurityContext
    ) -> Optional[Dict]:
        """Get automation execution status"""
        # Check running executions
        if execution_id in self.running_executions:
            return self.running_executions[execution_id]

        # Load from database
        record = await self.db_sdk.query_one(
            "SELECT * FROM operations_automation_executions WHERE execution_id = %s AND tenant_id = %s",
            (execution_id, self.tenant_id),
        )

        return dict(record) if record else None

    async def pause_automation_rule(self, rule_id: str, context: SecurityContext):
        """Pause automation rule"""
        if not await self.validate_operation_permissions(
            "automation.pause", [Permission.AUTOMATION_MANAGE], context
        ):
            raise PermissionError("Insufficient permissions")

        rule = await self._get_automation_rule(rule_id)
        if rule:
            rule.status = AutomationStatus.PAUSED
            await self.db_sdk.execute(
                "UPDATE operations_automation_rules SET status = %s WHERE rule_id = %s",
                (rule.status.value, rule_id),
            )

    async def health_check(self) -> Dict[str, Any]:
        """Health check for Automation Engine SDK"""
        try:
            await self.db_sdk.query_one("SELECT 1")

            return {
                "status": "healthy",
                "sdk": "AutomationEngineSDK",
                "tenant_id": self.tenant_id,
                "active_rules": len(self.active_rules),
                "running_executions": len(self.running_executions),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        except Exception as e:
            return {
                "status": "unhealthy",
                "sdk": "AutomationEngineSDK",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    def _get_sdk_capabilities(self) -> List[str]:
        """Get Automation Engine SDK capabilities"""
        return [
            "rule_based_automation",
            "event_triggers",
            "scheduled_triggers",
            "conditional_execution",
            "workflow_integration",
            "cross_plane_automation",
        ]
