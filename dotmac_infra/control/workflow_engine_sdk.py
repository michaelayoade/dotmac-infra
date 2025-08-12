"""
Workflow Engine SDK

Complex business process orchestration for the Operations Plane.
Supports multi-step workflows, saga patterns, compensation logic, and parallel execution.

Features:
- Multi-step workflow orchestration
- Saga pattern with compensation
- Parallel and sequential execution
- Conditional branching and loops
- Workflow state management
- SDK-to-SDK composition across all planes
- DRY patterns for cross-cutting concerns
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from uuid import uuid4
from enum import Enum

from .base_operations_sdk import BaseOperationsSDK, OperationPriority
from .job_queue_sdk import JobQueueSDK, JobType
from .task_scheduler_sdk import TaskSchedulerSDK
from ..base_sdk import SecurityContext
from ...core.enums import Permission

logger = logging.getLogger(__name__)


class WorkflowStatus(Enum):
    """Workflow status enumeration"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    SUSPENDED = "suspended"


class StepStatus(Enum):
    """Workflow step status enumeration"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    COMPENSATED = "compensated"


class ExecutionMode(Enum):
    """Step execution mode"""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"


class WorkflowStep:
    """Workflow step definition"""
    
    def __init__(
        self,
        step_id: str,
        name: str,
        action_type: str,
        action_config: Dict[str, Any],
        execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL,
        depends_on: Optional[List[str]] = None,
        compensation_action: Optional[Dict[str, Any]] = None,
        condition: Optional[str] = None,
        retry_config: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.step_id = step_id
        self.name = name
        self.action_type = action_type
        self.action_config = action_config
        self.execution_mode = execution_mode
        self.depends_on = depends_on or []
        self.compensation_action = compensation_action
        self.condition = condition
        self.retry_config = retry_config or {"max_retries": 3, "retry_delay": 5}
        self.timeout_seconds = timeout_seconds or 300
        self.metadata = metadata or {}
        
        self.status = StepStatus.PENDING
        self.started_at = None
        self.completed_at = None
        self.error_message = None
        self.result = None
        self.retry_count = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert step to dictionary"""
        return {
            "step_id": self.step_id,
            "name": self.name,
            "action_type": self.action_type,
            "action_config": self.action_config,
            "execution_mode": self.execution_mode.value,
            "depends_on": self.depends_on,
            "compensation_action": self.compensation_action,
            "condition": self.condition,
            "retry_config": self.retry_config,
            "timeout_seconds": self.timeout_seconds,
            "metadata": self.metadata,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "result": self.result,
            "retry_count": self.retry_count
        }


class Workflow:
    """Workflow definition and execution context"""
    
    def __init__(
        self,
        workflow_id: str,
        name: str,
        description: str,
        steps: List[WorkflowStep],
        priority: OperationPriority = OperationPriority.NORMAL,
        timeout_seconds: Optional[int] = None,
        compensation_enabled: bool = True,
        parallel_execution: bool = False,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.workflow_id = workflow_id
        self.name = name
        self.description = description
        self.steps = {step.step_id: step for step in steps}
        self.step_order = [step.step_id for step in steps]
        self.priority = priority
        self.timeout_seconds = timeout_seconds or 1800  # 30 minutes default
        self.compensation_enabled = compensation_enabled
        self.parallel_execution = parallel_execution
        self.metadata = metadata or {}
        
        self.status = WorkflowStatus.PENDING
        self.created_at = datetime.now(timezone.utc)
        self.started_at = None
        self.completed_at = None
        self.error_message = None
        self.result = None
        self.compensation_steps = []
        self.context_data = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert workflow to dictionary"""
        return {
            "workflow_id": self.workflow_id,
            "name": self.name,
            "description": self.description,
            "steps": {step_id: step.to_dict() for step_id, step in self.steps.items()},
            "step_order": self.step_order,
            "priority": self.priority.value,
            "timeout_seconds": self.timeout_seconds,
            "compensation_enabled": self.compensation_enabled,
            "parallel_execution": self.parallel_execution,
            "metadata": self.metadata,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "result": self.result,
            "compensation_steps": self.compensation_steps,
            "context_data": self.context_data
        }


class WorkflowEngineSDK(BaseOperationsSDK):
    """
    Workflow Engine SDK for complex business process orchestration
    
    Provides multi-step workflow execution with saga patterns, compensation logic,
    parallel execution, and comprehensive state management using DRY principles
    and SDK-to-SDK composition.
    """
    
    def __init__(self, tenant_id: str, **kwargs):
        super().__init__(tenant_id, **kwargs)
        
        # Compose with other Operations SDKs
        self.job_queue_sdk = JobQueueSDK(tenant_id)
        self.task_scheduler_sdk = TaskSchedulerSDK(tenant_id)
        
        # Workflow configuration
        self.max_concurrent_workflows = kwargs.get('max_concurrent_workflows', 20)
        self.step_timeout_default = kwargs.get('step_timeout_default', 300)
        
        # Step action handlers registry
        self.step_handlers: Dict[str, Callable] = {}
        
        # Workflow engine control
        self.engine_running = False
        self.engine_task: Optional[asyncio.Task] = None
        self.running_workflows: Dict[str, asyncio.Task] = {}
    
    async def create_workflow(
        self,
        name: str,
        description: str,
        steps_config: List[Dict[str, Any]],
        context: SecurityContext,
        priority: OperationPriority = OperationPriority.NORMAL,
        timeout_seconds: Optional[int] = None,
        compensation_enabled: bool = True,
        parallel_execution: bool = False,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a new workflow definition
        Uses DRY patterns for validation, caching, and events
        """
        try:
            # Validate permissions
            if not await self.validate_operation_permissions(
                "workflow_engine.create",
                [Permission.WORKFLOW_CREATE],
                context
            ):
                raise PermissionError("Insufficient permissions to create workflow")
            
            # Create workflow
            workflow_id = str(uuid4())
            
            # Convert step configurations to WorkflowStep objects
            steps = []
            for i, step_config in enumerate(steps_config):
                step = WorkflowStep(
                    step_id=step_config.get("step_id", f"step_{i+1}"),
                    name=step_config["name"],
                    action_type=step_config["action_type"],
                    action_config=step_config["action_config"],
                    execution_mode=ExecutionMode(step_config.get("execution_mode", "sequential")),
                    depends_on=step_config.get("depends_on", []),
                    compensation_action=step_config.get("compensation_action"),
                    condition=step_config.get("condition"),
                    retry_config=step_config.get("retry_config"),
                    timeout_seconds=step_config.get("timeout_seconds"),
                    metadata=step_config.get("metadata", {})
                )
                steps.append(step)
            
            # Create workflow object
            workflow = Workflow(
                workflow_id=workflow_id,
                name=name,
                description=description,
                steps=steps,
                priority=priority,
                timeout_seconds=timeout_seconds,
                compensation_enabled=compensation_enabled,
                parallel_execution=parallel_execution,
                metadata=metadata
            )
            
            # Store workflow in database
            await self.db_sdk.execute(
                """
                INSERT INTO operations_workflows (
                    workflow_id, tenant_id, name, description, steps_config,
                    priority, timeout_seconds, compensation_enabled, parallel_execution,
                    metadata, status, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    workflow.workflow_id, self.tenant_id, workflow.name,
                    workflow.description, json.dumps(workflow.to_dict()["steps"]),
                    workflow.priority.value, workflow.timeout_seconds,
                    workflow.compensation_enabled, workflow.parallel_execution,
                    json.dumps(workflow.metadata), workflow.status.value,
                    workflow.created_at
                )
            )
            
            # Cache workflow
            cache_key = f"workflow:{self.tenant_id}:{workflow_id}"
            await self.cache_sdk.set(cache_key, workflow.to_dict(), ttl=3600)
            
            # Publish workflow created event
            await self.event_sdk.publish(
                "workflow.created",
                {
                    "workflow_id": workflow_id,
                    "name": name,
                    "tenant_id": self.tenant_id,
                    "steps_count": len(steps),
                    "priority": priority.value
                }
            )
            
            # Log operation
            await self.observability_sdk.log_info(
                f"Workflow {workflow_id} ({name}) created with {len(steps)} steps",
                {"workflow_id": workflow_id, "name": name, "steps_count": len(steps)}
            )
            
            return workflow_id
            
        except Exception as e:
            logger.error(f"Failed to create workflow: {e}")
            raise
    
    async def execute_workflow(
        self,
        workflow_id: str,
        input_data: Dict[str, Any],
        context: SecurityContext
    ) -> str:
        """
        Execute a workflow with input data
        Returns execution ID for tracking
        """
        try:
            # Validate permissions
            if not await self.validate_operation_permissions(
                "workflow_engine.execute",
                [Permission.WORKFLOW_EXECUTE],
                context
            ):
                raise PermissionError("Insufficient permissions to execute workflow")
            
            # Get workflow definition
            workflow_data = await self.get_workflow(workflow_id, context)
            if not workflow_data:
                raise ValueError(f"Workflow {workflow_id} not found")
            
            # Create execution ID
            execution_id = str(uuid4())
            
            # Store workflow execution
            await self.db_sdk.execute(
                """
                INSERT INTO operations_workflow_executions (
                    execution_id, workflow_id, tenant_id, input_data, status,
                    created_at, started_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    execution_id, workflow_id, self.tenant_id,
                    json.dumps(input_data), WorkflowStatus.RUNNING.value,
                    datetime.now(timezone.utc), datetime.now(timezone.utc)
                )
            )
            
            # Start workflow execution
            if len(self.running_workflows) < self.max_concurrent_workflows:
                execution_task = asyncio.create_task(
                    self._execute_workflow_instance(workflow_data, execution_id, input_data, context)
                )
                self.running_workflows[execution_id] = execution_task
                
                # Clean up completed workflows
                execution_task.add_done_callback(
                    lambda t, eid=execution_id: self.running_workflows.pop(eid, None)
                )
            else:
                # Queue for later execution
                await self.job_queue_sdk.enqueue_job(
                    job_type=JobType.BULK_OPERATION,
                    payload={
                        "operation": "execute_workflow",
                        "workflow_id": workflow_id,
                        "execution_id": execution_id,
                        "input_data": input_data
                    },
                    context=context,
                    priority=OperationPriority(workflow_data["priority"])
                )
            
            # Publish execution started event
            await self.event_sdk.publish(
                "workflow.execution.started",
                {
                    "execution_id": execution_id,
                    "workflow_id": workflow_id,
                    "tenant_id": self.tenant_id,
                    "input_data": input_data
                }
            )
            
            return execution_id
            
        except Exception as e:
            logger.error(f"Failed to execute workflow {workflow_id}: {e}")
            raise
    
    async def _execute_workflow_instance(
        self,
        workflow_data: Dict[str, Any],
        execution_id: str,
        input_data: Dict[str, Any],
        context: SecurityContext
    ):
        """Execute a single workflow instance"""
        workflow_id = workflow_data["workflow_id"]
        
        try:
            # Initialize workflow execution context
            execution_context = {
                "execution_id": execution_id,
                "workflow_id": workflow_id,
                "input_data": input_data,
                "step_results": {},
                "context_data": input_data.copy()
            }
            
            # Execute steps based on execution mode
            if workflow_data.get("parallel_execution", False):
                await self._execute_steps_parallel(workflow_data, execution_context, context)
            else:
                await self._execute_steps_sequential(workflow_data, execution_context, context)
            
            # Complete workflow execution
            await self._complete_workflow_execution(
                execution_id, WorkflowStatus.COMPLETED, execution_context["step_results"]
            )
            
            # Publish completion event
            await self.event_sdk.publish(
                "workflow.execution.completed",
                {
                    "execution_id": execution_id,
                    "workflow_id": workflow_id,
                    "tenant_id": self.tenant_id,
                    "result": execution_context["step_results"]
                }
            )
            
            logger.info(f"Workflow execution {execution_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Workflow execution {execution_id} failed: {e}")
            
            # Handle failure with compensation if enabled
            if workflow_data.get("compensation_enabled", True):
                await self._compensate_workflow(workflow_data, execution_context, context)
            
            await self._complete_workflow_execution(
                execution_id, WorkflowStatus.FAILED, None, str(e)
            )
            
            # Publish failure event
            await self.event_sdk.publish(
                "workflow.execution.failed",
                {
                    "execution_id": execution_id,
                    "workflow_id": workflow_id,
                    "tenant_id": self.tenant_id,
                    "error": str(e)
                }
            )
    
    async def _execute_steps_sequential(
        self,
        workflow_data: Dict[str, Any],
        execution_context: Dict[str, Any],
        context: SecurityContext
    ):
        """Execute workflow steps sequentially"""
        steps = workflow_data["steps"]
        step_order = workflow_data["step_order"]
        
        for step_id in step_order:
            step_data = steps[step_id]
            
            # Check step condition if specified
            if step_data.get("condition") and not self._evaluate_condition(
                step_data["condition"], execution_context["context_data"]
            ):
                logger.info(f"Step {step_id} skipped due to condition")
                continue
            
            # Check dependencies
            if not await self._check_step_dependencies(step_data, execution_context):
                raise RuntimeError(f"Step {step_id} dependencies not satisfied")
            
            # Execute step
            step_result = await self._execute_step(step_data, execution_context, context)
            execution_context["step_results"][step_id] = step_result
            
            # Update context data with step result
            if isinstance(step_result, dict):
                execution_context["context_data"].update(step_result)
    
    async def _execute_steps_parallel(
        self,
        workflow_data: Dict[str, Any],
        execution_context: Dict[str, Any],
        context: SecurityContext
    ):
        """Execute workflow steps in parallel where possible"""
        steps = workflow_data["steps"]
        step_order = workflow_data["step_order"]
        
        # Build dependency graph
        self._build_dependency_graph(steps)
        
        # Execute steps in dependency order with parallelization
        executed_steps = set()
        
        while len(executed_steps) < len(step_order):
            # Find steps ready for execution
            ready_steps = []
            for step_id in step_order:
                if step_id not in executed_steps:
                    dependencies = steps[step_id].get("depends_on", [])
                    if all(dep in executed_steps for dep in dependencies):
                        ready_steps.append(step_id)
            
            if not ready_steps:
                raise RuntimeError("Circular dependency detected in workflow")
            
            # Execute ready steps in parallel
            tasks = []
            for step_id in ready_steps:
                step_data = steps[step_id]
                task = asyncio.create_task(
                    self._execute_step(step_data, execution_context, context)
                )
                tasks.append((step_id, task))
            
            # Wait for all tasks to complete
            for step_id, task in tasks:
                try:
                    step_result = await task
                    execution_context["step_results"][step_id] = step_result
                    executed_steps.add(step_id)
                    
                    # Update context data
                    if isinstance(step_result, dict):
                        execution_context["context_data"].update(step_result)
                        
                except Exception as e:
                    logger.error(f"Step {step_id} failed: {e}")
                    raise
    
    async def _execute_step(
        self,
        step_data: Dict[str, Any],
        execution_context: Dict[str, Any],
        context: SecurityContext
    ) -> Any:
        """Execute a single workflow step"""
        step_id = step_data["step_id"]
        action_type = step_data["action_type"]
        action_config = step_data["action_config"]
        
        # Update step status
        await self._update_step_status(
            execution_context["execution_id"], step_id, StepStatus.RUNNING
        )
        
        try:
            # Execute step based on action type
            if action_type == "job_queue":
                # Submit job to queue
                job_type = JobType(action_config["job_type"])
                payload = action_config.get("payload", {})
                # Merge context data into payload
                payload.update(execution_context["context_data"])
                
                job_id = await self.job_queue_sdk.enqueue_job(
                    job_type=job_type,
                    payload=payload,
                    context=context
                )
                
                # Wait for job completion (simplified)
                # In production, use proper job status polling
                await asyncio.sleep(5)
                return {"job_id": job_id, "status": "submitted"}
                
            elif action_type in self.step_handlers:
                # Execute custom handler
                handler = self.step_handlers[action_type]
                # Pass context data to handler
                enhanced_config = action_config.copy()
                enhanced_config["context_data"] = execution_context["context_data"]
                result = await handler(enhanced_config)
                
                await self._update_step_status(
                    execution_context["execution_id"], step_id, StepStatus.COMPLETED
                )
                return result
                
            else:
                raise ValueError(f"No handler for action type: {action_type}")
                
        except Exception as e:
            await self._update_step_status(
                execution_context["execution_id"], step_id, StepStatus.FAILED, str(e)
            )
            raise
    
    def _evaluate_condition(self, condition: str, context_data: Dict[str, Any]) -> bool:
        """Evaluate step condition (simplified implementation)"""
        # This is a simplified implementation
        # In production, use a proper expression evaluator
        try:
            # Simple variable substitution
            for key, value in context_data.items():
                condition = condition.replace(f"${key}", str(value))
            
            # Basic evaluation (unsafe - use proper evaluator in production)
            return eval(condition)
        except Exception:
            return False
    
    async def _check_step_dependencies(
        self,
        step_data: Dict[str, Any],
        execution_context: Dict[str, Any]
    ) -> bool:
        """Check if step dependencies are satisfied"""
        depends_on = step_data.get("depends_on", [])
        step_results = execution_context["step_results"]
        
        return all(dep_id in step_results for dep_id in depends_on)
    
    def _build_dependency_graph(self, steps: Dict[str, Any]) -> Dict[str, List[str]]:
        """Build step dependency graph"""
        graph = {}
        for step_id, step_data in steps.items():
            graph[step_id] = step_data.get("depends_on", [])
        return graph
    
    async def _compensate_workflow(
        self,
        workflow_data: Dict[str, Any],
        execution_context: Dict[str, Any],
        context: SecurityContext
    ):
        """Execute compensation actions for failed workflow"""
        logger.info(f"Starting compensation for workflow execution {execution_context['execution_id']}")
        
        # Execute compensation in reverse order
        step_results = execution_context["step_results"]
        steps = workflow_data["steps"]
        
        for step_id in reversed(list(step_results.keys())):
            step_data = steps[step_id]
            compensation_action = step_data.get("compensation_action")
            
            if compensation_action:
                try:
                    # Execute compensation action
                    if compensation_action["action_type"] in self.step_handlers:
                        handler = self.step_handlers[compensation_action["action_type"]]
                        await handler(compensation_action["action_config"])
                    
                    await self._update_step_status(
                        execution_context["execution_id"], step_id, StepStatus.COMPENSATED
                    )
                    
                except Exception as e:
                    logger.error(f"Compensation failed for step {step_id}: {e}")
    
    async def _update_step_status(
        self,
        execution_id: str,
        step_id: str,
        status: StepStatus,
        error_message: Optional[str] = None
    ):
        """Update step status in database"""
        await self.db_sdk.execute(
            """
            INSERT INTO operations_workflow_step_status (
                execution_id, step_id, tenant_id, status, updated_at, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (execution_id, step_id) DO UPDATE SET
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at,
                error_message = EXCLUDED.error_message
            """,
            (execution_id, step_id, self.tenant_id, status.value, 
             datetime.now(timezone.utc), error_message)
        )
    
    async def _complete_workflow_execution(
        self,
        execution_id: str,
        status: WorkflowStatus,
        result: Optional[Any] = None,
        error_message: Optional[str] = None
    ):
        """Complete workflow execution"""
        await self.db_sdk.execute(
            """
            UPDATE operations_workflow_executions
            SET status = %s, completed_at = %s, result = %s, error_message = %s
            WHERE execution_id = %s AND tenant_id = %s
            """,
            (status.value, datetime.now(timezone.utc), json.dumps(result) if result else None,
             error_message, execution_id, self.tenant_id)
        )
    
    async def get_workflow(
        self,
        workflow_id: str,
        context: SecurityContext
    ) -> Optional[Dict[str, Any]]:
        """Get workflow definition with caching (DRY pattern)"""
        try:
            # Check cache first
            cache_key = f"workflow:{self.tenant_id}:{workflow_id}"
            cached_workflow = await self.cache_sdk.get(cache_key)
            
            if cached_workflow:
                return cached_workflow
            
            # Fallback to database
            workflow_record = await self.db_sdk.query_one(
                """
                SELECT workflow_id, name, description, steps_config, priority,
                       timeout_seconds, compensation_enabled, parallel_execution,
                       metadata, status, created_at
                FROM operations_workflows
                WHERE workflow_id = %s AND tenant_id = %s
                """,
                (workflow_id, self.tenant_id)
            )
            
            if workflow_record:
                workflow_data = dict(workflow_record)
                workflow_data['steps'] = json.loads(workflow_data['steps_config'])
                workflow_data['step_order'] = list(workflow_data['steps'].keys())
                workflow_data['metadata'] = json.loads(workflow_data['metadata']) if workflow_data['metadata'] else {}
                
                # Update cache
                await self.cache_sdk.set(cache_key, workflow_data, ttl=3600)
                return workflow_data
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get workflow {workflow_id}: {e}")
            raise
    
    async def register_step_handler(
        self,
        action_type: str,
        handler_func: Callable
    ):
        """Register a step handler function"""
        self.step_handlers[action_type] = handler_func
        logger.info(f"Registered handler for action type: {action_type}")
    
    async def health_check(self) -> Dict[str, Any]:
        """Health check for Workflow Engine SDK"""
        try:
            # Test database connectivity
            await self.db_sdk.query_one("SELECT 1")
            
            return {
                "status": "healthy",
                "sdk": "WorkflowEngineSDK",
                "tenant_id": self.tenant_id,
                "engine_running": self.engine_running,
                "running_workflows": len(self.running_workflows),
                "registered_handlers": len(self.step_handlers),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "sdk": "WorkflowEngineSDK",
                "tenant_id": self.tenant_id,
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    def _get_sdk_capabilities(self) -> List[str]:
        """Get Workflow Engine SDK capabilities"""
        return [
            "workflow_orchestration",
            "saga_patterns",
            "compensation_logic",
            "parallel_execution",
            "conditional_branching",
            "step_dependencies",
            "workflow_monitoring"
        ]
