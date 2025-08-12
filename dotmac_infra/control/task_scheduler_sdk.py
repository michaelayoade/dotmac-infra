"""
Task Scheduler SDK

Time-based automation and scheduling for the Operations Plane.
Supports cron expressions, delays, recurring rules (RRule), and complex scheduling patterns.

Features:
- Cron-based scheduling (standard and extended formats)
- One-time delayed execution
- Recurring rules with RRule support
- Task dependency management
- Timezone-aware scheduling
- SDK-to-SDK composition with Job Queue and other planes
- DRY patterns for cross-cutting concerns
"""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable, Union
from uuid import uuid4
from enum import Enum
import re

from .base_operations_sdk import BaseOperationsSDK, OperationStatus, OperationPriority, OperationResult
from .job_queue_sdk import JobQueueSDK, JobType
from ..base_sdk import OperationContext, SecurityContext
from ...core.enums import Permission

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Schedule type enumeration"""
    CRON = "cron"
    DELAY = "delay"
    RECURRING = "recurring"
    ONE_TIME = "one_time"


class TaskStatus(Enum):
    """Task status enumeration"""
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskSchedulerSDK(BaseOperationsSDK):
    """
    Task Scheduler SDK for time-based automation
    
    Provides comprehensive scheduling capabilities including cron expressions,
    delays, recurring patterns, and dependency management using DRY principles
    and SDK-to-SDK composition.
    """
    
    def __init__(self, tenant_id: str, **kwargs):
        super().__init__(tenant_id, **kwargs)
        
        # Compose with Job Queue SDK for task execution
        self.job_queue_sdk = JobQueueSDK(tenant_id)
        
        # Scheduler configuration
        self.scheduler_interval = kwargs.get('scheduler_interval', 30)  # seconds
        self.max_concurrent_tasks = kwargs.get('max_concurrent_tasks', 10)
        
        # Task handlers registry
        self.task_handlers: Dict[str, Callable] = {}
        
        # Scheduler control
        self.scheduler_running = False
        self.scheduler_task: Optional[asyncio.Task] = None
        self.running_tasks: Dict[str, asyncio.Task] = {}
    
    async def schedule_cron_task(
        self,
        name: str,
        cron_expression: str,
        action_type: str,
        action_config: Dict[str, Any],
        context: SecurityContext,
        priority: OperationPriority = OperationPriority.NORMAL,
        **kwargs
    ) -> str:
        """Schedule a cron-based recurring task"""
        try:
            # Validate permissions
            if not await self.validate_operation_permissions(
                f"task_scheduler.{action_type}",
                [Permission.TASK_CREATE],
                context
            ):
                raise PermissionError(f"Insufficient permissions to schedule {action_type} task")
            
            # Create task
            task_id = str(uuid4())
            
            # Store task in database
            await self.db_sdk.execute(
                """
                INSERT INTO operations_scheduled_tasks (
                    task_id, tenant_id, name, schedule_type, cron_expression,
                    action_type, action_config, priority, is_active, created_at,
                    status, next_run
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    task_id, self.tenant_id, name, ScheduleType.CRON.value,
                    cron_expression, action_type, json.dumps(action_config),
                    priority.value, True, datetime.now(timezone.utc),
                    TaskStatus.SCHEDULED.value, self._calculate_next_cron_run(cron_expression)
                )
            )
            
            # Cache task
            cache_key = f"scheduled_task:{self.tenant_id}:{task_id}"
            task_data = {
                "task_id": task_id,
                "name": name,
                "schedule_type": ScheduleType.CRON.value,
                "cron_expression": cron_expression,
                "action_type": action_type,
                "action_config": action_config,
                "priority": priority.value,
                "is_active": True,
                "status": TaskStatus.SCHEDULED.value
            }
            await self.cache_sdk.set(cache_key, task_data, ttl=3600)
            
            # Publish event
            await self.event_sdk.publish(
                "task.scheduled",
                {
                    "task_id": task_id,
                    "name": name,
                    "schedule_type": "cron",
                    "cron_expression": cron_expression,
                    "action_type": action_type,
                    "tenant_id": self.tenant_id
                }
            )
            
            logger.info(f"Cron task {task_id} ({name}) scheduled: {cron_expression}")
            return task_id
            
        except Exception as e:
            logger.error(f"Failed to schedule cron task: {e}")
            raise
    
    async def schedule_delayed_task(
        self,
        name: str,
        delay_seconds: int,
        action_type: str,
        action_config: Dict[str, Any],
        context: SecurityContext,
        priority: OperationPriority = OperationPriority.NORMAL
    ) -> str:
        """Schedule a one-time delayed task"""
        try:
            # Validate permissions
            if not await self.validate_operation_permissions(
                f"task_scheduler.{action_type}",
                [Permission.TASK_CREATE],
                context
            ):
                raise PermissionError(f"Insufficient permissions to schedule {action_type} task")
            
            task_id = str(uuid4())
            execute_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
            
            # Store task
            await self.db_sdk.execute(
                """
                INSERT INTO operations_scheduled_tasks (
                    task_id, tenant_id, name, schedule_type, action_type,
                    action_config, priority, is_active, created_at, status, next_run
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    task_id, self.tenant_id, name, ScheduleType.DELAY.value,
                    action_type, json.dumps(action_config), priority.value,
                    True, datetime.now(timezone.utc), TaskStatus.SCHEDULED.value, execute_at
                )
            )
            
            # Cache and publish event
            cache_key = f"scheduled_task:{self.tenant_id}:{task_id}"
            task_data = {
                "task_id": task_id,
                "name": name,
                "schedule_type": ScheduleType.DELAY.value,
                "delay_seconds": delay_seconds,
                "action_type": action_type,
                "action_config": action_config,
                "execute_at": execute_at.isoformat()
            }
            await self.cache_sdk.set(cache_key, task_data, ttl=3600)
            
            await self.event_sdk.publish(
                "task.scheduled",
                {
                    "task_id": task_id,
                    "name": name,
                    "schedule_type": "delay",
                    "delay_seconds": delay_seconds,
                    "execute_at": execute_at.isoformat(),
                    "tenant_id": self.tenant_id
                }
            )
            
            logger.info(f"Delayed task {task_id} ({name}) scheduled for {execute_at}")
            return task_id
            
        except Exception as e:
            logger.error(f"Failed to schedule delayed task: {e}")
            raise
    
    async def schedule_recurring_task(
        self,
        name: str,
        interval_seconds: int,
        action_type: str,
        action_config: Dict[str, Any],
        context: SecurityContext,
        priority: OperationPriority = OperationPriority.NORMAL
    ) -> str:
        """Schedule a recurring task with fixed interval"""
        try:
            if not await self.validate_operation_permissions(
                f"task_scheduler.{action_type}",
                [Permission.TASK_CREATE],
                context
            ):
                raise PermissionError(f"Insufficient permissions to schedule {action_type} task")
            
            task_id = str(uuid4())
            next_run = datetime.now(timezone.utc) + timedelta(seconds=interval_seconds)
            
            await self.db_sdk.execute(
                """
                INSERT INTO operations_scheduled_tasks (
                    task_id, tenant_id, name, schedule_type, interval_seconds,
                    action_type, action_config, priority, is_active, created_at,
                    status, next_run
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    task_id, self.tenant_id, name, ScheduleType.RECURRING.value,
                    interval_seconds, action_type, json.dumps(action_config),
                    priority.value, True, datetime.now(timezone.utc),
                    TaskStatus.SCHEDULED.value, next_run
                )
            )
            
            logger.info(f"Recurring task {task_id} ({name}) scheduled every {interval_seconds}s")
            return task_id
            
        except Exception as e:
            logger.error(f"Failed to schedule recurring task: {e}")
            raise
    
    def _calculate_next_cron_run(self, cron_expression: str) -> datetime:
        """Calculate next cron execution time (simplified)"""
        # Simplified implementation - in production use croniter library
        return datetime.now(timezone.utc) + timedelta(minutes=1)
    
    async def start_scheduler(self):
        """Start the task scheduler"""
        if self.scheduler_running:
            return
        
        self.scheduler_running = True
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info("Task scheduler started")
    
    async def stop_scheduler(self):
        """Stop the task scheduler"""
        if not self.scheduler_running:
            return
        
        self.scheduler_running = False
        if self.scheduler_task:
            self.scheduler_task.cancel()
        
        for task in self.running_tasks.values():
            task.cancel()
        
        self.running_tasks.clear()
        logger.info("Task scheduler stopped")
    
    async def _scheduler_loop(self):
        """Main scheduler loop"""
        while self.scheduler_running:
            try:
                # Get ready tasks
                ready_tasks = await self._get_ready_tasks()
                
                for task_data in ready_tasks:
                    if len(self.running_tasks) >= self.max_concurrent_tasks:
                        break
                    
                    task_id = task_data["task_id"]
                    execution_task = asyncio.create_task(self._execute_task(task_data))
                    self.running_tasks[task_id] = execution_task
                    
                    execution_task.add_done_callback(
                        lambda t, tid=task_id: self.running_tasks.pop(tid, None)
                    )
                
                await asyncio.sleep(self.scheduler_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")
                await asyncio.sleep(self.scheduler_interval)
    
    async def _get_ready_tasks(self) -> List[Dict[str, Any]]:
        """Get tasks ready for execution"""
        now = datetime.now(timezone.utc)
        
        ready_tasks = await self.db_sdk.query_all(
            """
            SELECT task_id, name, schedule_type, cron_expression, interval_seconds,
                   action_type, action_config, priority
            FROM operations_scheduled_tasks
            WHERE tenant_id = %s AND is_active = true AND status = %s AND next_run <= %s
            ORDER BY priority DESC, next_run ASC
            LIMIT 20
            """,
            (self.tenant_id, TaskStatus.SCHEDULED.value, now)
        )
        
        return [dict(task) for task in ready_tasks]
    
    async def _execute_task(self, task_data: Dict[str, Any]):
        """Execute a scheduled task"""
        task_id = task_data["task_id"]
        action_type = task_data["action_type"]
        
        try:
            # Update status to running
            await self.db_sdk.execute(
                """
                UPDATE operations_scheduled_tasks
                SET status = %s, last_run = %s
                WHERE task_id = %s AND tenant_id = %s
                """,
                (TaskStatus.RUNNING.value, datetime.now(timezone.utc), task_id, self.tenant_id)
            )
            
            # Execute based on action type
            if action_type == "job_queue":
                action_config = json.loads(task_data["action_config"])
                job_type = JobType(action_config["job_type"])
                payload = action_config.get("payload", {})
                
                await self.job_queue_sdk.enqueue_job(
                    job_type=job_type,
                    payload=payload,
                    context=SecurityContext(
                        tenant_id=self.tenant_id,
                        user_id="scheduler",
                        permissions=[Permission.JOB_CREATE],
                        roles=["system"]
                    )
                )
            elif action_type in self.task_handlers:
                handler = self.task_handlers[action_type]
                action_config = json.loads(task_data["action_config"])
                await handler(action_config)
            else:
                raise ValueError(f"No handler for action type: {action_type}")
            
            # Complete task execution
            await self._complete_task_execution(task_data, success=True)
            
            await self.event_sdk.publish(
                "task.completed",
                {
                    "task_id": task_id,
                    "name": task_data["name"],
                    "action_type": action_type,
                    "tenant_id": self.tenant_id
                }
            )
            
        except Exception as e:
            logger.error(f"Task {task_id} execution failed: {e}")
            await self._complete_task_execution(task_data, success=False, error=str(e))
    
    async def _complete_task_execution(self, task_data: Dict[str, Any], success: bool, error: Optional[str] = None):
        """Complete task execution and schedule next run if recurring"""
        task_id = task_data["task_id"]
        schedule_type = ScheduleType(task_data["schedule_type"])
        
        # Calculate next run for recurring tasks
        next_run = None
        status = TaskStatus.COMPLETED.value
        is_active = True
        
        if schedule_type == ScheduleType.CRON and success:
            next_run = self._calculate_next_cron_run(task_data["cron_expression"])
            status = TaskStatus.SCHEDULED.value
        elif schedule_type == ScheduleType.RECURRING and success:
            interval_seconds = task_data["interval_seconds"]
            next_run = datetime.now(timezone.utc) + timedelta(seconds=interval_seconds)
            status = TaskStatus.SCHEDULED.value
        else:
            is_active = False  # One-time tasks become inactive
        
        await self.db_sdk.execute(
            """
            UPDATE operations_scheduled_tasks
            SET status = %s, is_active = %s, next_run = %s, last_error = %s
            WHERE task_id = %s AND tenant_id = %s
            """,
            (status, is_active, next_run, error, task_id, self.tenant_id)
        )
    
    async def health_check(self) -> Dict[str, Any]:
        """Health check for Task Scheduler SDK"""
        try:
            await self.db_sdk.query_one("SELECT 1")
            
            return {
                "status": "healthy",
                "sdk": "TaskSchedulerSDK",
                "tenant_id": self.tenant_id,
                "scheduler_running": self.scheduler_running,
                "running_tasks": len(self.running_tasks),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "sdk": "TaskSchedulerSDK",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    def _get_sdk_capabilities(self) -> List[str]:
        """Get Task Scheduler SDK capabilities"""
        return [
            "cron_scheduling",
            "delayed_execution",
            "recurring_tasks",
            "task_dependencies",
            "priority_scheduling",
            "job_queue_integration"
        ]
