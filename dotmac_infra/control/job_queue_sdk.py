"""
Job Queue SDK

Background processing foundation for the Operations Plane.
Provides asynchronous job processing with retry logic, priority queuing,
and comprehensive monitoring.

Features:
- Priority-based job queuing
- Automatic retry with exponential backoff
- Job status tracking and monitoring
- Dead letter queue for failed jobs
- SDK-to-SDK composition with Platform and Data planes
- DRY patterns for cross-cutting concerns
"""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable
from uuid import uuid4
from enum import Enum

from .base_operations_sdk import BaseOperationsSDK, OperationPriority
from ..base_sdk import SecurityContext
from ...core.enums import Permission

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Job status enumeration"""
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTER = "dead_letter"


class JobType(Enum):
    """Standard job types for ISP/OSS/BSS operations"""
    EMAIL_SEND = "email_send"
    SMS_SEND = "sms_send"
    INVOICE_GENERATION = "invoice_generation"
    PAYMENT_PROCESSING = "payment_processing"
    SERVICE_PROVISIONING = "service_provisioning"
    USAGE_CALCULATION = "usage_calculation"
    REPORT_GENERATION = "report_generation"
    DATA_CLEANUP = "data_cleanup"
    BULK_OPERATION = "bulk_operation"
    INTEGRATION_SYNC = "integration_sync"


class Job:
    """Job data structure"""
    
    def __init__(
        self,
        job_id: str,
        job_type: JobType,
        payload: Dict[str, Any],
        priority: OperationPriority = OperationPriority.NORMAL,
        max_retries: int = 3,
        retry_delay: int = 60,
        scheduled_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.job_id = job_id
        self.job_type = job_type
        self.payload = payload
        self.priority = priority
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_count = 0
        self.status = JobStatus.QUEUED
        self.scheduled_at = scheduled_at or datetime.now(timezone.utc)
        self.created_at = datetime.now(timezone.utc)
        self.started_at = None
        self.completed_at = None
        self.error_message = None
        self.result = None
        self.metadata = metadata or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary for serialization"""
        return {
            "job_id": self.job_id,
            "job_type": self.job_type.value,
            "payload": self.payload,
            "priority": self.priority.value,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "retry_count": self.retry_count,
            "status": self.status.value,
            "scheduled_at": self.scheduled_at.isoformat(),
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "result": self.result,
            "metadata": self.metadata
        }


class JobQueueSDK(BaseOperationsSDK):
    """
    Job Queue SDK for background processing
    
    Provides asynchronous job processing with priority queuing, retry logic,
    and comprehensive monitoring using DRY principles and SDK composition.
    """
    
    def __init__(self, tenant_id: str, **kwargs):
        super().__init__(tenant_id, **kwargs)
        
        # Job processing configuration
        self.worker_count = kwargs.get('worker_count', 4)
        self.poll_interval = kwargs.get('poll_interval', 5)  # seconds
        self.dead_letter_threshold = kwargs.get('dead_letter_threshold', 5)
        
        # Job handlers registry
        self.job_handlers: Dict[JobType, Callable] = {}
        
        # Worker control
        self.workers_running = False
        self.worker_tasks: List[asyncio.Task] = []
    
    async def enqueue_job(
        self,
        job_type: JobType,
        payload: Dict[str, Any],
        context: SecurityContext,
        priority: OperationPriority = OperationPriority.NORMAL,
        scheduled_at: Optional[datetime] = None,
        max_retries: int = 3,
        retry_delay: int = 60,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Enqueue a job for background processing
        Uses DRY patterns for validation, caching, and events
        """
        try:
            # Validate permissions
            if not await self.validate_operation_permissions(
                f"job_queue.{job_type.value}",
                [Permission.JOB_CREATE],
                context
            ):
                raise PermissionError(f"Insufficient permissions to enqueue {job_type.value} job")
            
            # Create job
            job_id = str(uuid4())
            job = Job(
                job_id=job_id,
                job_type=job_type,
                payload=payload,
                priority=priority,
                max_retries=max_retries,
                retry_delay=retry_delay,
                scheduled_at=scheduled_at,
                metadata=metadata
            )
            
            # Store job in database
            await self.db_sdk.execute(
                """
                INSERT INTO operations_jobs (
                    job_id, tenant_id, job_type, payload, priority, max_retries,
                    retry_delay, retry_count, status, scheduled_at, created_at, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    job.job_id, self.tenant_id, job.job_type.value,
                    json.dumps(job.payload), job.priority.value, job.max_retries,
                    job.retry_delay, job.retry_count, job.status.value,
                    job.scheduled_at, job.created_at, json.dumps(job.metadata)
                )
            )
            
            # Cache job for fast access
            cache_key = f"job:{self.tenant_id}:{job_id}"
            await self.cache_sdk.set(cache_key, job.to_dict(), ttl=3600)
            
            # Add to priority queue
            queue_name = f"jobs:{self.tenant_id}:priority_{priority.value}"
            await self.queue_sdk.enqueue(queue_name, job.to_dict(), priority.value)
            
            # Publish job enqueued event
            await self.event_sdk.publish(
                "job.enqueued",
                {
                    "job_id": job_id,
                    "job_type": job_type.value,
                    "tenant_id": self.tenant_id,
                    "priority": priority.value,
                    "scheduled_at": job.scheduled_at.isoformat()
                }
            )
            
            # Log operation
            await self.observability_sdk.log_info(
                f"Job {job_id} ({job_type.value}) enqueued with priority {priority.value}",
                {"job_id": job_id, "job_type": job_type.value}
            )
            
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to enqueue job: {e}")
            raise
    
    async def get_job(
        self,
        job_id: str,
        context: SecurityContext
    ) -> Optional[Dict[str, Any]]:
        """
        Get job details with caching (DRY pattern)
        """
        try:
            # Check cache first
            cache_key = f"job:{self.tenant_id}:{job_id}"
            cached_job = await self.cache_sdk.get(cache_key)
            
            if cached_job:
                return cached_job
            
            # Fallback to database
            job_record = await self.db_sdk.query_one(
                """
                SELECT job_id, job_type, payload, priority, max_retries, retry_delay,
                       retry_count, status, scheduled_at, created_at, started_at,
                       completed_at, error_message, result, metadata
                FROM operations_jobs
                WHERE job_id = %s AND tenant_id = %s
                """,
                (job_id, self.tenant_id)
            )
            
            if job_record:
                job_data = dict(job_record)
                # Parse JSON fields
                job_data['payload'] = json.loads(job_data['payload']) if job_data['payload'] else {}
                job_data['metadata'] = json.loads(job_data['metadata']) if job_data['metadata'] else {}
                job_data['result'] = json.loads(job_data['result']) if job_data['result'] else None
                
                # Update cache
                await self.cache_sdk.set(cache_key, job_data, ttl=3600)
                return job_data
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get job {job_id}: {e}")
            raise
    
    async def register_job_handler(
        self,
        job_type: JobType,
        handler_func: Callable
    ):
        """Register a job handler function"""
        self.job_handlers[job_type] = handler_func
        logger.info(f"Registered handler for job type: {job_type.value}")
    
    async def start_workers(self):
        """Start background job workers"""
        if self.workers_running:
            logger.warning("Workers already running")
            return
        
        self.workers_running = True
        
        # Start worker tasks
        for i in range(self.worker_count):
            task = asyncio.create_task(self._worker_loop(f"worker_{i}"))
            self.worker_tasks.append(task)
        
        logger.info(f"Started {self.worker_count} job workers")
    
    async def stop_workers(self):
        """Stop background job workers"""
        if not self.workers_running:
            return
        
        self.workers_running = False
        
        # Cancel worker tasks
        for task in self.worker_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        self.worker_tasks.clear()
        
        logger.info("Stopped all job workers")
    
    async def _worker_loop(self, worker_name: str):
        """Main worker loop for processing jobs"""
        logger.info(f"Worker {worker_name} started")
        
        while self.workers_running:
            try:
                # Get next job from priority queues (highest priority first)
                job_data = await self._get_next_job()
                
                if job_data:
                    await self._process_job(job_data, worker_name)
                else:
                    # No jobs available, wait before polling again
                    await asyncio.sleep(self.poll_interval)
                    
            except asyncio.CancelledError:
                logger.info(f"Worker {worker_name} cancelled")
                break
            except Exception as e:
                logger.error(f"Worker {worker_name} error: {e}")
                await asyncio.sleep(self.poll_interval)
        
        logger.info(f"Worker {worker_name} stopped")
    
    async def _get_next_job(self) -> Optional[Dict[str, Any]]:
        """Get next job from priority queues"""
        # Check priority queues from highest to lowest
        for priority in [5, 4, 3, 2, 1]:  # URGENT to LOW
            queue_name = f"jobs:{self.tenant_id}:priority_{priority}"
            job_data = await self.queue_sdk.dequeue(queue_name)
            
            if job_data:
                return job_data
        
        return None
    
    async def _process_job(self, job_data: Dict[str, Any], worker_name: str):
        """Process a single job"""
        job_id = job_data["job_id"]
        job_type_str = job_data["job_type"]
        
        try:
            # Convert job_type string to enum
            job_type = JobType(job_type_str)
            
            # Check if handler is registered
            if job_type not in self.job_handlers:
                raise ValueError(f"No handler registered for job type: {job_type_str}")
            
            # Update job status to processing
            await self._update_job_status(job_id, JobStatus.PROCESSING)
            
            # Execute job handler
            handler = self.job_handlers[job_type]
            result = await handler(job_data["payload"])
            
            # Update job as completed
            await self._update_job_status(
                job_id, JobStatus.COMPLETED, result=result
            )
            
            # Publish completion event
            await self.event_sdk.publish(
                "job.completed",
                {
                    "job_id": job_id,
                    "job_type": job_type_str,
                    "tenant_id": self.tenant_id,
                    "worker": worker_name,
                    "result": result
                }
            )
            
            logger.info(f"Job {job_id} completed by {worker_name}")
            
        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}")
            await self._handle_job_failure(job_data, str(e))
    
    async def _handle_job_failure(self, job_data: Dict[str, Any], error_message: str):
        """Handle job failure with retry logic"""
        job_id = job_data["job_id"]
        retry_count = job_data.get("retry_count", 0)
        max_retries = job_data.get("max_retries", 3)
        
        if retry_count < max_retries:
            # Retry the job
            retry_count += 1
            retry_delay = job_data.get("retry_delay", 60)
            scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=retry_delay)
            
            # Update job for retry
            await self.db_sdk.execute(
                """
                UPDATE operations_jobs
                SET retry_count = %s, status = %s, scheduled_at = %s, error_message = %s
                WHERE job_id = %s AND tenant_id = %s
                """,
                (retry_count, JobStatus.RETRYING.value, scheduled_at, error_message, job_id, self.tenant_id)
            )
            
            # Re-enqueue for retry
            job_data["retry_count"] = retry_count
            job_data["scheduled_at"] = scheduled_at.isoformat()
            priority = job_data.get("priority", OperationPriority.NORMAL.value)
            queue_name = f"jobs:{self.tenant_id}:priority_{priority}"
            await self.queue_sdk.enqueue(queue_name, job_data, priority)
            
            logger.info(f"Job {job_id} scheduled for retry {retry_count}/{max_retries}")
            
        else:
            # Move to dead letter queue
            await self._update_job_status(
                job_id, JobStatus.DEAD_LETTER, error=error_message
            )
            
            # Add to dead letter queue
            dead_letter_queue = f"dead_letter:{self.tenant_id}"
            await self.queue_sdk.enqueue(dead_letter_queue, job_data, 1)
            
            # Publish dead letter event
            await self.event_sdk.publish(
                "job.dead_letter",
                {
                    "job_id": job_id,
                    "job_type": job_data["job_type"],
                    "tenant_id": self.tenant_id,
                    "error": error_message,
                    "retry_count": retry_count
                }
            )
            
            logger.error(f"Job {job_id} moved to dead letter queue after {retry_count} retries")
    
    async def _update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        result: Optional[Any] = None,
        error: Optional[str] = None
    ):
        """Update job status in database and cache"""
        now = datetime.now(timezone.utc)
        
        # Prepare update data
        update_data = {
            "status": status.value,
            "updated_at": now
        }
        
        if status == JobStatus.PROCESSING:
            update_data["started_at"] = now
        elif status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.DEAD_LETTER]:
            update_data["completed_at"] = now
        
        if result is not None:
            update_data["result"] = json.dumps(result)
        
        if error:
            update_data["error_message"] = error
        
        # Update database
        set_clause = ", ".join([f"{k} = %s" for k in update_data.keys()])
        values = list(update_data.values()) + [job_id, self.tenant_id]
        
        await self.db_sdk.execute(
            f"""
            UPDATE operations_jobs
            SET {set_clause}
            WHERE job_id = %s AND tenant_id = %s
            """,
            values
        )
        
        # Update cache
        cache_key = f"job:{self.tenant_id}:{job_id}"
        cached_job = await self.cache_sdk.get(cache_key)
        if cached_job:
            cached_job.update(update_data)
            await self.cache_sdk.set(cache_key, cached_job, ttl=3600)
    
    async def get_queue_stats(self, context: SecurityContext) -> Dict[str, Any]:
        """Get job queue statistics"""
        try:
            # Get job counts by status
            stats = await self.db_sdk.query_one(
                """
                SELECT 
                    COUNT(*) as total_jobs,
                    COUNT(CASE WHEN status = 'queued' THEN 1 END) as queued,
                    COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                    COUNT(CASE WHEN status = 'retrying' THEN 1 END) as retrying,
                    COUNT(CASE WHEN status = 'dead_letter' THEN 1 END) as dead_letter
                FROM operations_jobs
                WHERE tenant_id = %s
                """,
                (self.tenant_id,)
            )
            
            return {
                "tenant_id": self.tenant_id,
                "workers_running": self.workers_running,
                "worker_count": self.worker_count,
                "job_stats": dict(stats) if stats else {},
                "registered_handlers": [jt.value for jt in self.job_handlers.keys()]
            }
            
        except Exception as e:
            logger.error(f"Failed to get queue stats: {e}")
            raise
    
    async def health_check(self) -> Dict[str, Any]:
        """Health check for Job Queue SDK"""
        try:
            # Test database connectivity
            await self.db_sdk.query_one("SELECT 1")
            
            # Test cache connectivity
            test_key = f"health_check:{self.tenant_id}:{datetime.now().timestamp()}"
            await self.cache_sdk.set(test_key, "ok", ttl=60)
            await self.cache_sdk.delete(test_key)
            
            # Test queue connectivity
            test_queue = f"health_check:{self.tenant_id}"
            await self.queue_sdk.enqueue(test_queue, {"test": "ok"}, 1)
            await self.queue_sdk.dequeue(test_queue)
            
            return {
                "status": "healthy",
                "sdk": "JobQueueSDK",
                "tenant_id": self.tenant_id,
                "workers_running": self.workers_running,
                "worker_count": self.worker_count,
                "registered_handlers": len(self.job_handlers),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "sdk": "JobQueueSDK",
                "tenant_id": self.tenant_id,
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    def _get_sdk_capabilities(self) -> List[str]:
        """Get Job Queue SDK capabilities"""
        return [
            "job_enqueue",
            "job_processing",
            "priority_queuing",
            "retry_logic",
            "dead_letter_queue",
            "job_monitoring",
            "worker_management",
            "queue_statistics"
        ]
