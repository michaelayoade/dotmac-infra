"""
Operations Plane Integration Tests

Comprehensive testing of all Operations Plane SDKs with DRY principles
and SDK-to-SDK composition validation.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any

from .workflow_engine_sdk import WorkflowEngineSDK, WorkflowStatus
from .task_scheduler_sdk import TaskSchedulerSDK, TaskStatus
from .job_queue_sdk import JobQueueSDK, JobType, JobStatus
from .state_machine_sdk import StateMachineSDK, StateType, StateMachineStatus
from .automation_engine_sdk import AutomationEngineSDK, TriggerType, ActionType
from ..base_sdk import SecurityContext
from ...core.enums import Permission

logger = logging.getLogger(__name__)


class OperationsPlaneIntegrationTests:
    """Integration tests for Operations Plane SDKs"""
    
    def __init__(self, tenant_id: str = "test_tenant"):
        self.tenant_id = tenant_id
        self.test_context = SecurityContext(
            user_id="test_user",
            permissions=[
                Permission.WORKFLOW_CREATE, Permission.WORKFLOW_EXECUTE,
                Permission.TASK_CREATE, Permission.TASK_EXECUTE,
                Permission.JOB_CREATE, Permission.JOB_EXECUTE,
                Permission.STATE_MACHINE_CREATE, Permission.STATE_MACHINE_EXECUTE,
                Permission.AUTOMATION_CREATE, Permission.AUTOMATION_EXECUTE
            ],
            roles=["admin"]
        )
        
        # Initialize SDKs
        self.workflow_sdk = WorkflowEngineSDK(tenant_id)
        self.task_sdk = TaskSchedulerSDK(tenant_id)
        self.job_sdk = JobQueueSDK(tenant_id)
        self.state_machine_sdk = StateMachineSDK(tenant_id)
        self.automation_sdk = AutomationEngineSDK(tenant_id)
        
        self.test_results = []
    
    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all Operations Plane integration tests"""
        logger.info("Starting Operations Plane integration tests...")
        
        test_methods = [
            self.test_workflow_engine_basic,
            self.test_task_scheduler_basic,
            self.test_job_queue_basic,
            self.test_state_machine_basic,
            self.test_automation_engine_basic,
            self.test_cross_sdk_composition,
            self.test_workflow_with_tasks,
            self.test_automation_with_workflows,
            self.test_state_machine_with_events,
            self.test_sdk_health_checks
        ]
        
        passed = 0
        failed = 0
        
        for test_method in test_methods:
            try:
                await test_method()
                self.test_results.append({
                    "test": test_method.__name__,
                    "status": "PASSED",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
                passed += 1
                logger.info(f"✅ {test_method.__name__} PASSED")
                
            except Exception as e:
                self.test_results.append({
                    "test": test_method.__name__,
                    "status": "FAILED",
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
                failed += 1
                logger.error(f"❌ {test_method.__name__} FAILED: {e}")
        
        success_rate = (passed / (passed + failed)) * 100 if (passed + failed) > 0 else 0
        
        summary = {
            "total_tests": len(test_methods),
            "passed": passed,
            "failed": failed,
            "success_rate": f"{success_rate:.1f}%",
            "test_results": self.test_results,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"Operations Plane integration tests completed: {success_rate:.1f}% success rate")
        return summary
    
    async def test_workflow_engine_basic(self):
        """Test basic Workflow Engine SDK functionality"""
        # Create a simple workflow
        steps_config = [
            {
                "step_id": "step1",
                "name": "Initialize Process",
                "action_type": "job_queue",
                "action_config": {
                    "job_type": "data_processing",
                    "payload": {"operation": "initialize"}
                }
            },
            {
                "step_id": "step2",
                "name": "Process Data",
                "action_type": "job_queue",
                "action_config": {
                    "job_type": "data_processing",
                    "payload": {"operation": "process"}
                },
                "depends_on": ["step1"]
            }
        ]
        
        workflow_id = await self.workflow_sdk.create_workflow(
            name="Test Workflow",
            description="Basic workflow test",
            steps_config=steps_config,
            context=self.test_context
        )
        
        assert workflow_id, "Workflow creation failed"
        
        # Execute workflow
        execution_id = await self.workflow_sdk.execute_workflow(
            workflow_id=workflow_id,
            input_data={"test_data": "value"},
            context=self.test_context
        )
        
        assert execution_id, "Workflow execution failed"
        
        # Get workflow definition
        workflow_data = await self.workflow_sdk.get_workflow(workflow_id, self.test_context)
        assert workflow_data["name"] == "Test Workflow"
    
    async def test_task_scheduler_basic(self):
        """Test basic Task Scheduler SDK functionality"""
        # Schedule immediate task
        task_id = await self.task_sdk.schedule_task(
            task_type="test_task",
            payload={"message": "Hello from scheduler"},
            context=self.test_context,
            schedule_time=datetime.now(timezone.utc) + timedelta(seconds=1)
        )
        
        assert task_id, "Task scheduling failed"
        
        # Schedule recurring task
        recurring_task_id = await self.task_sdk.schedule_recurring_task(
            task_id="test_recurring",
            task_type="recurring_test",
            payload={"recurring": True},
            schedule_config={
                "interval_seconds": 60,
                "max_executions": 5
            },
            context=self.test_context
        )
        
        assert recurring_task_id, "Recurring task scheduling failed"
        
        # Get task status
        task_status = await self.task_sdk.get_task_status(task_id, self.test_context)
        assert task_status is not None
    
    async def test_job_queue_basic(self):
        """Test basic Job Queue SDK functionality"""
        # Enqueue job
        job_id = await self.job_sdk.enqueue_job(
            job_type=JobType.DATA_PROCESSING,
            payload={"data": "test_data", "operation": "validate"},
            context=self.test_context
        )
        
        assert job_id, "Job enqueuing failed"
        
        # Get job status
        job_status = await self.job_sdk.get_job_status(job_id, self.test_context)
        assert job_status is not None
        assert job_status["status"] in [JobStatus.QUEUED.value, JobStatus.PROCESSING.value]
        
        # Enqueue bulk job
        bulk_job_id = await self.job_sdk.enqueue_job(
            job_type=JobType.BULK_OPERATION,
            payload={"items": ["item1", "item2", "item3"]},
            context=self.test_context
        )
        
        assert bulk_job_id, "Bulk job enqueuing failed"
    
    async def test_state_machine_basic(self):
        """Test basic State Machine SDK functionality"""
        # Define states
        states_config = [
            {
                "state_id": "initial",
                "name": "Initial State",
                "state_type": "initial"
            },
            {
                "state_id": "processing",
                "name": "Processing State",
                "state_type": "intermediate"
            },
            {
                "state_id": "completed",
                "name": "Completed State",
                "state_type": "final"
            }
        ]
        
        # Define transitions
        transitions_config = [
            {
                "from_state": "initial",
                "to_state": "processing",
                "event": "start_processing"
            },
            {
                "from_state": "processing",
                "to_state": "completed",
                "event": "finish_processing"
            }
        ]
        
        # Create state machine
        machine_id = await self.state_machine_sdk.create_state_machine(
            name="Test State Machine",
            description="Basic state machine test",
            states_config=states_config,
            transitions_config=transitions_config,
            initial_state="initial",
            context=self.test_context
        )
        
        assert machine_id, "State machine creation failed"
        
        # Start instance
        instance_id = await self.state_machine_sdk.start_instance(
            machine_id=machine_id,
            context_data={"test": "data"},
            context=self.test_context
        )
        
        assert instance_id, "State machine instance start failed"
        
        # Send events
        transition1 = await self.state_machine_sdk.send_event(
            instance_id=instance_id,
            event="start_processing",
            event_data={"step": 1},
            context=self.test_context
        )
        
        assert transition1, "First transition failed"
        
        transition2 = await self.state_machine_sdk.send_event(
            instance_id=instance_id,
            event="finish_processing",
            event_data={"step": 2},
            context=self.test_context
        )
        
        assert transition2, "Second transition failed"
    
    async def test_automation_engine_basic(self):
        """Test basic Automation Engine SDK functionality"""
        # Create automation rule
        trigger_config = {
            "type": "event",
            "event_pattern": "test.event.*"
        }
        
        condition_config = {
            "type": "expression",
            "expression": "$event_data.priority > 5"
        }
        
        actions_config = [
            {
                "type": "notification",
                "notification_config": {
                    "message": "High priority event detected: $event_data.message"
                }
            }
        ]
        
        rule_id = await self.automation_sdk.create_automation_rule(
            name="Test Automation Rule",
            description="Basic automation test",
            trigger_config=trigger_config,
            condition_config=condition_config,
            actions_config=actions_config,
            context=self.test_context
        )
        
        assert rule_id, "Automation rule creation failed"
        
        # Execute rule manually
        execution_id = await self.automation_sdk.execute_automation_rule(
            rule_id=rule_id,
            trigger_data={
                "event_data": {
                    "priority": 8,
                    "message": "Test high priority event"
                }
            },
            context=self.test_context
        )
        
        assert execution_id, "Automation rule execution failed"
    
    async def test_cross_sdk_composition(self):
        """Test SDK-to-SDK composition across Operations Plane"""
        # Test that SDKs can compose with each other
        
        # Workflow Engine should compose with Job Queue and Task Scheduler
        assert hasattr(self.workflow_sdk, 'job_queue_sdk')
        assert hasattr(self.workflow_sdk, 'task_scheduler_sdk')
        
        # Automation Engine should compose with Workflow Engine and Task Scheduler
        assert hasattr(self.automation_sdk, 'workflow_engine')
        assert hasattr(self.automation_sdk, 'task_scheduler')
        
        # All SDKs should have Platform SDK composition
        for sdk in [self.workflow_sdk, self.task_sdk, self.job_sdk, 
                   self.state_machine_sdk, self.automation_sdk]:
            assert hasattr(sdk, 'db_sdk'), f"{sdk.__class__.__name__} missing db_sdk"
            assert hasattr(sdk, 'cache_sdk'), f"{sdk.__class__.__name__} missing cache_sdk"
            assert hasattr(sdk, 'event_sdk'), f"{sdk.__class__.__name__} missing event_sdk"
            assert hasattr(sdk, 'observability_sdk'), f"{sdk.__class__.__name__} missing observability_sdk"
    
    async def test_workflow_with_tasks(self):
        """Test workflow that schedules tasks"""
        # Register custom step handler for task scheduling
        async def schedule_task_handler(config):
            task_id = await self.task_sdk.schedule_task(
                task_type=config["task_type"],
                payload=config["payload"],
                context=self.test_context,
                schedule_time=datetime.now(timezone.utc) + timedelta(seconds=1)
            )
            return {"scheduled_task_id": task_id}
        
        await self.workflow_sdk.register_step_handler("schedule_task", schedule_task_handler)
        
        # Create workflow with task scheduling steps
        steps_config = [
            {
                "step_id": "schedule_task1",
                "name": "Schedule First Task",
                "action_type": "schedule_task",
                "action_config": {
                    "task_type": "data_validation",
                    "payload": {"data": "test1"}
                }
            },
            {
                "step_id": "schedule_task2",
                "name": "Schedule Second Task",
                "action_type": "schedule_task",
                "action_config": {
                    "task_type": "data_processing",
                    "payload": {"data": "test2"}
                },
                "depends_on": ["schedule_task1"]
            }
        ]
        
        workflow_id = await self.workflow_sdk.create_workflow(
            name="Task Scheduling Workflow",
            description="Workflow that schedules tasks",
            steps_config=steps_config,
            context=self.test_context
        )
        
        execution_id = await self.workflow_sdk.execute_workflow(
            workflow_id=workflow_id,
            input_data={"workflow_type": "task_scheduling"},
            context=self.test_context
        )
        
        assert execution_id, "Task scheduling workflow execution failed"
    
    async def test_automation_with_workflows(self):
        """Test automation that triggers workflows"""
        # First create a workflow
        steps_config = [
            {
                "step_id": "automated_step",
                "name": "Automated Processing Step",
                "action_type": "job_queue",
                "action_config": {
                    "job_type": "automated_processing",
                    "payload": {"automated": True}
                }
            }
        ]
        
        workflow_id = await self.workflow_sdk.create_workflow(
            name="Automated Workflow",
            description="Workflow triggered by automation",
            steps_config=steps_config,
            context=self.test_context
        )
        
        # Create automation rule that triggers the workflow
        trigger_config = {
            "type": "manual"
        }
        
        condition_config = {
            "type": "always"
        }
        
        actions_config = [
            {
                "type": "workflow",
                "workflow_id": workflow_id,
                "input_data": {
                    "triggered_by": "automation"
                }
            }
        ]
        
        rule_id = await self.automation_sdk.create_automation_rule(
            name="Workflow Trigger Rule",
            description="Automation that triggers workflow",
            trigger_config=trigger_config,
            condition_config=condition_config,
            actions_config=actions_config,
            context=self.test_context
        )
        
        # Execute the automation
        execution_id = await self.automation_sdk.execute_automation_rule(
            rule_id=rule_id,
            trigger_data={"source": "test"},
            context=self.test_context
        )
        
        assert execution_id, "Automation with workflow execution failed"
    
    async def test_state_machine_with_events(self):
        """Test state machine integration with event system"""
        # This test validates that state machines can publish and respond to events
        
        # Create a simple state machine
        states_config = [
            {"state_id": "waiting", "name": "Waiting", "state_type": "initial"},
            {"state_id": "active", "name": "Active", "state_type": "intermediate"},
            {"state_id": "done", "name": "Done", "state_type": "final"}
        ]
        
        transitions_config = [
            {"from_state": "waiting", "to_state": "active", "event": "activate"},
            {"from_state": "active", "to_state": "done", "event": "complete"}
        ]
        
        machine_id = await self.state_machine_sdk.create_state_machine(
            name="Event-Driven State Machine",
            description="State machine with event integration",
            states_config=states_config,
            transitions_config=transitions_config,
            initial_state="waiting",
            context=self.test_context
        )
        
        instance_id = await self.state_machine_sdk.start_instance(
            machine_id=machine_id,
            context_data={"event_driven": True},
            context=self.test_context
        )
        
        # Simulate event-driven transitions
        await self.state_machine_sdk.send_event(
            instance_id=instance_id,
            event="activate",
            event_data={"activation_source": "test"},
            context=self.test_context
        )
        
        await self.state_machine_sdk.send_event(
            instance_id=instance_id,
            event="complete",
            event_data={"completion_source": "test"},
            context=self.test_context
        )
        
        # Verify final state
        status = await self.state_machine_sdk.get_instance_status(instance_id, self.test_context)
        assert status["current_state"] == "done"
    
    async def test_sdk_health_checks(self):
        """Test health checks for all Operations Plane SDKs"""
        sdks = [
            ("WorkflowEngineSDK", self.workflow_sdk),
            ("TaskSchedulerSDK", self.task_sdk),
            ("JobQueueSDK", self.job_sdk),
            ("StateMachineSDK", self.state_machine_sdk),
            ("AutomationEngineSDK", self.automation_sdk)
        ]
        
        for sdk_name, sdk in sdks:
            health_status = await sdk.health_check()
            assert health_status["status"] == "healthy", f"{sdk_name} health check failed"
            assert health_status["sdk"] == sdk_name
            assert health_status["tenant_id"] == self.tenant_id


async def run_operations_plane_tests():
    """Run Operations Plane integration tests"""
    tests = OperationsPlaneIntegrationTests()
    return await tests.run_all_tests()


if __name__ == "__main__":
    # Run tests if executed directly
    async def main():
        results = await run_operations_plane_tests()
        print(json.dumps(results, indent=2))
    
    asyncio.run(main())
