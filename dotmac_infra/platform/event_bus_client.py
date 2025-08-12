"""
Platform SDK Event Bus Client (Python)
Provides event publishing and subscription with tenant isolation
"""

from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
import uuid
import asyncio

from dotmac_infra.utils.logging import logger


class EventBusClient:
    """Platform SDK Event Bus Client with tenant isolation"""

    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.logger = logger
        self._subscribers = {}
        self._event_history = []

    async def publish(self, event_type: str, data: Dict[str, Any]) -> bool:
        """Publish an event"""
        try:
            event = {
                "id": str(uuid.uuid4()),
                "type": event_type,
                "tenant_id": self.tenant_id,
                "data": data,
                "timestamp": datetime.utcnow().isoformat(),
                "published_at": datetime.utcnow(),
            }

            # Store in history (limited to last 1000 events)
            self._event_history.append(event)
            if len(self._event_history) > 1000:
                self._event_history.pop(0)

            # Notify subscribers
            subscribers = self._subscribers.get(event_type, [])
            for callback in subscribers:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(event)
                    else:
                        callback(event)
                except Exception as e:
                    self.logger.error(f"Event subscriber error: {str(e)}")

            self.logger.info(f"Event published: {event_type}")
            return True

        except Exception as e:
            self.logger.error(f"Event publish failed: {str(e)}")
            return False

    def subscribe(self, event_type: str, callback: Callable) -> str:
        """Subscribe to an event type"""
        try:
            subscription_id = str(uuid.uuid4())

            if event_type not in self._subscribers:
                self._subscribers[event_type] = []

            self._subscribers[event_type].append(callback)

            self.logger.info(f"Subscribed to event: {event_type}")
            return subscription_id

        except Exception as e:
            self.logger.error(f"Event subscribe failed: {str(e)}")
            return ""

    def unsubscribe(self, event_type: str, callback: Callable) -> bool:
        """Unsubscribe from an event type"""
        try:
            if event_type in self._subscribers:
                if callback in self._subscribers[event_type]:
                    self._subscribers[event_type].remove(callback)
                    return True
            return False

        except Exception as e:
            self.logger.error(f"Event unsubscribe failed: {str(e)}")
            return False

    def get_event_history(
        self, event_type: Optional[str] = None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get recent event history"""
        try:
            events = self._event_history

            if event_type:
                events = [e for e in events if e["type"] == event_type]

            # Filter by tenant
            events = [e for e in events if e["tenant_id"] == self.tenant_id]

            return events[-limit:] if limit else events

        except Exception as e:
            self.logger.error(f"Get event history failed: {str(e)}")
            return []
