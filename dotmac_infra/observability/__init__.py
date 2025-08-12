"""
Observability Plane Integration for FastAPI Backend
Provides comprehensive logging, tracing, monitoring, and alerting
"""

from .middleware import ObservabilityMiddleware
from .manager import ObservabilityManager
from .decorators import (
    with_observability,
    track_performance,
    monitor_health,
    alert_on_failure,
)

__all__ = [
    "ObservabilityMiddleware",
    "ObservabilityManager",
    "with_observability",
    "track_performance",
    "monitor_health",
    "alert_on_failure",
]
