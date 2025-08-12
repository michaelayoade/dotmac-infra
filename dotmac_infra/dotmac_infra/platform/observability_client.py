"""
Platform SDK Observability Client (Python)
Provides logging, tracing, and monitoring with tenant isolation
"""

from typing import Any, Dict, List, Optional, ContextManager, Iterator, Literal
from datetime import datetime
import uuid
from contextlib import contextmanager
import time

from dotmac_infra.utils.logging import logger as base_logger


class ObservabilityClient:
    """Platform SDK Observability Client with tenant isolation"""

    def __init__(self, tenant_id: str) -> None:
        self.tenant_id = tenant_id
        self.base_logger = base_logger
        self._active_traces: Dict[str, Dict[str, Any]] = {}

    def get_logger(self, component: str) -> "ComponentLogger":
        """Get logger for specific component"""
        return ComponentLogger(self.base_logger, self.tenant_id, component)

    @contextmanager
    def trace(
        self, operation: str, trace_id: Optional[str] = None
    ) -> Iterator[Dict[str, Any]]:
        """Create a trace context for an operation"""
        trace_id = trace_id or str(uuid.uuid4())
        start_time = time.time()

        trace_data = {
            "trace_id": trace_id,
            "operation": operation,
            "tenant_id": self.tenant_id,
            "start_time": start_time,
            "started_at": datetime.utcnow(),
        }

        self._active_traces[trace_id] = trace_data

        try:
            self.base_logger.info(
                f"Trace started: {operation}",
                extra={
                    "trace_id": trace_id,
                    "tenant_id": self.tenant_id,
                    "operation": operation,
                },
            )

            yield trace_data

        except Exception as e:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000

            self.base_logger.error(
                f"Trace failed: {operation}",
                extra={
                    "trace_id": trace_id,
                    "tenant_id": self.tenant_id,
                    "operation": operation,
                    "duration_ms": duration_ms,
                    "error": str(e),
                },
            )
            raise

        finally:
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000

            self.base_logger.info(
                f"Trace completed: {operation}",
                extra={
                    "trace_id": trace_id,
                    "tenant_id": self.tenant_id,
                    "operation": operation,
                    "duration_ms": duration_ms,
                },
            )

            # Clean up trace
            self._active_traces.pop(trace_id, None)

    def get_active_traces(self) -> List[Dict[str, Any]]:
        """Get currently active traces"""
        return list(self._active_traces.values())

    # Backward/alternative API used by BaseSDK
    def trace_operation(
        self, operation: str, trace_id: Optional[str] = None
    ) -> ContextManager[Dict[str, Any]]:
        return self.trace(operation, trace_id)

    async def log_operation(
        self,
        operation: str,
        data: Dict[str, Any],
        level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO",
    ) -> None:
        """Structured operation log used by BaseSDK audit."""
        extra = {"tenant_id": self.tenant_id, **data}
        lvl = level.upper()
        if lvl == "ERROR":
            self.base_logger.error(operation, extra=extra)
        elif lvl == "WARNING":
            self.base_logger.warning(operation, extra=extra)
        elif lvl == "DEBUG":
            self.base_logger.debug(operation, extra=extra)
        else:
            self.base_logger.info(operation, extra=extra)


class ComponentLogger:
    """Logger wrapper with component and tenant context"""

    def __init__(
        self, base_logger: Any, tenant_id: str, component: str
    ) -> None:
        self.base_logger = base_logger
        self.tenant_id = tenant_id
        self.component = component

    def _log(
        self,
        level: str,
        message: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Internal logging method with context"""
        log_extra = {
            "tenant_id": self.tenant_id,
            "component": self.component,
            "timestamp": datetime.utcnow().isoformat(),
        }

        if extra:
            log_extra.update(extra)

        getattr(self.base_logger, level)(message, extra=log_extra)

    def info(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """Log info message"""
        self._log("info", message, extra)

    def error(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """Log error message"""
        self._log("error", message, extra)

    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """Log warning message"""
        self._log("warning", message, extra)

    def debug(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """Log debug message"""
        self._log("debug", message, extra)
