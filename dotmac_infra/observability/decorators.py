"""
Observability Decorators for FastAPI Endpoints
Provides easy-to-use decorators for adding observability to API endpoints
"""

import time
import functools
from typing import Callable, Any, Dict
from fastapi import Request

from .manager import get_observability_manager


def with_observability(operation_name: str = None, 
                      track_performance: bool = True,
                      alert_on_error: bool = True):
    """
    Decorator to add comprehensive observability to FastAPI endpoint functions
    
    Args:
        operation_name: Custom operation name for tracing (defaults to function name)
        track_performance: Whether to track performance metrics
        alert_on_error: Whether to fire alerts on errors
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            obs_manager = get_observability_manager()
            
            # Extract request from args if available
            request = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break
            
            # Get operation name
            op_name = operation_name or f"{func.__module__}.{func.__name__}"
            
            # Start performance tracking
            start_time = time.time()
            
            try:
                # Use observability tracing
                if obs_manager.observability:
                    async def traced_operation(span):
                        if request:
                            # Add request context to span
                            obs_manager.observability.info(f"Starting operation: {op_name}", {
                                'operation': op_name,
                                'request_id': getattr(request.state, 'request_id', None),
                                'user_id': getattr(request.state, 'current_user', {}).get('id', None) if hasattr(request.state, 'current_user') else None
                            })
                        
                        return await func(*args, **kwargs)
                    
                    result = await obs_manager.observability.trace(op_name, traced_operation)
                else:
                    result = await func(*args, **kwargs)
                
                # Track successful operation
                if track_performance:
                    duration_ms = (time.time() - start_time) * 1000
                    obs_manager.monitoring.observe_histogram(
                        'isp_operation_duration_ms', 
                        duration_ms, 
                        {'operation': op_name, 'status': 'success'}
                    )
                
                return result
                
            except Exception as e:
                # Track failed operation
                if track_performance:
                    duration_ms = (time.time() - start_time) * 1000
                    obs_manager.monitoring.observe_histogram(
                        'isp_operation_duration_ms', 
                        duration_ms, 
                        {'operation': op_name, 'status': 'error'}
                    )
                    
                    obs_manager.monitoring.increment_counter(
                        'isp_operation_errors_total',
                        1,
                        {'operation': op_name, 'error_type': type(e).__name__}
                    )
                
                # Fire alert on error
                if alert_on_error and obs_manager.alerting:
                    await obs_manager.fire_alert(
                        name='operation_failure',
                        description=f'Operation {op_name} failed: {str(e)}',
                        severity='error',
                        labels={
                            'service': 'isp-framework',
                            'operation': op_name,
                            'error_type': type(e).__name__
                        },
                        annotations={
                            'error_message': str(e),
                            'request_id': getattr(request.state, 'request_id', None) if request else None
                        }
                    )
                
                # Log error
                if obs_manager.observability:
                    obs_manager.observability.error(f"Operation failed: {op_name}", e, {
                        'operation': op_name,
                        'request_id': getattr(request.state, 'request_id', None) if request else None
                    })
                
                raise
        
        return wrapper
    return decorator


def track_performance(metric_name: str, labels: Dict[str, str] = None):
    """
    Decorator to track performance metrics for specific operations
    
    Args:
        metric_name: Name of the metric to track
        labels: Additional labels for the metric
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            obs_manager = get_observability_manager()
            
            start_time = time.time()
            
            try:
                result = await func(*args, **kwargs)
                
                # Track successful execution
                duration_ms = (time.time() - start_time) * 1000
                metric_labels = {**(labels or {}), 'status': 'success'}
                
                obs_manager.monitoring.observe_histogram(
                    f'isp_{metric_name}_duration_ms',
                    duration_ms,
                    metric_labels
                )
                
                obs_manager.monitoring.increment_counter(
                    f'isp_{metric_name}_total',
                    1,
                    metric_labels
                )
                
                return result
                
            except Exception as e:
                # Track failed execution
                duration_ms = (time.time() - start_time) * 1000
                metric_labels = {**(labels or {}), 'status': 'error', 'error_type': type(e).__name__}
                
                obs_manager.monitoring.observe_histogram(
                    f'isp_{metric_name}_duration_ms',
                    duration_ms,
                    metric_labels
                )
                
                obs_manager.monitoring.increment_counter(
                    f'isp_{metric_name}_total',
                    1,
                    metric_labels
                )
                
                raise
        
        return wrapper
    return decorator


def monitor_health(check_name: str, critical: bool = False):
    """
    Register endpoint as a health check monitor.
    
    Args:
        check_name: Name of the health check
        critical: Whether this is a critical health check
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)
        
        return wrapper
    return decorator


def alert_on_failure(alert_name: str, severity: str = 'warning', 
                    labels: Dict[str, str] = None):
    """
    Decorator to fire alerts when a function fails
    
    Args:
        alert_name: Name of the alert to fire
        severity: Severity level of the alert
        labels: Additional labels for the alert
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            obs_manager = get_observability_manager()
            
            try:
                return await func(*args, **kwargs)
                
            except Exception as e:
                # Fire alert on failure
                if obs_manager.alerting:
                    await obs_manager.fire_alert(
                        name=alert_name,
                        description=f'Function {func.__name__} failed: {str(e)}',
                        severity=severity,
                        labels={
                            'service': 'isp-framework',
                            'function': func.__name__,
                            'error_type': type(e).__name__,
                            **(labels or {})
                        },
                        annotations={
                            'error_message': str(e),
                            'function_module': func.__module__
                        }
                    )
                
                raise
        
        return wrapper
    return decorator


def business_metric(metric_name: str, metric_type: str = 'counter', 
                   value_extractor: Callable[[Any], float] = None,
                   labels_extractor: Callable[[Any], Dict[str, str]] = None):
    """
    Decorator to automatically track business metrics from function results
    
    Args:
        metric_name: Name of the business metric
        metric_type: Type of metric ('counter', 'gauge', 'histogram')
        value_extractor: Function to extract metric value from result
        labels_extractor: Function to extract labels from result
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            obs_manager = get_observability_manager()
            
            result = await func(*args, **kwargs)
            
            # Extract metric value
            if value_extractor:
                try:
                    value = value_extractor(result)
                except Exception:
                    value = 1  # Default value if extraction fails
            else:
                value = 1
            
            # Extract labels
            if labels_extractor:
                try:
                    labels = labels_extractor(result)
                except Exception:
                    labels = {}
            else:
                labels = {}
            
            # Update metric based on type
            if metric_type == 'counter':
                obs_manager.monitoring.increment_counter(
                    f'isp_business_{metric_name}_total',
                    value,
                    labels
                )
            elif metric_type == 'gauge':
                obs_manager.monitoring.set_gauge(
                    f'isp_business_{metric_name}',
                    value,
                    labels
                )
            elif metric_type == 'histogram':
                obs_manager.monitoring.observe_histogram(
                    f'isp_business_{metric_name}',
                    value,
                    labels
                )
            
            return result
        
        return wrapper
    return decorator


def customer_operation(operation_type: str):
    """
    Decorator specifically for customer-related operations
    
    Args:
        operation_type: Type of customer operation (registration, update, deletion, etc.)
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            obs_manager = get_observability_manager()
            
            start_time = time.time()
            
            try:
                result = await func(*args, **kwargs)
                
                # Track successful customer operation
                obs_manager.monitoring.increment_counter(
                    'isp_customer_operations_total',
                    1,
                    {'operation': operation_type, 'status': 'success'}
                )
                
                duration_ms = (time.time() - start_time) * 1000
                obs_manager.monitoring.observe_histogram(
                    'isp_customer_operation_duration_ms',
                    duration_ms,
                    {'operation': operation_type}
                )
                
                # Log customer operation
                obs_manager.observability.info(f"Customer operation completed: {operation_type}", {
                    'operation': operation_type,
                    'duration_ms': duration_ms,
                    'status': 'success'
                })
                
                return result
                
            except Exception as e:
                # Track failed customer operation
                obs_manager.monitoring.increment_counter(
                    'isp_customer_operations_total',
                    1,
                    {'operation': operation_type, 'status': 'error', 'error_type': type(e).__name__}
                )
                
                # Fire alert for customer operation failures
                if obs_manager.alerting:
                    await obs_manager.fire_alert(
                        name='customer_operation_failure',
                        description=f'Customer {operation_type} operation failed: {str(e)}',
                        severity='error',
                        labels={
                            'service': 'isp-framework',
                            'operation': operation_type,
                            'type': 'customer'
                        },
                        annotations={
                            'error_message': str(e),
                            'operation_type': operation_type
                        }
                    )
                
                raise
        
        return wrapper
    return decorator
