"""
Observability Middleware for FastAPI
Comprehensive request tracing, metrics collection, and error handling
"""

import time
import uuid
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from .manager import get_observability_manager


class ObservabilityMiddleware(BaseHTTPMiddleware):
    """Enhanced middleware for comprehensive observability across all API requests"""
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Generate unique request ID and trace ID
        request_id = f"req_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        trace_id = f"trace_{uuid.uuid4().hex}"
        
        # Get observability manager
        obs_manager = get_observability_manager()
        
        # Inject observability context into request state
        request.state.request_id = request_id
        request.state.trace_id = trace_id
        request.state.observability = obs_manager
        
        # Extract user information if available
        user_id = None
        if hasattr(request.state, 'current_user') and request.state.current_user:
            user_id = getattr(request.state.current_user, 'id', None)
        
        # Start request correlation
        if obs_manager.observability:
            obs_manager.observability.correlate_request(request_id, user_id)
        
        # Track request start time
        start_time = time.time()
        
        # Extract request metadata
        request_metadata = {
            'request_id': request_id,
            'trace_id': trace_id,
            'method': request.method,
            'path': request.url.path,
            'query_params': str(request.query_params) if request.query_params else None,
            'client_ip': request.client.host if request.client else None,
            'user_agent': request.headers.get('user-agent'),
            'user_id': user_id
        }
        
        # Log request start
        if obs_manager.observability:
            obs_manager.observability.info(
                f"API Request Started: {request.method} {request.url.path}",
                request_metadata
            )
        
        response = None
        error = None
        
        try:
            # Use observability tracing for the entire request
            if obs_manager.observability:
                async def process_request(span):
                    return await call_next(request)
                
                response = await obs_manager.observability.trace(
                    f"api_request_{request.method.lower()}_{request.url.path.replace('/', '_')}",
                    process_request
                )
            else:
                response = await call_next(request)
                
        except Exception as e:
            error = e
            # Create error response
            response = Response(
                content='{"error": "Internal server error"}',
                status_code=500,
                media_type="application/json"
            )
            
            # Fire alert for server errors
            if obs_manager.alerting:
                await obs_manager.fire_alert(
                    name='api_server_error',
                    description=f'Server error in {request.method} {request.url.path}: {str(e)}',
                    severity='error',
                    labels={
                        'service': 'isp-framework',
                        'endpoint': request.url.path,
                        'method': request.method,
                        'error_type': type(e).__name__
                    },
                    annotations={
                        'request_id': request_id,
                        'user_id': user_id or 'anonymous',
                        'error_message': str(e)
                    }
                )
        
        finally:
            # Calculate request duration
            duration_ms = (time.time() - start_time) * 1000
            
            # Track API request metrics and logs
            obs_manager.track_api_request(
                method=request.method,
                path=request.url.path,
                status_code=response.status_code if response else 500,
                duration_ms=duration_ms,
                user_id=user_id,
                error=error
            )
            
            # Add observability headers to response
            if response:
                response.headers['X-Request-ID'] = request_id
                response.headers['X-Trace-ID'] = trace_id
                
                # Log request completion
                if obs_manager.observability:
                    completion_metadata = {
                        **request_metadata,
                        'status_code': response.status_code,
                        'duration_ms': duration_ms,
                        'response_size': len(response.body) if hasattr(response, 'body') else None
                    }
                    
                    if error:
                        obs_manager.observability.error(
                            f"API Request Failed: {request.method} {request.url.path}",
                            error,
                            completion_metadata
                        )
                    elif response.status_code >= 400:
                        obs_manager.observability.warn(
                            f"API Request Error: {request.method} {request.url.path}",
                            completion_metadata
                        )
                    else:
                        obs_manager.observability.info(
                            f"API Request Completed: {request.method} {request.url.path}",
                            completion_metadata
                        )
                
                # Check for performance alerts
                if duration_ms > 5000:  # 5 second threshold
                    await obs_manager.fire_alert(
                        name='slow_api_request',
                        description=f'Slow API request: {request.method} {request.url.path} took {duration_ms:.2f}ms',
                        severity='warning',
                        labels={
                            'service': 'isp-framework',
                            'endpoint': request.url.path,
                            'method': request.method,
                            'performance': 'slow'
                        },
                        annotations={
                            'request_id': request_id,
                            'duration_ms': str(duration_ms),
                            'threshold_ms': '5000'
                        }
                    )
        
        return response
