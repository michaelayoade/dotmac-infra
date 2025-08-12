"""
Observability Manager for FastAPI Backend
Centralized management of observability, monitoring, and alerting SDKs
"""

import os
import asyncio
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

# Mock SDK imports for now - will be replaced with actual SDKs when TypeScript builds are fixed
class MockObservabilityClient:
    def __init__(self, config):
        self.config = config
        self.connected = False
    
    async def connect(self): 
        self.connected = True
    
    async def disconnect(self): 
        self.connected = False
    
    def info(self, message: str, metadata: Dict[str, Any] = None):
        print(f"[INFO] {message} | {metadata or {}}")
    
    def warn(self, message: str, metadata: Dict[str, Any] = None):
        print(f"[WARN] {message} | {metadata or {}}")
    
    def error(self, message: str, error: Exception = None, metadata: Dict[str, Any] = None):
        print(f"[ERROR] {message} | Error: {error} | {metadata or {}}")
    
    def correlate_request(self, request_id: str, user_id: str = None):
        print(f"[TRACE] Request correlated: {request_id} | User: {user_id}")
    
    async def trace(self, operation_name: str, fn):
        print(f"[TRACE] Starting: {operation_name}")
        try:
            result = await fn(None)  # Mock span
            print(f"[TRACE] Completed: {operation_name}")
            return result
        except Exception as e:
            print(f"[TRACE] Failed: {operation_name} | Error: {e}")
            raise

class MockMonitoringClient:
    def __init__(self, config):
        self.config = config
        self.connected = False
        self.metrics = {}
    
    async def connect(self): 
        self.connected = True
    
    async def disconnect(self): 
        self.connected = False
    
    def create_counter(self, name: str, description: str, labels: Dict[str, str] = None):
        self.metrics[name] = {'type': 'counter', 'value': 0, 'labels': labels or {}}
        return self.metrics[name]
    
    def create_gauge(self, name: str, description: str, labels: Dict[str, str] = None):
        self.metrics[name] = {'type': 'gauge', 'value': 0, 'labels': labels or {}}
        return self.metrics[name]
    
    def create_histogram(self, name: str, description: str, buckets: list, labels: Dict[str, str] = None):
        self.metrics[name] = {'type': 'histogram', 'buckets': buckets, 'labels': labels or {}}
        return self.metrics[name]
    
    def increment_counter(self, name: str, value: float = 1, labels: Dict[str, str] = None):
        if name in self.metrics:
            self.metrics[name]['value'] += value
            print(f"[METRIC] Counter {name} incremented by {value} | Labels: {labels or {}}")
    
    def set_gauge(self, name: str, value: float, labels: Dict[str, str] = None):
        if name in self.metrics:
            self.metrics[name]['value'] = value
            print(f"[METRIC] Gauge {name} set to {value} | Labels: {labels or {}}")
    
    def observe_histogram(self, name: str, value: float, labels: Dict[str, str] = None):
        if name in self.metrics:
            print(f"[METRIC] Histogram {name} observed {value} | Labels: {labels or {}}")
    
    def register_health_check(self, check):
        print(f"[HEALTH] Registered health check: {check['name']}")
    
    async def run_all_health_checks(self):
        return {
            'status': 'healthy',
            'checks': [],
            'timestamp': '2025-01-01T00:00:00Z',
            'uptime': 3600,
            'version': '1.0.0',
            'environment': 'production'
        }

class MockAlertingClient:
    def __init__(self, config):
        self.config = config
        self.connected = False
        self.alerts = []
    
    async def connect(self): 
        self.connected = True
    
    async def disconnect(self): 
        self.connected = False
    
    async def create_alert_rule(self, rule):
        print(f"[ALERT] Created alert rule: {rule['name']}")
        return rule
    
    async def fire_alert(self, alert):
        self.alerts.append(alert)
        print(f"[ALERT] Alert fired: {alert['name']} | Severity: {alert['severity']}")
        return alert
    
    async def create_notification_config(self, config):
        print(f"[ALERT] Created notification config: {config['name']} | Channel: {config['channel']}")
        return config


class ObservabilityManager:
    """Centralized manager for all observability capabilities"""
    
    def __init__(self):
        self.observability: Optional[MockObservabilityClient] = None
        self.monitoring: Optional[MockMonitoringClient] = None
        self.alerting: Optional[MockAlertingClient] = None
        self._initialized = False
        self._startup_metrics_created = False
    
    async def initialize(self):
        """Initialize all observability SDKs"""
        if self._initialized:
            return
        
        # Configuration from environment
        tenant_id = os.getenv('TENANT_ID', 'isp-framework')
        environment = os.getenv('ENVIRONMENT', 'production')
        
        # Initialize Observability SDK
        observability_config = {
            'tenantId': tenant_id,
            'environment': environment,
            'logging': {
                'level': 'info',
                'format': 'json',
                'outputs': [{'type': 'console', 'config': {}}],
                'structured': True,
                'includeStackTrace': True
            },
            'tracing': {
                'enabled': True,
                'sampleRate': 1.0,
                'maxSpans': 1000,
                'exportInterval': 5000,
                'exporters': [{'type': 'console'}]
            }
        }
        
        self.observability = MockObservabilityClient(observability_config)
        
        # Initialize Monitoring SDK
        monitoring_config = {
            'enabled': True,
            'metricsInterval': 10000,
            'healthCheckInterval': 30000,
            'retentionPeriod': 3600,
            'maxMetrics': 10000,
            'exporters': [
                {
                    'type': 'prometheus',
                    'endpoint': 'http://localhost:9090/metrics',
                    'interval': 15000,
                    'enabled': True
                }
            ]
        }
        
        self.monitoring = MockMonitoringClient(monitoring_config)
        
        # Initialize Alerting SDK
        alerting_config = {
            'enabled': True,
            'evaluationInterval': 30000,
            'groupWait': 10000,
            'groupInterval': 300000,
            'repeatInterval': 3600000,
            'maxRetries': 3,
            'retryDelay': 60000,
            'defaultSeverity': 'warning',
            'globalLabels': {
                'environment': environment,
                'service': 'isp-framework'
            }
        }
        
        self.alerting = MockAlertingClient(alerting_config)
        
        # Connect all SDKs
        await self.observability.connect()
        await self.monitoring.connect()
        await self.alerting.connect()
        
        # Setup core metrics and health checks
        await self._setup_core_observability()
        
        self._initialized = True
        
        self.observability.info("Observability Plane initialized successfully", {
            'tenant_id': tenant_id,
            'environment': environment,
            'sdks': ['observability', 'monitoring', 'alerting']
        })
    
    async def _setup_core_observability(self):
        """Setup core metrics, health checks, and alerts for ISP Framework"""
        if self._startup_metrics_created:
            return
        
        # ============================================================================
        # Core Business Metrics
        # ============================================================================
        
        # Customer metrics
        self.monitoring.create_counter('isp_customers_total', 'Total number of customers')
        self.monitoring.create_counter('isp_customer_registrations_total', 'Total customer registrations')
        self.monitoring.create_gauge('isp_active_customers', 'Number of active customers')
        self.monitoring.create_gauge('isp_revenue_mrr_cents', 'Monthly recurring revenue in cents')
        
        # Service metrics
        self.monitoring.create_counter('isp_services_activated_total', 'Total services activated')
        self.monitoring.create_gauge('isp_active_services', 'Number of active services')
        self.monitoring.create_counter('isp_service_failures_total', 'Total service failures')
        
        # API metrics
        self.monitoring.create_counter('isp_api_requests_total', 'Total API requests')
        self.monitoring.create_counter('isp_api_errors_total', 'Total API errors')
        self.monitoring.create_histogram('isp_api_request_duration_ms', 'API request duration', 
                                       [10, 50, 100, 500, 1000, 5000])
        
        # Billing metrics
        self.monitoring.create_counter('isp_billing_processed_total', 'Total billing transactions processed')
        self.monitoring.create_counter('isp_billing_failures_total', 'Total billing failures')
        self.monitoring.create_gauge('isp_billing_revenue_total_cents', 'Total billing revenue in cents')
        
        # Network metrics
        self.monitoring.create_gauge('isp_network_latency_ms', 'Network latency in milliseconds')
        self.monitoring.create_gauge('isp_network_packet_loss_percent', 'Network packet loss percentage')
        self.monitoring.create_gauge('isp_network_bandwidth_mbps', 'Network bandwidth in Mbps')
        self.monitoring.create_gauge('isp_network_uptime_percent', 'Network uptime percentage')
        
        # ============================================================================
        # Health Checks
        # ============================================================================
        
        # Database health check
        self.monitoring.register_health_check({
            'name': 'database_connection',
            'description': 'PostgreSQL database connectivity',
            'timeout': 5000,
            'interval': 30000,
            'retries': 3,
            'critical': True,
            'tags': ['database', 'critical']
        })
        
        # Redis health check
        self.monitoring.register_health_check({
            'name': 'redis_connection',
            'description': 'Redis cache connectivity',
            'timeout': 3000,
            'interval': 30000,
            'retries': 2,
            'critical': False,
            'tags': ['cache', 'redis']
        })
        
        # External services health check
        self.monitoring.register_health_check({
            'name': 'external_services',
            'description': 'External service dependencies',
            'timeout': 10000,
            'interval': 60000,
            'retries': 3,
            'critical': False,
            'tags': ['external', 'dependencies']
        })
        
        # ============================================================================
        # Alert Rules
        # ============================================================================
        
        # High API error rate alert
        await self.alerting.create_alert_rule({
            'name': 'high_api_error_rate',
            'description': 'API error rate exceeds 5%',
            'expression': 'api_error_rate > 5',
            'duration': 300000,  # 5 minutes
            'severity': 'error',
            'labels': {'service': 'isp-framework', 'type': 'availability'},
            'annotations': {
                'summary': 'High API error rate detected',
                'description': 'API error rate has exceeded 5% for more than 5 minutes'
            },
            'enabled': True,
            'tenantId': 'isp-framework',
            'groupBy': ['service', 'endpoint'],
            'evaluationInterval': 60000
        })
        
        # Database connectivity alert
        await self.alerting.create_alert_rule({
            'name': 'database_unavailable',
            'description': 'Database health check failing',
            'expression': 'database_health == "unhealthy"',
            'duration': 60000,  # 1 minute
            'severity': 'critical',
            'labels': {'service': 'isp-framework', 'type': 'infrastructure'},
            'annotations': {
                'summary': 'Database connectivity lost',
                'description': 'Database health check has been failing for more than 1 minute'
            },
            'enabled': True,
            'tenantId': 'isp-framework',
            'groupBy': ['service'],
            'evaluationInterval': 30000
        })
        
        # Revenue anomaly alert
        await self.alerting.create_alert_rule({
            'name': 'revenue_anomaly',
            'description': 'Significant drop in MRR detected',
            'expression': 'mrr_change_percent < -10',
            'duration': 1800000,  # 30 minutes
            'severity': 'warning',
            'labels': {'service': 'isp-framework', 'type': 'business'},
            'annotations': {
                'summary': 'Revenue anomaly detected',
                'description': 'Monthly recurring revenue has dropped by more than 10%'
            },
            'enabled': True,
            'tenantId': 'isp-framework',
            'groupBy': ['service'],
            'evaluationInterval': 300000
        })
        
        # Network performance alert
        await self.alerting.create_alert_rule({
            'name': 'network_degradation',
            'description': 'Network performance degraded',
            'expression': 'network_latency > 100 OR packet_loss > 1.0',
            'duration': 180000,  # 3 minutes
            'severity': 'warning',
            'labels': {'service': 'isp-framework', 'type': 'network'},
            'annotations': {
                'summary': 'Network performance degradation',
                'description': 'Network latency or packet loss has exceeded thresholds'
            },
            'enabled': True,
            'tenantId': 'isp-framework',
            'groupBy': ['service'],
            'evaluationInterval': 60000
        })
        
        # ============================================================================
        # Notification Channels
        # ============================================================================
        
        # Operations team Slack notifications
        await self.alerting.create_notification_config({
            'name': 'ops_team_slack',
            'channel': 'slack',
            'enabled': True,
            'settings': {
                'webhook': os.getenv('SLACK_WEBHOOK_URL', 'https://hooks.slack.com/services/...'),
                'channel': '#ops-alerts',
                'username': 'ISP Framework Monitor',
                'iconEmoji': ':warning:'
            },
            'filters': [
                {'field': 'severity', 'operator': 'equals', 'value': 'critical'},
                {'field': 'severity', 'operator': 'equals', 'value': 'error'}
            ],
            'tenantId': 'isp-framework'
        })
        
        # Business team email notifications
        await self.alerting.create_notification_config({
            'name': 'business_team_email',
            'channel': 'email',
            'enabled': True,
            'settings': {
                'to': ['business@ispframework.com', 'revenue@ispframework.com'],
                'subject': 'ISP Framework Business Alert: {{.Alert.Name}}',
                'template': 'business_alert_template'
            },
            'filters': [
                {'field': 'labels.type', 'operator': 'equals', 'value': 'business'}
            ],
            'tenantId': 'isp-framework'
        })
        
        self._startup_metrics_created = True
        
        self.observability.info("Core observability setup completed", {
            'metrics_created': 15,
            'health_checks': 3,
            'alert_rules': 4,
            'notification_channels': 2
        })
    
    async def shutdown(self):
        """Shutdown all observability SDKs"""
        if not self._initialized:
            return
        
        self.observability.info("Shutting down Observability Plane")
        
        if self.observability:
            await self.observability.disconnect()
        
        if self.monitoring:
            await self.monitoring.disconnect()
        
        if self.alerting:
            await self.alerting.disconnect()
        
        self._initialized = False
    
    @asynccontextmanager
    async def trace_operation(self, operation_name: str, metadata: Dict[str, Any] = None):
        """Context manager for tracing operations"""
        if not self.observability:
            yield None
            return
        
        async def trace_fn(span):
            yield span
        
        async for span in self.observability.trace(operation_name, trace_fn):
            yield span
    
    def track_api_request(self, method: str, path: str, status_code: int, duration_ms: float, 
                         user_id: str = None, error: Exception = None):
        """Track API request metrics and logs"""
        if not self._initialized:
            return
        
        # Update API metrics
        self.monitoring.increment_counter('isp_api_requests_total', 1, {
            'method': method,
            'path': path,
            'status_code': str(status_code)
        })
        
        if status_code >= 400:
            self.monitoring.increment_counter('isp_api_errors_total', 1, {
                'method': method,
                'path': path,
                'status_code': str(status_code)
            })
        
        self.monitoring.observe_histogram('isp_api_request_duration_ms', duration_ms, {
            'method': method,
            'path': path
        })
        
        # Log request
        if error:
            self.observability.error(f"API request failed: {method} {path}", error, {
                'method': method,
                'path': path,
                'status_code': status_code,
                'duration_ms': duration_ms,
                'user_id': user_id
            })
        else:
            self.observability.info(f"API request completed: {method} {path}", {
                'method': method,
                'path': path,
                'status_code': status_code,
                'duration_ms': duration_ms,
                'user_id': user_id
            })
    
    async def fire_alert(self, name: str, description: str, severity: str, 
                        labels: Dict[str, str] = None, annotations: Dict[str, str] = None):
        """Fire an alert"""
        if not self.alerting:
            return
        
        alert = {
            'name': name,
            'description': description,
            'severity': severity,
            'source': 'isp-framework',
            'tenantId': 'isp-framework',
            'labels': labels or {},
            'annotations': annotations or {}
        }
        
        return await self.alerting.fire_alert(alert)
    
    def get_health_status(self):
        """Get current health status"""
        if not self._initialized:
            return {'status': 'not_initialized'}
        
        return {
            'status': 'healthy',
            'observability': self.observability.connected if self.observability else False,
            'monitoring': self.monitoring.connected if self.monitoring else False,
            'alerting': self.alerting.connected if self.alerting else False,
            'initialized': self._initialized
        }


# Global observability manager instance
_observability_manager: Optional[ObservabilityManager] = None


def get_observability_manager() -> ObservabilityManager:
    """Get the global observability manager instance"""
    global _observability_manager
    if _observability_manager is None:
        _observability_manager = ObservabilityManager()
    return _observability_manager


async def initialize_observability():
    """Initialize the global observability manager"""
    manager = get_observability_manager()
    await manager.initialize()
    return manager


async def shutdown_observability():
    """Shutdown the global observability manager"""
    global _observability_manager
    if _observability_manager:
        await _observability_manager.shutdown()
        _observability_manager = None
