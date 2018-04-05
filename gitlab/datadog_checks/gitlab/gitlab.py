# stdlib
import urlparse

# 3rd party
import requests

try:
    # Agent5 compatibility layer
    from datadog_checks.errors import CheckException
    from datadog_checks.checks.prometheus import Scraper, PrometheusCheck
except ImportError:
    from checks import CheckException
    from checks.prometheus_check import Scraper, PrometheusCheck

from config import _is_affirmative
from util import headers

class GitlabCheck(PrometheusCheck):

    # Readiness signals ability to serve traffic, liveness that Gitlab is healthy overall
    ALLOWED_SERVICE_CHECKS = ['readiness', 'liveness']
    EVENT_TYPE = SOURCE_TYPE_NAME = 'gitlab'
    DEFAULT_CONNECT_TIMEOUT = 5
    DEFAULT_RECEIVE_TIMEOUT = 15

    PROMETHEUS_CONFIG = {
        'core': {
            'namespace': 'gitlab.core',
            'service_check_name': 'gitlab.prometheus_core_endpoint_up',
            'allowed_metrics': [],
            'ignore_unmapped': False
        },
        'internal': {
            'namespace': 'gitlab.internal',
            'service_check_name': 'gitlab.prometheus_internal_endpoint_up',
            'allowed_metrics': [
                'gitlab_find_commit_real_duration_seconds_count',
                'gitlab_rails_queue_duration_seconds_count',
                'gitlab_sql_duration_seconds_count',
                'gitlab_transaction_duration_seconds_count',
                'http_requests_total',
                'job_queue_duration_seconds_count',
                'pipelines_created_total',
                'unicorn_queued_connections'
            ],
            'ignore_unmapped': True
        },
        'database': {
            'namespace': 'gitlab.database',
            'service_check_name': 'gitlab.prometheus_database_endpoint_up',
            'allowed_metrics': [
                'ci_created_builds',
                'ci_running_builds',
                'gitlab_database_rows'
            ],
            'ignore_unmapped': True
        },
        'sidekiq': {
            'namespace': 'gitlab.sidekiq',
            'service_check_name': 'gitlab.prometheus_sidekiq_endpoint_up',
            'allowed_metrics': [
                'sidekiq_queue_size',
                'sidekiq_queue_latency',
                'sidekiq_running_jobs_count',
                'sidekiq_dead_jobs_total'
            ],
            'ignore_unmapped': True
        }
    }

    """
    Collect Gitlab metrics from Prometheus and validates that the connectivity with Gitlab
    """
    def __init__(self, name, init_config, agentConfig, instances=None):
        super(GitlabCheck, self).__init__(name, init_config, agentConfig, instances)
        self._scrapers = {}

    def check(self, instance):
        for metrics_type, config in self.PROMETHEUS_CONFIG.iteritems():
            self._process_metrics(instance, metrics_type, config)

        #### Service check to check Gitlab's health endpoints
        for check_type in self.ALLOWED_SERVICE_CHECKS:
            self._check_health_endpoint(instance, check_type)

    def _process_metrics(self, instance, metrics_type, config):
        # The endpoint is still configured via yml i.e., prometheus_core_endpoint
        config_key = "prometheus_%s_endpoint" % metrics_type
        endpoint = instance.get(config_key)
        if endpoint is None:
            raise CheckException("Unable to find %s in config file." % config_key)

        # By default we send the buckets
        scraper = self._get_scraper(instance, endpoint, config['namespace'], config['allowed_metrics'])

        try:
            scraper.process(endpoint, instance=instance, ignore_unmapped=config['ignore_unmapped'],
                            send_histograms_buckets=instance.get('send_histograms_buckets', True))
            self.service_check(config['service_check_name'], PrometheusCheck.OK)
        except requests.exceptions.ConnectionError as e:
            # Unable to connect to the metrics endpoint
            self.service_check(config['service_check_name'], PrometheusCheck.CRITICAL,
                               message="Unable to retrieve metrics from endpoint %s: %s" % (endpoint, e.message))

    def _get_scraper(self, instance, endpoint, namespace, metrics):
        """
        Grab the gitlab core scraper from the dict and return it if it exists,
        otherwise create the scraper and add it to the dict
        """
        if self._scrapers.get(endpoint, None):
            return self._scrapers.get(endpoint)

        scraper = Scraper(self)
        self._scrapers[endpoint] = scraper
        scraper.NAMESPACE = namespace
        # 1:1 mapping for now
        scraper.metrics_mapper = dict(zip(metrics, metrics))
        scraper.label_to_hostname = endpoint
        scraper = self._shared_scraper_config(scraper, instance)

        return scraper

    def _shared_scraper_config(self, scraper, instance):
        """
        Configuration that is shared by all the scrapers
        """
        scraper.labels_mapper = instance.get("labels_mapper", {})
        scraper.label_joins = instance.get("label_joins", {})
        scraper.type_overrides = instance.get("type_overrides", {})
        scraper.exclude_labels = instance.get("exclude_labels", [])
        # For simple values instance settings overrides optional defaults
        scraper.health_service_check = instance.get("health_service_check", True)
        scraper.ssl_cert = instance.get("ssl_cert", None)
        scraper.ssl_private_key = instance.get("ssl_private_key", None)
        scraper.ssl_ca_cert = instance.get("ssl_ca_cert", None)

        return scraper

    def _verify_ssl(self, instance):
        ## Load the ssl configuration
        ssl_params = {
            'ssl_cert_validation': _is_affirmative(instance.get('ssl_cert_validation', True)),
            'ssl_ca_certs': instance.get('ssl_ca_certs'),
        }

        for key, param in ssl_params.items():
            if param is None:
                del ssl_params[key]

        return ssl_params.get('ssl_ca_certs', True) if ssl_params['ssl_cert_validation'] else False

    def _service_check_tags(self, url):
        parsed_url = urlparse.urlparse(url)
        gitlab_host = parsed_url.hostname
        gitlab_port = 443 if parsed_url.scheme == 'https' else (parsed_url.port or 80)
        return ['gitlab_host:%s' % gitlab_host, 'gitlab_port:%s' % gitlab_port]

    # Validates an health endpoint
    #
    # Valid endpoints are:
    # - /-/readiness
    # - /-/liveness
    #
    # https://docs.gitlab.com/ce/user/admin_area/monitoring/health_check.html
    def _check_health_endpoint(self, instance, check_type, tags):
        if check_type not in self.ALLOWED_SERVICE_CHECKS:
            raise CheckException("Health endpoint %s is not a valid endpoint" % check_type)

        url = instance.get('gitlab_url')

        if url is None:
            # Simply ignore this service check if not configured
            self.log.debug("gitlab_url not configured, service check %s skipped" % check_type)
            return

        service_check_tags = self._service_check_tags(url)
        service_check_tags.extend(tags)
        verify_ssl = self._verify_ssl(instance)

        ## Timeout settings
        timeouts = (int(instance.get('connect_timeout', GitlabCheck.DEFAULT_CONNECT_TIMEOUT)),
                    int(instance.get('receive_timeout', GitlabCheck.DEFAULT_RECEIVE_TIMEOUT)))

        ## Auth settings
        auth = None
        if 'gitlab_user' in instance and 'gitlab_password' in instance:
            auth = (instance['gitlab_user'], instance['gitlab_password'])

        # These define which endpoint is hit and which type of check is actually performed
        # TODO: parse errors and report for single sub-service failure?
        service_check_name = "gitlab.%s" % check_type
        check_url = "%s/-/%s" % (url, check_type)

        try:
            self.log.debug('checking %s against %s' % (check_type, check_url))
            r = requests.get(check_url, auth=auth, verify=verify_ssl, timeout=timeouts,
                             headers=headers(self.agentConfig))
            if r.status_code != 200:
                self.service_check(service_check_name, PrometheusCheck.CRITICAL,
                                   message="Got %s when hitting %s" % (r.status_code, check_url),
                                   tags=service_check_tags)
                raise Exception("Http status code {0} on check_url {1}".format(r.status_code, check_url))
            else:
                r.raise_for_status()

        except requests.exceptions.Timeout:
            # If there's a timeout
            self.service_check(service_check_name, PrometheusCheck.CRITICAL,
                               message="Timeout when hitting %s" % check_url,
                               tags=service_check_tags)
            raise
        except Exception as e:
            self.service_check(service_check_name, PrometheusCheck.CRITICAL,
                               message="Error hitting %s. Error: %s" % (check_url, e.message),
                               tags=service_check_tags)
            raise
        else:
            self.service_check(service_check_name, PrometheusCheck.OK, tags=service_check_tags)
        self.log.debug("gitlab check %s succeeded" % check_type)
