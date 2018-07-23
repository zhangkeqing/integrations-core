# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)

from fnmatch import fnmatchcase
import logging
import requests
from urllib3 import disable_warnings
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from math import isnan, isinf
from prometheus_client.parser import text_fd_to_metric_families

# toolkit
from .. import AgentCheck

class PrometheusScraperMixin(object):
    # pylint: disable=E1101
    # This class is not supposed to be used by itself, it provides scraping behavior but
    # need to be within a check in the end

    REQUESTS_CHUNK_SIZE = 1024 * 10  # use 10kb as chunk size when using the Stream feature in requests.get
    SAMPLE_NAME = 0
    SAMPLE_LABELS = 1
    SAMPLE_VALUE = 2

    def __init__(self, *args, **kwargs):
        super(PrometheusScraperMixin, self).__init__(*args, **kwargs)

        # The scraper needs its own logger
        self.log = logging.getLogger(__name__)

        self.METRIC_TYPES = ['counter', 'gauge', 'summary', 'histogram']

        # `NAMESPACE` is the prefix metrics will have. Need to be hardcoded in the
        # child check class.
        self.NAMESPACE = ''

        # `metrics_mapper` is a dictionary where the keys are the metrics to capture
        # and the values are the corresponding metrics names to have in datadog.
        # Note: it is empty in the parent class but will need to be
        # overloaded/hardcoded in the final check not to be counted as custom metric.
        self.metrics_mapper = {}

        # `rate_metrics` contains the metrics that should be sent as rates
        self.rate_metrics = []

        # `_metrics_wildcards` holds the potential wildcards to match for metrics
        self._metrics_wildcards = None

        # `prometheus_metrics_prefix` allows to specify a prefix that all
        # prometheus metrics should have. This can be used when the prometheus
        # endpoint we are scrapping allows to add a custom prefix to it's
        # metrics.
        self.prometheus_metrics_prefix = ''

        # `label_joins` holds the configuration for extracting 1:1 labels from
        # a target metric to all metric matching the label, example:
        # self.label_joins = {
        #     'kube_pod_info': {
        #         'label_to_match': 'pod',
        #         'labels_to_get': ['node', 'host_ip']
        #     }
        # }
        self.label_joins = {}

        # `_label_mapping` holds the additionals label info to add for a specific
        # label value, example:
        # self._label_mapping = {
        #     'pod': {
        #         'dd-agent-9s1l1': [("node","yolo"),("host_ip","yey")]
        #     }
        # }
        self._label_mapping = {}

        # `_active_label_mapping` holds a dictionary of label values found during the run
        # to cleanup the label_mapping of unused values, example:
        # self._active_label_mapping = {
        #     'pod': {
        #         'dd-agent-9s1l1': True
        #     }
        # }
        self._active_label_mapping = {}

        # `_watched_labels` holds the list of label to watch for enrichment
        self._watched_labels = set()

        self._dry_run = True

        # Some metrics are ignored because they are duplicates or introduce a
        # very high cardinality. Metrics included in this list will be silently
        # skipped without a 'Unable to handle metric' debug line in the logs
        self.ignore_metrics = []

        # If the `labels_mapper` dictionary is provided, the metrics labels names
        # in the `labels_mapper` will use the corresponding value as tag name
        # when sending the gauges.
        self.labels_mapper = {}

        # `exclude_labels` is an array of labels names to exclude. Those labels
        # will just not be added as tags when submitting the metric.
        self.exclude_labels = []

        # `type_overrides` is a dictionary where the keys are prometheus metric names
        # and the values are a metric type (name as string) to use instead of the one
        # listed in the payload. It can be used to force a type on untyped metrics.
        # Note: it is empty in the parent class but will need to be
        # overloaded/hardcoded in the final check not to be counted as custom metric.
        self.type_overrides = {}

        # Some metrics are retrieved from differents hosts and often
        # a label can hold this information, this transfers it to the hostname
        self.label_to_hostname = None

        # Add a "health" service check for the prometheus endpoint
        self.health_service_check = False

        # Can either be only the path to the certificate and thus you should specify the private key
        # or it can be the path to a file containing both the certificate & the private key
        self.ssl_cert = None

        # Needed if the certificate does not include the private key
        #
        # /!\ The private key to your local certificate must be unencrypted.
        # Currently, Requests does not support using encrypted keys.
        self.ssl_private_key = None

        # The path to the trusted CA used for generating custom certificates
        self.ssl_ca_cert = None

        # Extra http headers to be sent when polling endpoint
        self.extra_headers = {}

        # Timeout used during the network request
        self.prometheus_timeout = 10

        # List of strings to filter the input text payload on. If any line contains
        # one of these strings, it will be filtered out before being parsed.
        # INTERNAL FEATURE, might be removed in future versions
        self._text_filter_blacklist = []

    def parse_metric_family(self, response):
        """
        Parse the MetricFamily from a valid requests.Response object to provide a MetricFamily object (see [0])

        The text format uses iter_lines() generator.

        :param response: requests.Response
        :return: TBD
        """
        input_gen = response.iter_lines(chunk_size=self.REQUESTS_CHUNK_SIZE)
        if self._text_filter_blacklist:
            input_gen = self._text_filter_input(input_gen)

        for metric in text_fd_to_metric_families(input_gen):
            metric.type = self.type_overrides.get(metric.name, metric.type)
            if metric.type not in self.METRIC_TYPES:
                continue
            metric.name = self.remove_metric_prefix(metric.name)
            yield metric

    def _text_filter_input(self, input_gen):
        """
        Filters out the text input line by line to avoid parsing and processing
        metrics we know we don't want to process. This only works on `text/plain`
        payloads, and is an INTERNAL FEATURE implemented for the kubelet check
        :param input_get: line generator
        :output: generator of filtered lines
        """
        for line in input_gen:
            for item in self._text_filter_blacklist:
                if item in line:
                    break
            else:
                # No blacklist matches, passing the line through
                yield line

    def remove_metric_prefix(self, metric):
        return metric[len(self.prometheus_metrics_prefix):] if metric.startswith(self.prometheus_metrics_prefix) else metric

    def scrape_metrics(self, endpoint):
        """
        Poll the data from prometheus and return the metrics as a generator.
        """
        response = self.poll(endpoint)
        try:
            # no dry run if no label joins
            if not self.label_joins:
                self._dry_run = False
            elif not self._watched_labels:
                # build the _watched_labels set
                for metric, val in self.label_joins.iteritems():
                    self._watched_labels.add(val['label_to_match'])

            for metric in self.parse_metric_family(response):
                yield metric

            # Set dry run off
            self._dry_run = False
            # Garbage collect unused mapping and reset active labels
            for metric, mapping in self._label_mapping.items():
                for key, val in mapping.items():
                    if key not in self._active_label_mapping[metric]:
                        del self._label_mapping[metric][key]
            self._active_label_mapping = {}
        finally:
            response.close()

    def process(self, endpoint, **kwargs):
        """
        Polls the data from prometheus and pushes them as gauges
        `endpoint` is the metrics endpoint to use to poll metrics from Prometheus

        Note that if the instance has a 'tags' attribute, it will be pushed
        automatically as additional custom tags and added to the metrics
        """
        instance = kwargs.get('instance')
        if instance:
            kwargs['custom_tags'] = instance.get('tags', [])

        for metric in self.scrape_metrics(endpoint):
            self.process_metric(metric, **kwargs)

    def store_labels(self, metric):
        # If targeted metric, store labels
        if metric.name in self.label_joins:
            matching_label = self.label_joins[metric.name]['label_to_match']
            for sample in metric.samples:
                labels_list = []
                matching_value = None
                for label_name, label_value in sample[self.SAMPLE_LABELS].iteritems():
                    if label_name == matching_label:
                        matching_value = label_value
                    elif label_name in self.label_joins[metric.name]['labels_to_get']:
                        labels_list.append((label_name, label_value))
                try:
                    self._label_mapping[matching_label][matching_value] = labels_list
                except KeyError:
                    if matching_value is not None:
                        self._label_mapping[matching_label] = {matching_value: labels_list}

    def join_labels(self, metric):
        # Filter metric to see if we can enrich with joined labels
        if self.label_joins:
            for sample in metric.samples:
                for label_name in self._watched_labels.intersection(set(sample[self.SAMPLE_LABELS].keys())):
                    # Set this label value as active
                    if label_name not in self._active_label_mapping:
                        self._active_label_mapping[label_name] = {}
                    self._active_label_mapping[label_name][sample[self.SAMPLE_LABELS][label_name]] = True
                    # If mapping found add corresponding labels
                    try:
                        for label_tuple in self._label_mapping[label_name][sample[self.SAMPLE_LABELS][label_name]]:
                            sample[self.SAMPLE_LABELS][label_tuple[0]] = label_tuple[1]
                    except KeyError:
                        pass

    def process_metric(self, metric, **kwargs):
        """
        Handle a prometheus metric according to the following flow:
            - search self.metrics_mapper for a prometheus.metric <--> datadog.metric mapping
            - call check method with the same name as the metric
            - log some info if none of the above worked

        `send_histograms_buckets` is used to specify if yes or no you want to send the buckets as tagged values when dealing with histograms.
        """

        # If targeted metric, store labels
        self.store_labels(metric)

        if metric.name in self.ignore_metrics:
            return  # Ignore the metric

        # Filter metric to see if we can enrich with joined labels
        self.join_labels(metric)

        send_histograms_buckets = kwargs.get('send_histograms_buckets', True)
        send_monotonic_counter = kwargs.get('send_monotonic_counter', False)
        custom_tags = kwargs.get('custom_tags')
        ignore_unmapped = kwargs.get('ignore_unmapped', False)

        try:
            if not self._dry_run:
                try:
                    self._submit(self.metrics_mapper[metric.name], metric, send_histograms_buckets, send_monotonic_counter, custom_tags)
                except KeyError:
                    if not ignore_unmapped:
                        # call magic method (non-generic check)
                        handler = getattr(self, metric.name)  # Lookup will throw AttributeError if not found
                        try:
                            handler(metric, **kwargs)
                        except Exception as err:
                            self.log.warning("Error handling metric: {} - error: {}".format(metric.name, err))
                    else:
                        # build the wildcard list if first pass
                        if self._metrics_wildcards is None:
                            self._metrics_wildcards = [x for x in self.metrics_mapper.keys() if '*' in x]
                        # try matching wildcard (generic check)
                        for wildcard in self._metrics_wildcards:
                            if fnmatchcase(metric.name, wildcard):
                                self._submit(metric.name, metric, send_histograms_buckets, send_monotonic_counter, custom_tags)

        except AttributeError as err:
            self.log.debug("Unable to handle metric: {} - error: {}".format(metric.name, err))

    def poll(self, endpoint, headers=None):
        """
        Polls the metrics from the prometheus metrics endpoint provided in text format.
        Custom headers can be added to the default headers.

        Returns a valid requests.Response, raise requests.HTTPError if the status code of the requests.Response
        isn't valid - see response.raise_for_status()

        The caller needs to close the requests.Response

        :param endpoint: string url endpoint
        :param headers: extra headers
        :return: requests.Response
        """
        if headers is None:
            headers = {}
        if 'accept-encoding' not in headers:
            headers['accept-encoding'] = 'gzip'
        headers.update(self.extra_headers)
        cert = None
        if isinstance(self.ssl_cert, basestring):
            cert = self.ssl_cert
            if isinstance(self.ssl_private_key, basestring):
                cert = (self.ssl_cert, self.ssl_private_key)
        verify = True
        if isinstance(self.ssl_ca_cert, basestring):
            verify = self.ssl_ca_cert
        elif self.ssl_ca_cert is False:
            disable_warnings(InsecureRequestWarning)
            verify = False
        try:
            response = requests.get(endpoint, headers=headers, stream=True, timeout=self.prometheus_timeout, cert=cert, verify=verify)
        except requests.exceptions.SSLError:
            self.log.error("Invalid SSL settings for requesting {} endpoint".format(endpoint))
            raise
        except IOError:
            if self.health_service_check:
                self._submit_service_check(
                    "{}{}".format(self.NAMESPACE, ".prometheus.health"),
                    AgentCheck.CRITICAL,
                    tags=["endpoint:" + endpoint]
                )
            raise
        try:
            response.raise_for_status()
            if self.health_service_check:
                self._submit_service_check(
                    "{}{}".format(self.NAMESPACE, ".prometheus.health"),
                    AgentCheck.OK,
                    tags=["endpoint:" + endpoint]
                )
            return response
        except requests.HTTPError:
            response.close()
            if self.health_service_check:
                self._submit_service_check(
                    "{}{}".format(self.NAMESPACE, ".prometheus.health"),
                    AgentCheck.CRITICAL,
                    tags=["endpoint:" + endpoint]
                )
            raise

    def _submit(self, metric_name, metric, send_histograms_buckets=True, send_monotonic_counter=False, custom_tags=None, hostname=None):
        """
        For each sample in the metric, report it as a gauge with all labels as tags
        except if a labels dict is passed, in which case keys are label names we'll extract
        and corresponding values are tag names we'll use (eg: {'node': 'node'}).

        Histograms generate a set of values instead of a unique metric.
        send_histograms_buckets is used to specify if yes or no you want to
            send the buckets as tagged values when dealing with histograms.

        `custom_tags` is an array of 'tag:value' that will be added to the
        metric when sending the gauge to Datadog.
        """
        if metric.type in ["gauge", "counter"]:
            for sample in metric.samples:
                custom_hostname = self._get_hostname(hostname, sample)
                val = sample[self.SAMPLE_VALUE]
                if not self._is_value_valid(val):
                    self.log.debug("Metric value is not supported for metric {}".format(sample[self.SAMPLE_NAME]))
                    continue
                if metric.type == "counter" and send_monotonic_counter:
                    self._submit_monotonic_count(metric_name, val, sample, custom_tags, custom_hostname)
                elif sample[self.SAMPLE_NAME] in self.rate_metrics:
                    self._submit_rate(metric_name, val, sample, custom_tags, custom_hostname)
                else:
                    self._submit_gauge(metric_name, val, sample, custom_tags, custom_hostname)
        elif metric.type == "histogram":
            self._submit_gauges_from_histogram(metric_name, metric, send_histograms_buckets, custom_tags, hostname)
        elif metric.type == "summary":
            self._submit_gauges_from_summary(metric_name, metric, custom_tags, hostname)
        else:
            self.log.error("Metric type {} unsupported for metric {}.".format(metric.type, metric_name))

    def _get_hostname(self, hostname, sample):
        """
        If hostname is None, look at label_to_hostname setting
        """
        if hostname is None and self.label_to_hostname is not None:
            if self.label_to_hostname in sample[self.SAMPLE_LABELS]:
                return sample[self.SAMPLE_LABELS][self.label_to_hostname]

        return hostname

    def _finalize_tags_to_submit(self, _tags, metric_name, val, metric, custom_tags=None, hostname=None):
        """
        Format the finalized tags
        This is generally a noop, but it can be used to hook into _submit_gauge and change the tags before sending
        """
        return _tags

    def _submit_gauges_from_summary(self, name, metric, custom_tags=None, hostname=None):
        """
        Extracts metrics from a prometheus summary metric and sends them as gauges
        The python client does not expose/store quantile for now
        """
        if custom_tags is None:
            custom_tags = []
        for sample in metric.samples:
            custom_hostname = self._get_hostname(hostname, sample)
            val = sample[self.SAMPLE_VALUE]
            if not self._is_value_valid(val):
                self.log.debug("Metric value is not supported for metric {}".format(sample[self.SAMPLE_NAME]))
                continue
            if sample[self.SAMPLE_NAME].endswith("_sum"):
                self._submit_gauge("{}.sum".format(name), val, sample, custom_tags, custom_hostname)
            elif sample[self.SAMPLE_NAME].endswith("_count"):
                self._submit_gauge("{}.count".format(name), val, sample, custom_tags, custom_hostname)

    def _submit_gauges_from_histogram(self, name, metric, send_histograms_buckets=True, custom_tags=None, hostname=None):
        """
        Extracts metrics from a prometheus histogram and sends them as gauges
        """
        if custom_tags is None:
            custom_tags = []
        for sample in metric.samples:
            custom_hostname = self._get_hostname(hostname, sample)
            val = sample[self.SAMPLE_VALUE]
            if not self._is_value_valid(val):
                self.log.debug("Metric value is not supported for metric {}".format(sample[self.SAMPLE_NAME]))
                continue
            if sample[self.SAMPLE_NAME].endswith("_sum"):
                self._submit_gauge("{}.sum".format(name), val, sample, custom_tags, custom_hostname)
            elif sample[self.SAMPLE_NAME].endswith("_count"):
                self._submit_gauge("{}.count".format(name), val, sample, custom_tags, custom_hostname)
            elif sample[self.SAMPLE_NAME].endswith("_bucket") and send_histograms_buckets:
                self._submit_gauge("{}.count".format(name), val, sample, custom_tags, custom_hostname)

    def _is_value_valid(self, val):
        return not (isnan(val) or isinf(val))

    def set_prometheus_timeout(self, instance, default_value=10):
        """ extract `prometheus_timeout` directly from the instance configuration """
        self.prometheus_timeout = instance.get('prometheus_timeout', default_value)
