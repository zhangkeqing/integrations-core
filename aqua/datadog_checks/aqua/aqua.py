# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
import requests
import simplejson as json
from urlparse import urljoin

from datadog_checks.checks import AgentCheck


SEVERITIES = {
    'total': 'all',
    'high': 'high',
    'medium': 'medium',
    'ok': 'ok',
    'low': 'low'
}


class AquaCheck(AgentCheck):
    """
    Collect metrics from Aqua.
    """
    def check(self, instance):
        self.validate_instance(instance)
        try:
            token = self.get_aqua_token(instance)
        except Exception as ex:
            self.log.error("Failed to get Aqua token, skipping check. Error: %s" % ex)
            return
        self._report_base_metrics(instance, token)
        self._report_audits(instance, token)
        self._report_scan_queues(instance, token)
        self._report_connected_enforcers(instance, token)


    def validate_instance(self, instance):
        """
        Validate that all required parameters are set in the instance.
        """
        if any(map(lambda x: x not in instance, ['api_user', 'password', 'url'])):
            raise Exception("Aqua instance missing one of api_user, password, or url")

    def get_aqua_token(self, instance):
        """
        Retrieve the Aqua token for next queries.
        """
        headers = {'Content-Type': 'application/json','charset':'UTF-8'}
        data={ "id": instance['api_user'], "password": instance['password'] }
        res = requests.post(instance['url'] + '/api/v1/login', data=json.dumps(data),headers=headers, timeout=self.default_integration_http_timeout)
        res.raise_for_status()
        return json.loads(res.text)['token']

    def _perform_query(self, instance, route, token):
        """
        Form queries and interact with the Aqua API.
        """
        headers = {'Content-Type': 'application/json','charset':'UTF-8','Authorization': 'Bearer '+ token}
        res = requests.get(urljoin(instance['url'], route), headers=headers, timeout=60)
        res.raise_for_status()
        return json.loads(res.text)

    def _report_base_metrics(self, instance, token):
        """
        Report metrics about images, vulnerabilities, running containers, and enforcer hosts
        """
        metrics = self._perform_query(instance, '/api/v1/dashboard', token)
        # images
        # FIXME: haissam registry_counts is weird in these metric names
        # FIXME: haissam factorize these
        metric_name = 'aqua.registry_counts.images'
        image_metrics = metrics['registry_counts']['images']
        for sev, sev_tag in SEVERITIES.iteritems():
            self.gauge(metric_name, image_metrics[sev], tags=instance.get('tags', []) + ['severity:%s' % sev_tag])
        # vulnerabilities
        metric_name = 'aqua.registry_counts.vulnerabilities'
        vuln_metrics = metrics['registry_counts']['vulnerabilities']
        for sev, sev_tag in SEVERITIES.iteritems():
            self.gauge(metric_name, vuln_metrics[sev], tags=instance.get('tags', []) + ['severity:%s' % sev_tag])
        # running containers
        metric_name = 'aqua.running_containers'
        container_metrics = metrics['running_containers']
        # FIXME: haissam is the tagging and substraction logic legit here?
        self.gauge(metric_name, container_metrics['total'], tags=instance.get('tags', []) + ['status:all'])
        self.gauge(metric_name, container_metrics['unregistered'], tags=instance.get('tags', []) + ['status:unregistered'])
        self.gauge(metric_name, container_metrics['total'] - container_metrics['unregistered'], tags=instance.get('tags', []) + ['status:registered'])
        # disconnected enforcers
        # FIXME: haissam should we move this to the dedicated enforcer method?
        metric_name = 'aqua.enforcers'
        enforcer_metrics = metrics['hosts']
        self.gauge('aqua.enforcers', enforcer_metrics['disconnected_count'], tags=instance.get('tags', []) + ['status:disconnected'])

    def _report_connected_enforcers(self, instance, token):
        """
        Report metrics about enforcers
        """
        # FIXME: haissam is there more to collect here?
        metrics = self._perform_query(instance, '/api/v1/hosts', token)
        # FIXME: haissam is it all or connected here?
        self.gauge('aqua.enforcers', metrics['count'], tags=instance.get('tags', []) + ['status:all'])

    def _report_audits(self, instance, token):
        metrics = self._perform_query(instance, '/api/v1/audit/access_totals?alert=-1&limit=100&time=hour&type=all', token)
        metric_name = 'aqua.audit.access'
        status = {
            'total': 'all',
            'success': 'success',
            'blocked': 'blocked',
            'detect': 'detect',
            'alert': 'alert'
        }
        for status, status_tag in status.iteritems():
            self.gauge(metric_name, metrics[status], tags=instance.get('tags', []) + ['status:%s' % status_tag])

    def _report_scan_queues(self, instance, token):
        metrics = self._perform_query(instance, '/api/v1/scanqueue/summary', token)
        metric_name = 'aqua.scan_queue'
        status = {
            'total': 'all',
            'failed': 'failed',
            'in_progress': 'in_progress',
            'finished': 'finished',
            'pending': 'pending'
        }
        for status, status_tag in status.iteritems():
            self.gauge(metric_name, metrics[status], tags=instance.get('tags', []) + ['status:%s' % status_tag])
