# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)

try:
    # Agent5 compatibility layer
    from checks import AgentCheck, PrometheusCheck
except ImportError:
    from .base import AgentCheck
    from .prometheus_check import PrometheusCheck

__all__ = [
    'AgentCheck',
    'PrometheusCheck'
]
