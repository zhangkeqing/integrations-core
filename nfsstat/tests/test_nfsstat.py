# (C) Datadog, Inc. 2010-2017
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
import os

# 3p
import mock
import pytest

# project
from datadog_checks.nfsstat import NfsStatCheck

metrics = [
    'system.nfs.ops',
    'system.nfs.rpc_bklog',
    'system.nfs.read_per_op',
    'system.nfs.read.ops',
    'system.nfs.read_per_s',
    'system.nfs.read.retrans',
    'system.nfs.read.retrans.pct',
    'system.nfs.read.rtt',
    'system.nfs.read.exe',
    'system.nfs.write_per_op',
    'system.nfs.write.ops',
    'system.nfs.write_per_s',
    'system.nfs.write.retrans',
    'system.nfs.write.retrans.pct',
    'system.nfs.write.rtt',
    'system.nfs.write.exe',
]

@pytest.fixture(scope="module")
def patch_nfsiostat():
    file_name = os.path.join(os.path.dirname(__file__), 'fixtures', 'nfsiostat')
    with open(file_name):
        nfsiostat_data = file_name.read()

    p = mock.patch('datadog_checks.nfsstat.nfsstat.get_subprocess_output',
        return_value=nfsiostat_data,
        __name__="get_subprocess_output")
    yield p.start()

    p.stop()

@pytest.fixture
def nfsstatio_check():
    init_config = {
        'nfsiostat_path': '/opt/datadog-agent/embedded/sbin/nfsiostat'
    }
    return NfsStatCheck('nfsstat_check', init_config, {}, [{}])

@pytest.fixture
def aggregator():
    from datadog_checks.stubs import aggregator
    aggregator.reset()
    return aggregator

def test_check(aggregator, nfsstatio_check):
    """
    Testing Nfsstat check.
    """
    nfsstatio_check.check({})

    nfs_server_tag = 'nfs_server:192.168.34.1'
    nfs_export_tag = 'nfs_export:/exports/nfs/datadog/{0}'
    nfs_mount_tag = 'nfs_mount:/mnt/datadog/{0}'

    folder_names = ['two']

    # self.assertTrue(False)

    for metric in metrics:
        for folder in folder_names:
            tags = []
            tags.append(nfs_server_tag)
            tags.append(nfs_export_tag.format(folder))
            tags.append(nfs_mount_tag.format(folder))
            aggregator.assert_metric(metric, tags=tags)

    assert aggregator.metrics_asserted_pct == 100.0
