# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)

import subprocess
import stat
import os
import logging
import pytest

from . import common
from datadog_checks.dev import docker_run, TempDir, WaitFor
from datadog_checks.dev.utils import copy_path

log = logging.getLogger(__file__)


def wait_on_docker_logs(sentences):
    args = [
        'docker',
        'logs'
    ]
    out = subprocess.check_output(args)
    if any(str.encode(s) in out for s in sentences):
        return True
    log.info(out)
    return False


def get_container_ip(container_id_or_name):
    """
    Get a docker container's IP address from its id or name
    """
    args = [
        'docker', 'inspect',
        '-f', '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', container_id_or_name
    ]

    return subprocess.check_output(args).strip()


@pytest.fixture(scope="session")
def dd_environment():
    """
        Start the cassandra cluster with required configuration
    """
    env = os.environ
    compose_file = os.path.join(common.HERE, 'compose', 'docker-compose.yaml')
    env['CONTAINER_PORT'] = common.PORT

    # We need to restrict permission on the password file
    # Create a temporary file so if we have to run tests more than once on a machine
    # the original file's perms aren't modified
    with TempDir() as tmpdir:
        jmx_pass_file = os.path.join(common.HERE, "compose", 'jmxremote.password')
        copy_path(jmx_pass_file, tmpdir)
        temp_jmx_file = os.path.join(tmpdir, 'jmxremote.password')
        env['JMX_PASS_FILE'] = temp_jmx_file
        os.chmod(temp_jmx_file, stat.S_IRWXU)
        sentences = [
            'Listening for thrift clients',
            'Not starting RPC server as requested',
            "Created default superuser role 'cassandra'"
        ]
        with docker_run(
            compose_file,
            log_patterns=sentences
        ):
            cassandra_seed = get_container_ip("{}".format(common.CASSANDRA_CONTAINER_NAME))
            env['CASSANDRA_SEEDS'] = cassandra_seed.decode('utf-8')

            subprocess.check_call([
                "docker",
                "exec", common.CASSANDRA_CONTAINER_NAME,
                "cqlsh",
                "-e",
                "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':2}"
            ])
            yield
