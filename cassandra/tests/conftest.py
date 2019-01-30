# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)

import os
import stat
import tarfile

import pytest

from datadog_checks.dev import docker_run, TempDir

from .common import HERE, URL, CONFIG


@pytest.fixture(scope="session")
def dd_environment():
    # use os.path.realpath to avoid mounting issues of symlinked /var -> /private/var in Docker on macOS
    with docker_run(
        compose_file=os.path.join(HERE, 'compose', 'docker-compose.yaml'),
        log_patterns=['Listening for thrift clients']
    ):
        yield CONFIG
