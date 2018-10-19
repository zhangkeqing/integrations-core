# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)

import os
import pytest
import requests
import subprocess

from datadog_checks.dev import docker_run, temp_dir
from datadog_checks.rabbitmq import RabbitMQ

from .common import HERE, CHECK_NAME, HOST, PORT, USERNAME, PASSWORD

COMPOSE_FILE = os.path.join(HERE, 'compose', 'docker-compose.yaml')


@pytest.fixture(scope="session")
def spin_up_rabbitmq(request):
    """
    Start a cluster with one master, one replica and one unhealthy replica and
    stop it after the tests are done.
    If there's any problem executing docker-compose, let the exception bubble
    up.
    """

    env = {}

    if not os.environ.get('RABBITMQ_VERSION'):
        env['RABBITMQ_VERSION'] = '3.6.0'

    # def down():
    #     pass

    with docker_run(COMPOSE_FILE,
                    log_patterns='Server startup complete',
                    env_vars=env):
        yield


@pytest.fixture(scope="session")
def setup_rabbitmq():
    with temp_dir() as tmpdir:
        url = 'http://{}:{}/cli/rabbitmqadmin'.format(HOST, PORT)
        res = requests.get(url)
        res.raise_for_status()

        rabbitmq_admin_script = os.path.join(tmpdir, 'rabbitmqadmin')
        with open(rabbitmq_admin_script, 'w+') as f:
            f.write(res.text)

        # if os.environ.get('RABBITMQ_VERSION') != '3.5.0':
        #     # the rabbitmqctl does not work in the 3.5.0 image for some reason
        #     setup_perms()
        setup_vhosts(rabbitmq_admin_script)
        setup_more(rabbitmq_admin_script)
        setup_more_with_vhosts(rabbitmq_admin_script)
        yield


def setup_vhosts(rabbitmq_admin_script):
    for vhost in ['myvhost', 'myothervhost']:
        cmd = ['python',
               rabbitmq_admin_script,
               'declare',
               'vhost',
               'name={}'.format(vhost)]
        subprocess.check_call(cmd)

        cmd = ['python',
               rabbitmq_admin_script,
               'declare',
               'permission',
               'vhost={}'.format(vhost),
               'user=guest',
               'write=.*',
               'read=.*',
               'configure=.*']
        subprocess.check_call(cmd)


def setup_more(rabbitmq_admin_script):
    for name in ['test1', 'test5', 'tralala']:
        cmd = ['python',
               rabbitmq_admin_script,
               'declare',
               'queue',
               'name={}'.format(name)]
        subprocess.check_call(cmd)

        cmd = ['python',
               rabbitmq_admin_script,
               'declare',
               'exchange',
               'name={}'.format(name),
               'type=topic']
        subprocess.check_call(cmd)

        cmd = ['python',
               rabbitmq_admin_script,
               'declare',
               'binding',
               'source={}'.format(name),
               'destination_type=queue',
               'destination={}'.format(name),
               'routing_key={}'.format(name)]
        subprocess.check_call(cmd)

        cmd = ['python',
               rabbitmq_admin_script,
               'publish',
               'exchange={}'.format(name),
               'routing_key={}'.format(name),
               'payload="hello, world"']
        subprocess.check_call(cmd)

        cmd = ['python',
               rabbitmq_admin_script,
               'publish',
               'exchange={}'.format(name),
               'routing_key=bad_key',
               'payload="unroutable"']
        subprocess.check_call(cmd)


def setup_more_with_vhosts(rabbitmq_admin_script):
    for name in ['test1', 'test5', 'tralala', 'testaaaaa', 'bbbbbbbb']:
        for vhost in ['myvhost', 'myothervhost']:
            cmd = ['python',
                   rabbitmq_admin_script,
                   '--vhost={}'.format(vhost),
                   'declare',
                   'queue',
                   'name={}'.format(name)]
            subprocess.check_call(cmd)

            cmd = ['python',
                   rabbitmq_admin_script,
                   '--vhost={}'.format(vhost),
                   'publish',
                   'exchange=amq.default',
                   'routing_key={}'.format(name),
                   'payload="hello, world"']
            subprocess.check_call(cmd)


def setup_perms():
    cmd_base = [
        'docker',
        # '-f',
        # COMPOSE_FILE,
        'exec',
        'compose_rabbitmq_1',
        'rabbitmqctl'
    ]

    add_user = cmd_base + [
        'add_user',
        USERNAME,
        PASSWORD
    ]

    subprocess.check_call(add_user)

    set_perms = cmd_base + [
        'set_permissions',
        '-p',
        '/',
        USERNAME,
        '"^aliveness-test$"',
        '"^amq\.default$"',
        '".*"'
    ]

    subprocess.check_call(set_perms)

    set_tags = cmd_base + [
        'set_user_tags',
        USERNAME,
        'monitoring'
    ]

    subprocess.check_call(set_tags)

# def setup_perms():
#     cmd_base= [
#         'docker-compose',
#         '-f',
#         COMPOSE_FILE,
#         'exec',
#         'rabbitmq',
#         'rabbitmqctl',
#         'bash',
#         '-c'
#     ]
#
#     add_user = cmd_base + [
#         '"add_user {} {}"'.format(USERNAME, PASSWORD)
#     ]
#
#     subprocess.check_call(add_user, shell=True)
#
#     set_perms = cmd_base + [
#         '\'set_permissions -p / datadog "^aliveness-test$" "^amq\.default$" ".*"\''
#     ]
#
#     subprocess.check_call(set_perms, shell=True)
#
#     set_tags = cmd_base + [
#         '"set_user_tags datadog monitoring"'
#     ]
#
#     subprocess.check_call(set_tags, shell=True)


@pytest.fixture
def check():
    return RabbitMQ(CHECK_NAME, {}, {})
