#!/usr/bin/env python

# Copyright 2015 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

from importlib import import_module
import logging
import os
import sys

import click
from colorlog import ColoredFormatter

logger = logging.getLogger(__name__)


def setup_logging():  # pragma: no cover
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()

    formatter = ColoredFormatter(
        "%(log_color)s%(levelname)-8s%(reset)s %(asctime)s %(green)s%(name)s"
        "%(reset)s %(message)s",
        reset=True,
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'blue',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red,bg_white',
        }
    )

    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


def import_queue(location):
    module, attr = location.rsplit('.', 1)
    module = import_module(module)
    queue = getattr(module, attr)
    if hasattr(queue, '__call__'):
        queue = queue()
    return queue


@click.command()
@click.option(
    '--path', '-p',
    help='Import path. By default, this is the current working directory.')
@click.option(
    '--pid',
    help='Write the process ID to the specified file.')
@click.argument(
    'queue',
    nargs=1,
    required=True)
def main(path, pid, queue):
    """
    Standalone PSQ worker.

    The queue argument must be the full importable path to a psq.Queue
    instance.

    Example usage:

        psqworker config.q

        psqworker --path /opt/app queues.fast

    """
    setup_logging()

    if pid:
        with open(os.path.expanduser(pid), "w") as f:
            f.write(str(os.getpid()))

    if not path:
        path = os.getcwd()

    sys.path.insert(0, path)

    queue = import_queue(queue)

    import psq

    worker = psq.Worker(queue=queue)

    worker.listen()


if __name__ == '__main__':
    main()
