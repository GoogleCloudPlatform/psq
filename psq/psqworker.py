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
import os
import sys

import click


def import_queue(location):
    module, attr = location.rsplit('.', 1)
    module = import_module(module)
    queue = getattr(module, attr)
    return queue


@click.command()
@click.option(
    '--path', '-p',
    help='Import path. By default, this is the current working directory.')
@click.option(
    '--single-threaded', is_flag=True,
    help='Run everything in a single thread, do not start worker processes.')
@click.option(
    '--workers', '-n', type=click.IntRange(1, None),
    help='Number of worker processes.')
@click.option(
    '--pid',
    help='Write the process ID to the specified file.')
@click.argument(
    'queue',
    nargs=1,
    required=True)
def main(path, single_threaded, workers, pid, queue):
    """
    Standalone PSQ worker.

    The queue argument must be the full importable path to a psq.Queue
    instance.

    Example usage:

        psqworker config.q

        psqworker --path /opt/app queues.fast

    """
    if pid:
        with open(os.path.expanduser(pid), "w") as f:
            f.write(str(os.getpid()))

    # temporary hack
    here = os.path.dirname(os.path.abspath(__file__))
    sys.path = [x for x in sys.path if not x == here]

    if not path:
        path = os.getcwd()

    sys.path.insert(0, path)

    queue = import_queue(queue)

    import psq

    if single_threaded:
        worker = psq.Worker(queue=queue)
    else:
        worker = psq.MultiprocessWorker(
            queue=queue,
            num_workers=workers)

    worker.listen()


if __name__ == '__main__':
    main()
