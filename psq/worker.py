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

import multiprocessing
import signal
import time

from .logger import logger
from .queue import Queue
from .utils import measure_time


class Worker(object):
    def __init__(self, queue='default'):
        if isinstance(queue, str):
            self.queue = Queue(name=queue)
        else:
            self.queue = queue

        self.storage = self.queue.storage
        self.tasks_per_poll = 1

    def listen(self):
        logger.info('Listening, press Ctrl+C to exit.')
        try:
            while True:

                tasks = self.queue.dequeue(
                    max=self.tasks_per_poll,
                    block=True)

                if not tasks:
                    continue

                for task in tasks:
                    logger.info('Received task {}'.format(task.id))
                    self.run_task(task)

        except KeyboardInterrupt:
            logger.info('Stopped listening for tasks.')

        self.queue.cleanup()

    def run_task(self, task):
        with measure_time() as summary, self.queue.queue_context():
            task.execute(self.queue)
            summary(task.summary())


class MultiprocessWorker(Worker):
    def __init__(self, queue='default', num_workers=None, *args, **kwargs):
        super(MultiprocessWorker, self).__init__(queue, *args, **kwargs)

        if not num_workers:
            num_workers = multiprocessing.cpu_count()

        self.pool = multiprocessing.Pool(
            processes=num_workers,
            initializer=_init_worker_process,
            initargs=(self.queue,))

        self.tasks_per_poll = num_workers

        logger.info('Started {} worker threads.'.format(num_workers))

        self._install_signal_handlers()

    def listen(self):
        super(MultiprocessWorker, self).listen()

        logger.info('Waiting for any running tasks to complete...')

        # At this point, the first keyboard interrupt caused self.pool.close()
        # to be called. This means that the workers will finish up any tasks
        # they've been assigned and exit. The loop below ensures that the other
        # processes are joined without blocking this thread from receiving
        # signals. This allows us to catch a *second* keyboard interrupt and
        # force exit.
        while multiprocessing.active_children():
            time.sleep(1)

        # This will return immediately because of the loop above.
        self.pool.join()

        logger.info('All tasks done, graceful shutdown complete.')

    def run_task(self, task):
        self.pool.apply_async(
            _execute_task_in_worker,
            (task,))

    def _install_signal_handlers(self):

        # Second interrupt causes forced shutdown via pool.terminate().
        def force_exit(signum, frame):
            logger.warning('Forced exit, terminating all active tasks.')
            self.pool.terminate()
            raise SystemExit()

        # First interrupt causes graceful shutdown via pool.close().
        def graceful_exit(signum, frame):
            signal.signal(signal.SIGINT, force_exit)
            signal.signal(signal.SIGTERM, force_exit)
            logger.warning('Attempting graceful shutdown. Pressing Ctrl+C'
                           ' again will cause a forced exit.')
            self.pool.close()
            raise KeyboardInterrupt()

        signal.signal(signal.SIGINT, graceful_exit)
        signal.signal(signal.SIGTERM, graceful_exit)


# Each worker needs access to the queue, so this global variable will be set
# by _init_worker_process and available in _execute_task_in_worker.
_worker_queue = None


def _init_worker_process(queue):
    # Ignore interrupts in this process. The main process will handle these
    # interrupts to allow a graceful shutdown.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    global _worker_queue
    _worker_queue = queue


def _execute_task_in_worker(task):
    # Get the queue assigned to this worker
    worker_name = multiprocessing.current_process().name

    with measure_time() as summary, _worker_queue.queue_context():
        task.execute(_worker_queue)
        summary('{} finished {}'.format(worker_name, task.summary()))
