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

import importlib
import logging
import time

from six import string_types

from .utils import dumps
from .globals import current_queue, task_context


logger = logging.getLogger(__name__)


class Retry(Exception):
    """When raised within a task, the task will be re-queued"""
    pass


class TimeoutError(Exception):
    """The operation exceeded the given deadline."""
    pass


QUEUED = 'queued'
RETRYING = 'retrying'
FINISHED = 'finished'
FAILED = 'failed'
STARTED = 'started'

# Number of times the task is not acknowledged to trigger a retry in
ON_EXCEPTION_MAX_RETRIES_BEFORE_ACK_AND_GIVE_UP = 10


class Task(object):
    def __init__(self, id, f, args, kwargs):
        self.id = id
        self.f = f
        self.args = args
        self.kwargs = kwargs
        self.retries = 0
        self.reset()

        # these values will be set on dequeue if late acknowledgement is configured
        self.subscription = None
        """ :type: None|google.cloud.pubsub.Subscription """
        self.ack_id = ''

    def reset(self):
        self.status = QUEUED
        self.result = None
        self.retries = 0

    def retry(self):
        self.status = RETRYING
        self.retries += 1

    def start(self):
        self.status = STARTED
        self.result = None

    def finish(self, result):
        self.status = FINISHED
        self.result = result

    def fail(self, exception):
        self.status = FAILED
        self.result = exception

    def acknowledge(self):
        """Acknowledge that this task was completed. Used for tasks that were dequeued with a late_ack-enabled queue.

        :return: Returns `True` if late acknowledgement was required and has been done, `False` otherwise
        :rtype: bool

        """
        if self.ack_id:
            self.subscription.acknowledge([self.ack_id])
            self.ack_id, self.subscription = '', None
            return True
        return False

    def dump(self):
        """ Get prepared task data for enqueuing.

        :rtype: six.basestring
        """

        # make sure we don't dump complex data
        tmp = self.ack_id, self.subscription
        if self.ack_id:
            self.ack_id, self.subscription = '', None

        # fixme: (u)json support to improve compatibility with non-python clients (and speedup py2)
        data = dumps(self)

        # restore data
        if self.ack_id:
            self.ack_id, self.subscription = tmp[0], tmp[1]

        return data

    def summary(self):
        return '{id}: {f.__name__}({args}, {kwargs}) -> {result} ({status})'\
            .format(**self.__dict__)

    def execute(self, queue=None):
        if not queue:
            queue = current_queue

        with self.task_context():
            self.start()
            # Notify the storage that this task has been started.
            queue.storage.put_task(self)

            try:
                result = self.call_func()
                self.finish(result)
                self.acknowledge()
            except Retry:
                # Task raised Retry, so re-enqueue the task to run again later.
                self.retry()
                queue.enqueue_task(self)
                self.acknowledge()
            except Exception as e:
                if self.retries >= ON_EXCEPTION_MAX_RETRIES_BEFORE_ACK_AND_GIVE_UP:
                    logger.exception('Task {} failed permanently.'.format(self.id))
                    self.acknowledge()  # poor task failed forever
                else:
                    logger.exception('Task {} failed, task will be tried after acknowledge timeout.'.format(self.id))
                self.fail(e)
            finally:
                # Record success, failure, or retry in the storage.
                queue.storage.put_task(self)

    def call_func(self):
        return self.f(*self.args, **self.kwargs)

    def task_context(self):
        """
        Returns a context manager that sets this task as the current_task
        global. Similar to flask's app.request_context. This is used by the
        workers to make the global available inside of task functions.
        """
        return task_context(self)

    def _import_function(self):
        if not isinstance(self.f, string_types):
            return

        mod_name, func_name = self.f.rsplit('.', 1)
        module = importlib.import_module(mod_name)

        self.f = getattr(module, func_name)

    __call__ = execute

    def __str__(self):
        return '<Task {}>'.format(self.summary())

    def __setstate__(self, d):
        self.__dict__ = d
        self._import_function()


class TaskResult(object):
    """Similar to concurrent.futures.Future, this class can be used to get the
    result of a task.
    """
    def __init__(self, task_id, queue=None):
        self.task_id = task_id

        if not queue:
            queue = current_queue

        self.storage = queue.storage

    def get_task(self):
        return self.storage.get_task(self.task_id)

    def result(self, timeout=None):
        """Gets the result of the task.

        Arguments:
            timeout: Maximum seconds to wait for a result before raising a
                TimeoutError. If set to None, this will wait forever. If the
                queue doesn't store results and timeout is None, this call will
                never return.
        """
        start = time.time()
        while True:
            task = self.get_task()
            if not task or task.status not in (FINISHED, FAILED):
                if not timeout:
                    continue
                elif time.time() - start < timeout:
                    continue
                else:
                    raise TimeoutError()

            if task.status == FAILED:
                raise task.result

            return task.result
