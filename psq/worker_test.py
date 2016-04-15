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

import multiprocessing
from unittest import TestCase

from mock import Mock, patch
from psq.queue import dummy_context, Queue
from psq.task import Task
from psq.worker import MultiprocessWorker, Worker


class MockQueue(Queue):
    def __init__(self):
        self._tasks = []
        self._raise = False
        self.topic = Mock()
        self.storage = Mock()
        self.extra_context = dummy_context

    def enqueue_task(self, task):
        self._tasks.append(task)

    def dequeue(self, max=1, block=True):
        """If there's any tasks, it'll return those. Once it runs out,
        it returns an empty list of tasks, then the next time it's called it
        will raise a KeyboardInterrupt."""

        if self._raise:
            raise KeyboardInterrupt()

        tasks = self._tasks[:max]
        self._tasks = self._tasks[max:]

        if not tasks:
            self._raise = True

        return tasks


class TestWorker(TestCase):
    def test_listen(self):
        q = MockQueue()
        worker = Worker(queue=q)

        t = Mock()
        q.enqueue_task(t)

        worker.listen()

        assert t.execute.called

    @patch('retrying.time.sleep')
    def test_safe_dequeue(self, sleep_mock):
        q = Mock()
        t = Mock()
        worker = Worker(queue=q)

        # Test two errors and a success.
        q.dequeue.side_effect = [ValueError(''), ValueError(''), [t]]

        tasks = worker._safe_dequeue()

        assert tasks == [t]
        assert sleep_mock.call_count == 2

        # Test 5 sequential errors, should raise.
        sleep_mock.reset_mock()
        q.dequeue.side_effect = [
            ValueError(''), ValueError(''), ValueError(''), ValueError(''),
            RuntimeError('')]

        with self.assertRaises(RuntimeError):
            worker._safe_dequeue()

        assert sleep_mock.call_count == 4


# This is necessary to track the call across process boundaries.
mark_done_called = multiprocessing.Value('i')


def mark_done():
    mark_done_called.value = 1


class TestMultiprocessWorker(TestCase):
    def test(self):
        mark_done_called.value = 0

        q = MockQueue()

        worker = MultiprocessWorker(queue=q)
        assert worker.tasks_per_poll
        worker.pool.close()

        worker = MultiprocessWorker(queue=q, num_workers=1)

        t = Task('1', mark_done, (), {})
        q.enqueue_task(t)

        worker.listen()

        assert mark_done_called.value == 1
