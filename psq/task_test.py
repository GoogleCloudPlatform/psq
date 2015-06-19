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

from unittest import TestCase

from mock import Mock

import psq
from psq.queue import dummy_context
from psq.task import FAILED, FINISHED, QUEUED, RETRYING, TimeoutError


class MockQueue(psq.Queue):
    def __init__(self):
        self.topic = Mock()
        self.storage = Mock()
        self.context = dummy_context
        self.enqueue_task = Mock()


class TestTask(TestCase):

    def testExecution(self):
        q = MockQueue()

        def f():
            return '42'

        t = psq.Task('1', f, (), {})

        assert t.status == QUEUED

        t.execute(q)

        assert t.status == FINISHED
        assert t.result == '42'
        assert q.storage.put_task.call_count == 2

        t.reset()

        assert t.status == QUEUED
        assert t.result is None

        def fails():
            raise ValueError()

        t.f = fails

        t.execute(q)

        assert t.status == FAILED
        assert isinstance(t.result, ValueError)

        t.reset()

        def retries():
            raise psq.Retry()

        t.f = retries

        t.execute(q)

        assert t.status == RETRYING
        assert t.retries == 1
        assert q.enqueue_task.called

    def test_context(self):
        q = MockQueue()
        t = psq.Task('1', sum, (), {})

        assert not psq.current_task

        with psq.task_context(t):
            assert psq.current_task
            assert psq.current_task.id == t.id

        assert not psq.current_queue
        assert not psq.current_task

        def f():
            assert psq.current_task
            return True

        t.f = f

        t.execute(q)
        assert t.result is True


class TestTaskResult(TestCase):

    def testResult(self):
        t = psq.Task('1', sum, (), {})
        q = MockQueue()
        q.storage.get_task.return_value = None

        r = psq.TaskResult('1', q)

        with self.assertRaises(TimeoutError):
            r.result(timeout=0.1)

        q.storage.get_task.return_value = t

        with self.assertRaises(TimeoutError):
            r.result(timeout=0.1)

        t.start()

        with self.assertRaises(TimeoutError):
            r.result(timeout=0.1)

        t.finish(42)

        assert r.result(timeout=0.1) == 42

        t.fail(ValueError())

        with self.assertRaises(ValueError):
            r.result(timeout=0.1)
