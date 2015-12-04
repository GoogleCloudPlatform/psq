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
from psq import utils
from psq.queue import dummy_context
from psq.task import FAILED, FINISHED, QUEUED, RETRYING, TimeoutError


class MockQueue(psq.Queue):
    def __init__(self):
        self.topic = Mock()
        self.storage = Mock()
        self.extra_context = dummy_context
        self.enqueue_task = Mock()


def dummy_task_func():
    return 'Hello'


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

        # Test with global queue context
        with q.queue_context():
            t.execute()

            assert t.status == FINISHED
            assert t.result == '42'
            t.reset()

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

    def testContext(self):
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

    def testSummary(self):
        t = psq.Task('1', sum, ('2'), {'3': '4'})
        assert t.summary() == '1: sum(2, {\'3\': \'4\'}) -> None (queued)'
        assert t.summary() in str(t)

    def test_string_function(self):
        t = psq.Task(
            '1', 'psq.task_test.dummy_task_func', (), {})

        unpickled = utils.unpickle(utils.dumps(t))

        assert unpickled.f == dummy_task_func

        q = MockQueue()
        with q.queue_context():
            unpickled.execute()
            assert unpickled.result == 'Hello'

        # Bad functions/modules
        t = psq.Task(
            '1', 'psq.task_test.nonexistant_function', (), {})

        self.assertRaises(
            utils.UnpickleError,
            utils.unpickle,
            utils.dumps(t))

        t = psq.Task(
            '1', 'psq.nonexistant_module.nonexistant_function', (), {})

        self.assertRaises(
            utils.UnpickleError,
            utils.unpickle,
            utils.dumps(t))


class TestTaskResult(TestCase):

    def testResult(self):
        t = psq.Task('1', sum, (), {})
        q = MockQueue()
        q.storage.get_task.return_value = None

        with q.queue_context():
            r = psq.TaskResult('1')

        assert r.storage == q.storage

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

        # Test without timeout. This is tricky.

        t.reset()

        def side_effect(_):
            t.finish(43)
            q.storage.get_task.return_value = t
            q.storage.get_task.side_effect = None

        q.storage.get_task.side_effect = side_effect

        assert r.result() == 43
