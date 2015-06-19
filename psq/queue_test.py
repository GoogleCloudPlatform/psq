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

from contextlib import contextmanager
from pickle import dumps
from unittest import TestCase

from mock import Mock, patch

from psq import current_queue
from psq.queue import BroadcastQueue, Queue
from psq.task import Task


class MockMessage(object):
    def __init__(self, data):
        self.data = data


class TestQueue(TestCase):

    def testCreation(self):
        pubsub = Mock()
        topic = Mock()
        topic.exists.return_value = False
        pubsub.topic.return_value = topic

        q = Queue(pubsub)

        assert pubsub.topic.called
        assert topic.exists.called
        assert q.topic == topic

        sub = Mock()
        sub.exists.return_value = False

        with patch('gcloud.pubsub.Subscription') as sub_ctr:
            sub_ctr.return_value = sub
            rsub = q._get_or_create_subscription()

            assert rsub == sub
            assert sub_ctr.called_with('psq-default-shared', topic)
            assert sub.exists.called
            assert sub.create.called

    def testQueue(self):
        with patch('psq.queue.Queue._get_or_create_topic'):
            q = Queue(Mock())
            q.storage.put_task = Mock()

            r = q.enqueue(sum, 1, 2, arg='c')
            assert q.topic.publish.called
            assert q.storage.put_task.called

            t = q.storage.put_task.call_args[0][0]
            assert t.f == sum
            assert t.args == (1, 2)
            assert t.kwargs == {'arg': 'c'}

            assert r.task_id == t.id

    def testDequeue(self):
        with patch('psq.queue.Queue._get_or_create_topic'):
            q = Queue(Mock())
            t = Task('1', sum, (1, 2), {'arg': 'c'})
            sub_mock = Mock()
            q._get_or_create_subscription = Mock(return_value=sub_mock)

            sub_mock.pull.return_value = [
                ('ack_id', MockMessage(dumps(t)))]

            tasks = q.dequeue()

            assert sub_mock.pull.called
            sub_mock.acknowledge.assert_called_once_with(['ack_id'])
            assert tasks[0].id == t.id
            assert tasks[0].f == t.f
            assert tasks[0].args == t.args
            assert tasks[0].kwargs == t.kwargs

            sub_mock.pull.reset_mock()
            sub_mock.acknowledge.reset_mock()
            sub_mock.pull.return_value = [
                ('ack_id', MockMessage('this is a bad pickle string'))]

            tasks = q.dequeue()

            assert not tasks
            assert sub_mock.pull.called
            sub_mock.acknowledge.assert_called_once_with(['ack_id'])

    def testContext(self):
        pubsub = Mock()
        pubsub.topic.return_value = Mock()
        q = Queue(pubsub)

        with q.queue_context():
            assert current_queue == q

        spy = Mock()

        @contextmanager
        def extra_context():
            spy()
            yield

        q.extra_context = extra_context

        with q.queue_context():
            assert spy.called


class TestBroadcastQueue(TestCase):

    def testCleanup(self):
        pubsub = Mock()
        pubsub.topic.return_value = Mock()
        q = BroadcastQueue(pubsub)
        q.subscription = Mock()
        q.cleanup()
        assert q.subscription.delete.called

    def testCreateSubscription(self):
        pubsub = Mock()
        pubsub.topic.return_value = Mock()
        q = BroadcastQueue(pubsub)

        sub = Mock()
        sub.exists.return_value = False

        with patch('gcloud.pubsub.Subscription') as sub_ctr:
            sub_ctr.return_value = sub
            rsub = q._get_or_create_subscription()

            assert rsub == sub
            assert 'worker' in sub_ctr.call_args[0][0]
            assert 'broadcast' in sub_ctr.call_args[0][0]
            assert sub.exists.called
            assert sub.create.called
