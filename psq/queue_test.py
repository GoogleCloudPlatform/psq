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

import gcloud.exceptions
from mock import Mock, patch

from psq import current_queue
from psq.queue import BroadcastQueue, Queue
from psq.task import Task


class MockMessage(object):
    def __init__(self, data):
        self.data = data


class TestQueue(TestCase):

    def test_creation(self):
        # Test the case where queue needs to create the topic.
        pubsub = Mock()
        topic = Mock()
        topic.exists.return_value = False
        pubsub.topic.return_value = topic

        q = Queue(pubsub)

        assert pubsub.topic.called
        assert topic.create.called
        assert q.topic == topic

        sub = Mock()
        sub.exists.return_value = False

    def test_get_or_create_subscription(self):
        pubsub = Mock()
        topic = Mock()
        topic.exists.return_value = True
        pubsub.topic.return_value = topic

        q = Queue(pubsub)

        sub = Mock()
        sub.exists.return_value = False

        # Test the case where it needs to create the subcription.
        with patch('gcloud.pubsub.Subscription') as SubscriptionMock:
            SubscriptionMock.return_value = sub
            rsub = q._get_or_create_subscription()

            assert rsub == sub
            assert SubscriptionMock.called_with('psq-default-shared', topic)
            assert sub.exists.called
            assert sub.create.called

        # Test case where subscription exists and it should re-use it.
        with patch('gcloud.pubsub.Subscription') as SubscriptionMock:
            sub.reset_mock()
            SubscriptionMock.return_value = sub
            sub.exists.return_value = True
            rsub = q._get_or_create_subscription()

            assert rsub == sub
            assert not sub.create.called

        # Test case where subscription gets created after we check that it
        # doesn't exist.

        with patch('gcloud.pubsub.Subscription') as SubscriptionMock:
            sub.reset_mock()
            SubscriptionMock.return_value = sub
            sub.exists.return_value = False
            sub.create.side_effect = gcloud.exceptions.Conflict('')
            rsub = q._get_or_create_subscription()

            assert sub.create.called
            assert rsub == sub

    def test_creation_existing_topic(self):
        # Test the case where queue needs to create the topic.
        pubsub = Mock()
        topic = Mock()
        topic.exists.return_value = False
        topic.create.side_effect = gcloud.exceptions.Conflict('')
        pubsub.topic.return_value = topic

        q = Queue(pubsub)

        assert pubsub.topic.called
        assert topic.create.called
        assert q.topic == topic

    def test_queue(self):
        # Test queueing tasks.
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

    def test_dequeue(self):
        # Test dequeueing (fetching) tasks.
        with patch('psq.queue.Queue._get_or_create_topic'):
            q = Queue(Mock())
            t = Task('1', sum, (1, 2), {'arg': 'c'})
            sub_mock = Mock()
            q._get_or_create_subscription = Mock(return_value=sub_mock)

            # No messages
            sub_mock.pull.return_value = []

            tasks = q.dequeue()

            assert sub_mock.pull.called
            assert not tasks

            # One Message
            sub_mock.pull.reset_mock()
            sub_mock.pull.return_value = [
                ('ack_id', MockMessage(dumps(t)))]

            tasks = q.dequeue()

            assert sub_mock.pull.called
            sub_mock.acknowledge.assert_called_once_with(['ack_id'])
            assert tasks[0].id == t.id
            assert tasks[0].f == t.f
            assert tasks[0].args == t.args
            assert tasks[0].kwargs == t.kwargs

            # Bad message
            sub_mock.pull.reset_mock()
            sub_mock.acknowledge.reset_mock()
            sub_mock.pull.return_value = [
                ('ack_id', MockMessage('this is a bad pickle string'))]

            tasks = q.dequeue()

            assert not tasks
            assert sub_mock.pull.called
            sub_mock.acknowledge.assert_called_once_with(['ack_id'])

    def test_context(self):
        # Test queue-local context.
        pubsub = Mock()
        pubsub.topic.return_value = Mock()
        q = Queue(pubsub)

        with q.queue_context():
            assert current_queue == q

        # Test additional context manager.
        spy = Mock()

        @contextmanager
        def extra_context():
            spy()
            yield

        q.extra_context = extra_context

        with q.queue_context():
            assert spy.called

    def test_cleanup(self):
        pubsub = Mock()
        pubsub.topic.return_value = Mock()
        q = Queue(pubsub)

        q.cleanup()


class TestBroadcastQueue(TestCase):

    def test_cleanup(self):
        # Broadcast queue should delete its own subscription, as it's not
        # shared.
        pubsub = Mock()
        pubsub.topic.return_value = Mock()
        q = BroadcastQueue(pubsub)
        q.subscription = Mock()
        q.cleanup()
        assert q.subscription.delete.called

        # test without subscription
        q.subscription = None
        q.cleanup()

    def test_create_subscription(self):
        pubsub = Mock()
        pubsub.topic.return_value = Mock()
        q = BroadcastQueue(pubsub)

        sub = Mock()
        sub.exists.return_value = False

        # Test to make sure it creates a unique (non-shared) subscription.
        with patch('gcloud.pubsub.Subscription') as SubscriptionMock:
            SubscriptionMock.return_value = sub
            rsub = q._get_or_create_subscription()

            assert rsub == sub
            assert 'worker' in SubscriptionMock.call_args[0][0]
            assert 'broadcast' in SubscriptionMock.call_args[0][0]
            assert sub.exists.called
            assert sub.create.called

        # Test reusing existing
        with patch('gcloud.pubsub.Subscription') as SubscriptionMock:
            sub.reset_mock()
            SubscriptionMock.return_value = sub
            sub.exists.return_value = True
            rsub = q._get_or_create_subscription()

            assert rsub == sub
            assert not sub.create.called
