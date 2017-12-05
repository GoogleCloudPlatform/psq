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

from google.cloud import pubsub_v1
import google.cloud.exceptions
import google.cloud.pubsub_v1.subscriber.message
import mock
from psq import current_queue
from psq.queue import Queue
from psq.task import Task
import pytest


def make_publisher_client():
    return mock.create_autospec(
        pubsub_v1.PublisherClient, instance=True)


def make_subscriber_client():
    return mock.create_autospec(
        pubsub_v1.SubscriberClient, instance=True)


class TestStorage(object):
    def __init__(self):
        self._data = {}

    def get_task(self, task_id):
        return self._data.get(task_id)

    def put_task(self, task):
        self._data[task.id] = task


def dummy_queue_func():
    return "Hello"


def test_constructor_creates_topic():
    publisher_client = make_publisher_client()
    subscriber_client = make_subscriber_client()

    publisher_client.get_topic.side_effect = (
        google.cloud.exceptions.NotFound(None, None))

    q = Queue(publisher_client, subscriber_client, 'test-project')

    publisher_client.create_topic.assert_called_once_with(q._get_topic_path())


def test_constructor_existing_topic():
    publisher_client = make_publisher_client()
    subscriber_client = make_subscriber_client()

    Queue(publisher_client, subscriber_client, 'test-project')

    publisher_client.create_topic.assert_not_called()


def test_constructor_conflict():
    publisher_client = make_publisher_client()
    subscriber_client = make_subscriber_client()

    publisher_client.get_topic.side_effect = (
        google.cloud.exceptions.NotFound(None, None))
    publisher_client.create_topic.side_effect = (
        google.cloud.exceptions.Conflict(None, None))

    q = Queue(publisher_client, subscriber_client, 'test-project')

    publisher_client.get_topic.assert_called_once_with(
        q._get_topic_path())
    publisher_client.create_topic.assert_called_once_with(
        q._get_topic_path())


def make_queue(**kwargs):
    publisher_client = make_publisher_client()
    subscriber_client = make_subscriber_client()
    return Queue(publisher_client, subscriber_client, 'test-project', **kwargs)


def test_get_or_create_subscription_creates_new():
    q = make_queue()
    q.subscriber_client.get_subscription.side_effect = (
        google.cloud.exceptions.NotFound(None, None))

    subscription_path = q._get_or_create_subscription()

    q.subscriber_client.get_subscription.assert_called_once_with(
        subscription_path)
    q.subscriber_client.create_subscription.assert_called_once_with(
        subscription_path,
        topic=q._get_topic_path())


def test_get_or_create_subscription_existing():
    q = make_queue()

    subscription_path = q._get_or_create_subscription()

    q.subscriber_client.get_subscription.assert_called_once_with(
        subscription_path)
    q.subscriber_client.create_subscription.assert_not_called()


def test_get_or_create_subscription_conflict():
    q = make_queue()

    q.subscriber_client.get_subscription.side_effect = (
        google.cloud.exceptions.NotFound(None, None))
    q.subscriber_client.create_subscription.side_effect = (
        google.cloud.exceptions.Conflict(None, None))

    subscription_path = q._get_or_create_subscription()

    q.subscriber_client.get_subscription.assert_called_once_with(
        subscription_path)
    q.subscriber_client.create_subscription.assert_called_once_with(
        subscription_path,
        topic=q._get_topic_path())


def test_queue():
    storage = TestStorage()
    q = make_queue(storage=storage)

    r = q.enqueue(sum, 1, 2, arg='c')
    assert q.publisher_client.publish.called

    task = storage.get_task(r.task_id)
    assert task.f == sum
    assert task.args == (1, 2)
    assert task.kwargs == {'arg': 'c'}


def test_listen():
    q = make_queue()
    callback = mock.Mock()

    future = q.listen(callback)

    # Should create the subscription
    assert q.subscription

    # Should invoke the underlying pub/sub listen
    q.subscriber_client.subscribe.assert_called_once_with(
        q.subscription, callback=mock.ANY)
    assert future == q.subscriber_client.subscribe.return_value

    # Grab the callback and make sure the invoking it decodes the task and
    # passes it to the callback.
    wrapped_callback = q.subscriber_client.subscribe.call_args[1]['callback']

    t = Task('1', sum, (1, 2), {'arg': 'c'})
    message = mock.create_autospec(
        google.cloud.pubsub_v1.subscriber.message.Message, instance=True)
    message.data = dumps(t)

    wrapped_callback(message)

    message.ack.assert_called_once_with()
    callback.assert_called_once_with(mock.ANY)
    invoked_task = callback.call_args[0][0]
    assert invoked_task.id == t.id


def test_listen_existing_subscription():
    q = make_queue()
    q.subscription = mock.sentinel.subscription

    q.listen(mock.sentinel.callback)

    # Should not create the subscription
    assert not q.subscriber_client.create_subscription.called


def test__pubsub_message_callback_bad_value():
    callback = mock.Mock()
    message = mock.create_autospec(
        google.cloud.pubsub_v1.subscriber.message.Message, instance=True)
    message.data = b'bad'

    Queue._pubsub_message_callback(callback, message)

    assert not callback.called
    assert message.ack.called


def test_context():
    q = make_queue()

    with q.queue_context():
        assert current_queue == q

    # Test additional context manager.
    spy = mock.Mock()

    @contextmanager
    def extra_context():
        spy()
        yield

    q.extra_context = extra_context

    with q.queue_context():
        assert spy.called


def test_cleanup():
    q = make_queue()
    q.cleanup()


def test_synchronous_success():
    q = make_queue(storage=TestStorage(), async=False)
    r = q.enqueue(sum, [1, 2])
    assert r.result() == 3


def test_synchronous_fail():
    q = make_queue(storage=TestStorage(), async=False)
    r = q.enqueue(sum, "2")
    with pytest.raises(TypeError):
        r.result()


def test_string_function():
    q = make_queue(storage=TestStorage(), async=False)
    r = q.enqueue('psq.queue_test.dummy_queue_func')
    assert r.result() == "Hello"
