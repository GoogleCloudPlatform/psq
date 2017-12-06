# Copyright 2016 Google Inc.
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

from google.cloud import pubsub_v1
import google.cloud.exceptions
import mock
from psq.broadcast_queue import BroadcastQueue


def make_publisher_client():
    return mock.create_autospec(
        pubsub_v1.PublisherClient, instance=True)


def make_subscriber_client():
    return mock.create_autospec(
        pubsub_v1.SubscriberClient, instance=True)


def test_cleanup():
    # Broadcast queue should delete its own subscription, as it's not
    # shared.
    publisher_client = make_publisher_client()
    subscriber_client = make_subscriber_client()
    q = BroadcastQueue(
        publisher_client, subscriber_client, 'test-project')
    q.subscription = 'test-subscription'
    q.cleanup()
    subscriber_client.delete_subscription.assert_called_once_with(
        'test-subscription')


def test_cleanup_no_subscription():
    # test without subscription
    publisher_client = make_publisher_client()
    subscriber_client = make_subscriber_client()
    q = BroadcastQueue(
        publisher_client, subscriber_client, 'test-project')
    q.cleanup()
    subscriber_client.delete_subscription.assert_not_called()


def test_create_subscription():
    publisher_client = make_publisher_client()
    subscriber_client = make_subscriber_client()
    q = BroadcastQueue(
        publisher_client, subscriber_client, 'test-project')

    subscriber_client.get_subscription.side_effect = (
        google.cloud.exceptions.NotFound(None, None))
    subscriber_client.subscription_path.side_effect = (
        lambda project, topic: '{}/{}'.format(project, topic))

    subscription_path = q._get_or_create_subscription()

    assert subscriber_client.create_subscription.called
    assert 'worker' in subscription_path
    assert 'broadcast' in subscription_path


def test_existing_subscription():
    publisher_client = make_publisher_client()
    subscriber_client = make_subscriber_client()
    q = BroadcastQueue(
        publisher_client, subscriber_client, 'test-project')

    q._get_or_create_subscription()

    assert subscriber_client.get_subscription.called
    assert not subscriber_client.create_subscription.called
