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

from mock import Mock, patch

from psq.broadcast_queue import BroadcastQueue


def test_cleanup():
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


def test_create_subscription():
    pubsub = Mock()
    pubsub.topic.return_value = Mock()
    q = BroadcastQueue(pubsub)

    sub = Mock()
    sub.exists.return_value = False

    # Test to make sure it creates a unique (non-shared) subscription.
    with patch('google.cloud.pubsub.Subscription') as SubscriptionMock:
        SubscriptionMock.return_value = sub
        rsub = q._get_or_create_subscription()

        assert rsub == sub
        assert 'worker' in SubscriptionMock.call_args[0][0]
        assert 'broadcast' in SubscriptionMock.call_args[0][0]
        assert sub.exists.called
        assert sub.create.called

    # Test reusing existing
    with patch('google.cloud.pubsub.Subscription') as SubscriptionMock:
        sub.reset_mock()
        SubscriptionMock.return_value = sub
        sub.exists.return_value = True
        rsub = q._get_or_create_subscription()

        assert rsub == sub
        assert not sub.create.called
