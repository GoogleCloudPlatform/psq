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

import logging
from uuid import uuid4

from google.cloud import pubsub

from . import queue
from . import storage


logger = logging.getLogger(__name__)


class BroadcastQueue(queue.Queue):
    """Sends each task to all active workers.

    This is in contrast with the standard queue which distributes a task to a
    single worker.
    """
    def __init__(self, pubsub, name='broadcast', **kwargs):
        super(BroadcastQueue, self).__init__(
            pubsub, name=name, storage=storage.Storage(), **kwargs)

    def _get_or_create_subscription(self):
        """In a broadcast queue, workers have a unique subscription ensuring
        that every worker recieves a copy of every task."""
        subscription_name = '{}-{}-{}-worker'.format(
            queue.PUBSUB_OBJECT_PREFIX, self.name, uuid4().hex)

        subscription = pubsub.Subscription(subscription_name, topic=self.topic)

        if not subscription.exists():
            logger.info("Creating worker subscription {}".format(
                subscription_name))
            subscription.create()

        return subscription

    def cleanup(self):
        """Deletes this worker's subscription."""
        if self.subscription:
            logger.info("Deleting worker subscription...")
            self.subscription.delete()
