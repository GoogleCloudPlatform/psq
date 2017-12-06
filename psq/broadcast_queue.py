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

import google.cloud.exceptions

from . import queue
from . import storage


logger = logging.getLogger(__name__)


class BroadcastQueue(queue.Queue):
    """Sends each task to all active workers.

    This is in contrast with the standard queue which distributes a task to a
    single worker.
    """
    def __init__(self, publisher_client, subscriber_client, project,
                 name='broadcast', **kwargs):
        super(BroadcastQueue, self).__init__(
            publisher_client, subscriber_client, project, name=name,
            storage=storage.Storage(), **kwargs)

    def _get_or_create_subscription(self):
        """In a broadcast queue, workers have a unique subscription ensuring
        that every worker recieves a copy of every task."""
        topic_path = self._get_topic_path()
        subscription_name = '{}-{}-{}-worker'.format(
            queue.PUBSUB_OBJECT_PREFIX, self.name, uuid4().hex)
        subscription_path = self.subscriber_client.subscription_path(
            self.project, subscription_name)

        try:
            self.subscriber_client.get_subscription(subscription_path)
        except google.cloud.exceptions.NotFound:
            logger.info("Creating worker subscription {}".format(
                subscription_name))
            self.subscriber_client.create_subscription(
                subscription_path, topic_path)

        return subscription_path

    def cleanup(self):
        """Deletes this worker's subscription."""
        if self.subscription:
            logger.info("Deleting worker subscription...")
            self.subscriber_client.delete_subscription(self.subscription)
