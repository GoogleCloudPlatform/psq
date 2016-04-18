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

from __future__ import absolute_import

from contextlib import contextmanager
import logging
from uuid import uuid4

from gcloud import pubsub
import gcloud.exceptions

from .context_local_pubsub_connection import ContextLocalPubsubConnection
from .globals import queue_context
from .storage import Storage
from .task import Task, TaskResult
from .utils import dumps, unpickle, UnpickleError


logger = logging.getLogger(__name__)

PUBSUB_OBJECT_PREFIX = 'psq'


class Queue(object):
    def __init__(self, pubsub, name='default', storage=None,
                 extra_context=None):
        self.pubsub = pubsub
        self.pubsub.connection = ContextLocalPubsubConnection(
            self.pubsub.connection)
        self.name = name
        self.topic = self._get_or_create_topic()
        self.storage = storage or Storage()
        self.subscription = None
        self.extra_context = extra_context if extra_context else dummy_context

    def _get_or_create_topic(self):
        topic_name = '{}-{}'.format(PUBSUB_OBJECT_PREFIX, self.name)

        topic = self.pubsub.topic(topic_name)

        if not topic.exists():
            logger.info("Creating topic {}".format(topic_name))
            try:
                topic.create()
            except gcloud.exceptions.Conflict:
                # Another process created the topic before us, ignore.
                pass

        return topic

    def _get_or_create_subscription(self):
        """Workers all share the same subscription so that tasks are
        distributed across all workers."""
        subscription_name = '{}-{}-shared'.format(
            PUBSUB_OBJECT_PREFIX, self.name)

        subscription = pubsub.Subscription(
            subscription_name, topic=self.topic)

        if not subscription.exists():
            logger.info("Creating shared subscription {}".format(
                subscription_name))
            try:
                subscription.create()
            except gcloud.exceptions.Conflict:
                # Another worker created the subscription before us, ignore.
                pass

        return subscription

    def enqueue(self, f, *args, **kwargs):
        """Enqueues a function for the task queue to execute."""
        task = Task(uuid4().hex, f, args, kwargs)
        self.storage.put_task(task)
        return self.enqueue_task(task)

    def enqueue_task(self, task):
        """Enqueues a task directly. This is used when a task is retried or if
        a task was manually created.

        Note that this does not store the task.
        """
        data = dumps(task)
        self.topic.publish(data)
        logger.info("Task {} queued.".format(task.id))
        return TaskResult(task.id, self)

    def dequeue(self, max=1, block=False):
        """Returns tasks to be consumed by a worker."""
        if not self.subscription:
            self.subscription = self._get_or_create_subscription()

        messages = self.subscription.pull(
            return_immediately=not block, max_messages=max)

        if not messages:
            return None

        ack_ids = [x[0] for x in messages]

        tasks = []
        for x in messages:
            try:
                task = unpickle(x[1].data)
                tasks.append(task)
            except UnpickleError as e:
                logger.exception(e)
                logger.error("Failed to unpickle a task.")

        self.subscription.acknowledge(ack_ids)

        return tasks

    def cleanup(self):
        """Does nothing for this queue, but other queues types may use this to
        perform clean-up after listening for tasks."""
        pass

    def queue_context(self):
        """
        Returns a context manager that sets this queue as the current_queue
        global. Similar to flask's app.app_context. This is used by the workers
        to make the global available inside of task functions.
        """
        return queue_context(self)


class BroadcastQueue(Queue):
    """Sends each task to all active workers.

    This is in contrast with the standard queue which distributes a task to a
    single worker.
    """
    def __init__(self, pubsub, name='broadcast', **kwargs):
        super(BroadcastQueue, self).__init__(
            pubsub, name=name, storage=Storage(), **kwargs)

    def _get_or_create_subscription(self):
        """In a broadcast queue, workers have a unique subscription ensuring
        that every worker recieves a copy of every task."""
        subscription_name = '{}-{}-{}-worker'.format(
            PUBSUB_OBJECT_PREFIX, self.name, uuid4().hex)

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


@contextmanager
def dummy_context():
    yield
