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
import functools
import logging
from uuid import uuid4

import google.cloud.exceptions

from .globals import queue_context
from .storage import Storage
from .task import Task, TaskResult
from .utils import dumps, measure_time, unpickle, UnpickleError


logger = logging.getLogger(__name__)

PUBSUB_OBJECT_PREFIX = 'psq'


class Queue(object):
    def __init__(self, publisher_client, subscriber_client, project,
                 name='default', storage=None, extra_context=None, async=True):
        self._async = async
        self.name = name
        self.project = project

        if self._async:
            self.publisher_client = publisher_client
            self.subscriber_client = subscriber_client
            self.topic_path = self._get_or_create_topic()

        self.storage = storage or Storage()
        self.subscription = None
        self.extra_context = extra_context if extra_context else dummy_context

    def _get_topic_path(self):
        topic_name = '{}-{}'.format(PUBSUB_OBJECT_PREFIX, self.name)
        return self.publisher_client.topic_path(self.project, topic_name)

    def _get_or_create_topic(self):
        topic_path = self._get_topic_path()

        try:
            self.publisher_client.get_topic(topic_path)
        except google.cloud.exceptions.NotFound:
            logger.info("Creating topic {}".format(topic_path))
            try:
                self.publisher_client.create_topic(topic_path)
            except google.cloud.exceptions.Conflict:
                # Another process created the topic before us, ignore.
                pass

        return topic_path

    def _get_or_create_subscription(self):
        """Workers all share the same subscription so that tasks are
        distributed across all workers."""
        topic_path = self._get_topic_path()
        subscription_name = '{}-{}-shared'.format(
            PUBSUB_OBJECT_PREFIX, self.name)
        subscription_path = self.subscriber_client.subscription_path(
            self.project, subscription_name)

        try:
            self.subscriber_client.get_subscription(subscription_path)
        except google.cloud.exceptions.NotFound:
            logger.info("Creating shared subscription {}".format(
                subscription_name))
            try:
                self.subscriber_client.create_subscription(
                    subscription_path, topic=topic_path)
            except google.cloud.exceptions.Conflict:
                # Another worker created the subscription before us, ignore.
                pass

        return subscription_path

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

        if self._async:
            self.publisher_client.publish(self.topic_path, data=data)
            logger.info('Task {} queued.'.format(task.id))
        else:
            unpickled_task = unpickle(data)
            logger.info(
                'Executing task {} synchronously.'.format(unpickled_task.id)
            )
            with measure_time() as summary, self.queue_context():
                unpickled_task.execute(queue=self)
                summary(unpickled_task.summary())

        return TaskResult(task.id, self)

    @staticmethod
    def _pubsub_message_callback(task_callback, message):
        message.ack()

        try:
            task = unpickle(message.data)
            task_callback(task)
        except UnpickleError:
            logger.exception('Failed to unpickle task {}.'.format(message))

    def listen(self, callback):
        if not self.subscription:
            self.subscription = self._get_or_create_subscription()

        message_callback = functools.partial(
            self._pubsub_message_callback, callback)
        return self.subscriber_client.subscribe(
            self.subscription, callback=message_callback)

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


@contextmanager
def dummy_context():
    yield
