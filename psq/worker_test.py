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

import mock
from psq.queue import dummy_context, Queue
from psq.worker import Worker


class MockQueue(Queue):
    def __init__(self):
        self._tasks = []
        self.storage = None
        self.extra_context = dummy_context
        self.subscriber = mock.Mock()
        self.subscriber.result.side_effect = KeyboardInterrupt()

    def enqueue_task(self, task):
        self._tasks.append(task)

    def listen(self, callback):
        for task in self._tasks:
            callback(task)

        return self.subscriber


def test_worker_listen():
    q = MockQueue()
    worker = Worker(queue=q)

    t = mock.Mock()
    q.enqueue_task(t)

    worker.listen()

    assert t.execute.called
    assert q.subscriber.result.called
    assert q.subscriber.cancel.called


def test_worker_listen_failed_start():
    q = mock.create_autospec(Queue, instance=True)
    q.storage = None
    q.listen.side_effect = KeyboardInterrupt()
    worker = Worker(queue=q)

    worker.listen()

    assert q.cleanup.called
