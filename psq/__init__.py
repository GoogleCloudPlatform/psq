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
"""
psq - a distributed task queue using Google Cloud Pubsub.

Homepage: https://github.com/GoogleCloudPlatform/psq
"""

from __future__ import absolute_import

import logging

from .globals import current_queue, current_task, queue_context, task_context
from .queue import BroadcastQueue, Queue
from .storage import DatastoreStorage, Storage
from .task import Retry, Task, TaskResult
from .worker import MultiprocessWorker, Worker


__all__ = [
    'Queue',
    'BroadcastQueue',
    'Task',
    'TaskResult',
    'Retry',
    'Worker',
    'MultiprocessWorker',
    'Storage',
    'DatastoreStorage',
    'current_queue',
    'queue_context',
    'current_task',
    'task_context'
]


# Set default logging handler to avoid "No handler found" warnings.
logging.getLogger(__name__).addHandler(logging.NullHandler())
