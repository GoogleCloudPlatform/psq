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

import logging

from .utils import measure_time


logger = logging.getLogger(__name__)


class Worker(object):
    def __init__(self, queue):
        self.queue = queue
        self.storage = self.queue.storage
        self.max_sequential_errors = 5

    def listen(self):
        future = None

        try:
            future = self.queue.listen(self.run_task)
            logger.info('Listening for tasks, press Ctrl+C to exit.')
            future.result()

        except KeyboardInterrupt:
            if future is not None:
                future.cancel()
            logger.info('Stopped listening for tasks.')

        finally:
            self.queue.cleanup()

    def run_task(self, task):
        logger.info('Received task {}'.format(task.id))
        with measure_time() as summary, self.queue.queue_context():
            task.execute(self.queue)
            summary(task.summary())
