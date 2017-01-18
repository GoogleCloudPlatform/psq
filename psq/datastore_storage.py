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

from datetime import datetime

from google.cloud import datastore
from retrying import retry
from six.moves import range

from .storage import Storage
from .task import FAILED, FINISHED
from .utils import _check_for_thread_safety, dumps, loads


DATASTORE_KIND_PREFIX = 'psq'

_RETRY = retry(
    stop_max_attempt_number=5,
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
    retry_on_exception=lambda e: not isinstance(e, KeyboardInterrupt)
)


class DatastoreStorage(Storage):
    """
    Stores tasks in Google Cloud Datastore. By default, this only stores when
    the task is finished or failed. You can change the store_on_status property
    to change that, but be aware it will be slower.
    """
    store_on_status = (FINISHED, FAILED)

    def __init__(self, datastore):
        super(DatastoreStorage, self).__init__()
        _check_for_thread_safety(datastore)
        self.datastore = datastore

    @_RETRY
    def _get_task_key(self, task_id):
        return self.datastore.key(
            '{}-task'.format(DATASTORE_KIND_PREFIX), task_id)

    @_RETRY
    def get_task(self, task_id):
        entity = self.datastore.get(self._get_task_key(task_id))
        if not entity:
            return None
        return loads(entity['data'])

    @_RETRY
    def put_task(self, task):
        if task.status not in self.store_on_status:
            return

        entity = datastore.Entity(
            key=self._get_task_key(task.id),
            exclude_from_indexes=('data',))

        entity['data'] = dumps(task)
        entity['timestamp'] = datetime.utcnow()

        self.datastore.put(entity)

    @_RETRY
    def delete_task(self, task_id):
        self.datastore.delete(self._get_task_key(task_id))

    @_RETRY
    def list_tasks(self):
        q = self.datastore.query(
            kind='{}-task'.format(DATASTORE_KIND_PREFIX),
            order=('-timestamp',))

        return q.fetch()

    @_RETRY
    def delete_tasks(self):
        q = self.datastore.query(
            kind='{}-task'.format(DATASTORE_KIND_PREFIX),
            order=('-timestamp',))
        q.keys_only()

        keys = [x.key for x in q.fetch()]

        def chunks(l, n):
            """Yield successive n-sized chunks from l."""
            for i in range(0, len(l), n):
                yield l[i:i+n]

        for chunk in chunks(keys, 100):
            self.datastore.delete_multi(chunk)
