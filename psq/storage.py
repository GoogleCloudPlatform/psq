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

from gcloud import datastore
from six.moves import range

from .task import FAILED, FINISHED
from .utils import dumps, loads


DATASTORE_KIND_PREFIX = 'psq'


class Storage(object):
    """
    Storage implements task persistence. PSQ doesn't require any actual
    storage, but you can not get the return value of a task without it.
    This base class does not store any data and just stubs out the methods.
    """

    def get_task(self, task_id):
        pass

    def put_task(self, task):
        pass

    def delete_task(self, task_id):
        pass


class DatastoreStorage(Storage):
    """
    Stores tasks in Google Cloud Datastore. By default, this only stores when
    the task is finished or failed. You can change the store_on_status property
    to change that, but be aware it will be slower.
    """
    store_on_status = (FINISHED, FAILED)

    def __init__(self, datastore):
        super(DatastoreStorage, self).__init__()
        self.datastore = datastore

    def _get_task_key(self, task_id):
        return self.datastore.key(
            '{}-task'.format(DATASTORE_KIND_PREFIX), task_id)

    def get_task(self, task_id):
        entity = self.datastore.get(self._get_task_key(task_id))
        if not entity:
            return None
        return loads(entity['data'])

    def put_task(self, task):
        if task.status not in self.store_on_status:
            return

        entity = datastore.Entity(
            key=self._get_task_key(task.id),
            exclude_from_indexes=('data',))

        entity['data'] = dumps(task)
        entity['timestamp'] = datetime.utcnow()

        self.datastore.put(entity)

    def delete_task(self, task_id):
        self.datastore.delete(self._get_task_key(task_id))

    def list_tasks(self):
        q = self.datastore.query(
            kind='{}-task'.format(DATASTORE_KIND_PREFIX),
            order=('-timestamp',))

        return q.fetch()

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
