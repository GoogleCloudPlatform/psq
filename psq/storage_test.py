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

from unittest import TestCase

from gcloud import datastore
from mock import Mock
from psq.storage import DatastoreStorage, Storage
from psq.task import Task


class TestStorage(TestCase):

    def test_ops(self):
        storage = Storage()
        storage.put_task(None)
        assert storage.get_task(1) is None
        storage.delete_task(1)


class TestDatastoreStorage(TestCase):

    def test_ops(self):
        datastore_mock = Mock()
        storage = DatastoreStorage(datastore_mock)

        t = Task('1', sum, (), {})

        # shouldn't store queued, retrying, or started tasks.
        storage.put_task(t)
        assert not datastore_mock.put.called

        t.start()
        storage.put_task(t)
        assert not datastore_mock.put.called

        t.retry()
        storage.put_task(t)
        assert not datastore_mock.put.called

        # Should store finished and failed tasks
        t.finish('5')
        storage.put_task(t)
        assert datastore_mock.put.called

        datastore_mock.reset_mock()
        t.fail(ValueError())
        storage.put_task(t)
        assert datastore_mock.put.called

        # Should return back the same task
        datastore_mock.get.return_value = datastore_mock.put.call_args[0][0]
        rt = storage.get_task('1')
        assert str(rt) == str(t)

        # Should return None for a task that doesn't exist
        datastore_mock.get.return_value = None
        rt = storage.get_task('1')
        assert rt is None

        storage.delete_task('1')
        assert datastore_mock.delete.called

    def test_list(self):
        datastore_mock = Mock()
        storage = DatastoreStorage(datastore_mock)
        t = Task('1', sum, (), {})
        q_mock = Mock()
        q_mock.fetch.return_value = [t]
        datastore_mock.query.return_value = q_mock

        tasks = storage.list_tasks()

        assert len(tasks) == 1
        assert tasks[0] == t

    def test_mass_delete(self):
        datastore_mock = Mock()
        storage = DatastoreStorage(datastore_mock)
        q_mock = Mock()
        q_mock.fetch.return_value = [
            Mock(key=datastore.Key('task', n, project='test'))
            for n in range(102)]
        datastore_mock.query.return_value = q_mock

        storage.delete_tasks()

        assert datastore_mock.delete_multi.call_count == 2
