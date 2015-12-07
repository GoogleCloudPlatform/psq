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

from mock import Mock

from psq.context_local_pubsub_connection import ContextLocalPubsubConnection


class TestContextLocalPubsubConnection(TestCase):
    def test(self):
        credentials = Mock()
        credentials.authorize.side_effect = lambda x: x
        credentials.create_scoped.return_value = credentials
        prev_connection = Mock()
        prev_connection._credentials = credentials

        connection = ContextLocalPubsubConnection(prev_connection)

        assert connection._credentials == credentials

        # should create a new http client the first time.
        http = connection.http
        assert credentials.authorize.call_count == 1

        # should return the same http client the second time.
        new_http = connection.http
        assert http == new_http
        assert credentials.authorize.call_count == 1

    def test_no_credentials(self):
        prev_connection = Mock()
        prev_connection._credentials = None

        connection = ContextLocalPubsubConnection(prev_connection)

        assert connection._credentials is None
        assert connection.http
