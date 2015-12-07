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

from gcloud.pubsub.connection import Connection
import httplib2
from werkzeug.local import Local


class ContextLocalPubsubConnection(Connection):
    def __init__(self, connection):
        super(ContextLocalPubsubConnection, self).__init__(
            credentials=connection._credentials)
        self._local = Local()

    @property
    def http(self):
        if not hasattr(self._local, 'http'):
            self._local.http = httplib2.Http()
            if self._credentials:
                self._local.http = self._credentials.authorize(
                    self._local.http)
        return self._local.http
