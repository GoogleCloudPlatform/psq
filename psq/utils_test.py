# Copyright 2016 Google Inc.
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


from mock import MagicMock
from psq import utils
import pytest


# Used by the tests below
_USE_GAX = False
_USE_GRPC = False


class MockConnection(object):
    def __init__(self):
        self.http = MagicMock()


class MockClient(object):
    def __init__(self):
        self.connection = MockConnection()


class MockClientNoConnection():
    def __init__(self, error):
        self.error = error

    @property
    def connection(self):
        raise self.error()


def test_check_for_thread_safety_use_gax():
    global _USE_GAX
    _USE_GAX = True
    utils._check_for_thread_safety(MockClient())


def test_check_for_thread_safety_use_grpc():
    global _USE_GAX
    _USE_GAX = False
    global _USE_GRPC
    _USE_GRPC = True
    utils._check_for_thread_safety(MockClient())


def test_check_for_thread_safety_httplib2shim():
    global _USE_GAX
    _USE_GAX = False
    global _USE_GRPC
    _USE_GRPC = False
    client = MockClient()
    client.connection.http.__class__.__module__ = 'httplib2shim'
    utils._check_for_thread_safety(client)


def test_check_for_thread_safety_key_and_attr_error():
    global _USE_GAX
    _USE_GAX = False
    global _USE_GRPC
    _USE_GRPC = False
    client = MockClientNoConnection(KeyError)
    utils._check_for_thread_safety(client)
    client = MockClientNoConnection(AttributeError)
    utils._check_for_thread_safety(client)


def test_check_for_thread_safety_fail():
    global _USE_GAX
    _USE_GAX = False
    global _USE_GRPC
    _USE_GRPC = False
    client = MockClient()

    with pytest.raises(ValueError):
        utils._check_for_thread_safety(client)
