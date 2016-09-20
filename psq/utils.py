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
from functools import partial
import logging
import sys
import time

try:
    import cPickle as pickle
except ImportError:  # pragma: no cover
    import pickle


logger = logging.getLogger(__name__)
dumps = partial(pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL)
loads = pickle.loads


class UnpickleError(ValueError):
    pass


def unpickle(pickled_string):
    """Unpickles a string, but raises a unified UnpickleError in case anything
    fails.
    This is a helper method to not have to deal with the fact that `loads()`
    potentially raises many types of exceptions (e.g. AttributeError,
    IndexError, TypeError, KeyError, etc.)
    """
    try:
        obj = loads(pickled_string)
    except Exception as e:
        raise UnpickleError('Could not unpickle', pickled_string, e)
    return obj


@contextmanager
def measure_time():
    ts = time.time()
    props = {}

    def summary(v):
        props['summary'] = v

    try:
        yield summary

    finally:
        te = time.time()
        logger.info('{} Took {:.2f} sec'.format(
            props.get('summary', ''), te - ts))


def _check_for_thread_safety(client):
    try:
        # Is this client's module using grpc/gax?
        client_module_name = client.__module__
        client_module = sys.modules[client_module_name]
        if getattr(client_module, '_USE_GAX', True):
            return

        connection_module_name = client.connection.__module__
        connection_module = sys.modules[connection_module_name]

        if getattr(connection_module, '_USE_GRPC', True):
            return

        # Is the connection is using httplib2shim?
        if client.connection.http.__class__.__module__ == 'httplib2shim':
            return

        raise ValueError(
            'Client object {} is not threadsafe. psq requires clients to be '
            'threadsafe. You can either ensure grpc is installed or use '
            'httplib2shim.'.format(client))

    except (KeyError, AttributeError):
        pass
