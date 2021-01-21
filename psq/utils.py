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
import time
import io
import builtins

safe_builtins = {
    'range',
    'complex',
    'set',
    'frozenset',
    'slice',
}

try:
    import cPickle as pickle
except ImportError:  # pragma: no cover
    import pickle


logger = logging.getLogger(__name__)
dumps = partial(pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL)
loads = pickle.loads


class UnpickleError(ValueError):
    pass

class RestrictedUnpickler(pickle.Unpickler):

    def find_class(self, module, name):
        """Only allow safe classes from builtins"""
        if module == "builtins" and name in safe_builtins:
            return getattr(builtins, name)
        """Forbid everything else"""
        raise pickle.UnpicklingError("global '%s.%s' is forbidden" %
                                     (module, name))

def rloads(s):
    """Helper function analogous to pickle.loads()"""
    return RestrictedUnpickler(io.BytesIO(s)).load()


def unpickle(pickled_string):
    """Unpickles a string, but raises a unified UnpickleError in case anything
    fails.
    This is a helper method to not have to deal with the fact that `loads()`
    potentially raises many types of exceptions (e.g. AttributeError,
    IndexError, TypeError, KeyError, etc.)
    """
    try:
        obj = loads(rloads(pickled_string))
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
