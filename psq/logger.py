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

from colorlog import ColoredFormatter

logger = logging.getLogger('psq')
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()

formatter = ColoredFormatter(
    "%(log_color)s%(levelname)-8s%(reset)s %(asctime)s %(green)s%(name)s"
    "%(reset)s %(message)s",
    reset=True,
    log_colors={
        'DEBUG':    'cyan',
        'INFO':     'blue',
        'WARNING':  'yellow',
        'ERROR':    'red',
        'CRITICAL': 'red,bg_white',
    }
)

handler.setFormatter(formatter)
logger.addHandler(handler)
