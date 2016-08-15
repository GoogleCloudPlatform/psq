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

from codecs import open

from setuptools import setup


long_description = open('README.rst', 'r', encoding='utf-8').read()


setup(
    name='psq',

    version='0.4.0',

    description='A simple task queue using Google Cloud Pub/Sub',
    long_description=long_description,

    url='https://github.com/GoogleCloudPlatform/psq',

    author='Jon Wayne Parrott',
    author_email='jonwayne@google.com',

    license='Apache Software License',

    classifiers=[
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',

        'License :: OSI Approved :: Apache Software License',

        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',

        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Operating System :: Unix',
    ],

    keywords='queue tasks background worker',

    packages=['psq'],

    install_requires=[
        'gcloud>=0.9.0',
        'retrying>=1.0.0,<2.0.0',
        'werkzeug>=0.10.0,<1.0.0',
        'click>=4.0,<5.0',
        'colorlog>=2.6.0,<3.0.0'],

    entry_points={
        'console_scripts': [
            'psqworker=psq.psqworker:main',
        ],
    },
)
