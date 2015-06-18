from codecs import open
from os import path

from setuptools import setup


here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='psq',

    version='0.1.0',

    description='A simple task queue using Google Cloud Pub/Sub',
    long_description=long_description,

    url='https://github.com/GoogleCloudPlatform/psq',

    author='Jon Wayne Parrott, Google, Inc',
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
        'Programming Language :: Python :: 3.4'

        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Operating System :: Unix',
    ],

    keywords='queue tasks background worker',

    packages=['psq'],

    install_requires=['gcloud', 'werkzeug', 'click', 'colorlog'],

    entry_points={
        'console_scripts': [
            'psqworker=psq.psqworker:main',
        ],
    },
)
