from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from setuptools import setup, find_packages
import hysds

setup(
    name='hysds',
    version=hysds.__version__,
    long_description=hysds.__description__,
    url=hysds.__url__,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'redis>=3.2.1', 'celery>=4.4.0', 'requests>=2.20.0',
        'flower>=0.8.2', 'eventlet>=0.17.2', 'easywebdav>=1.2.0',
        'lxml>=3.4.0', 'httplib2>=0.9', 'gevent>=1.0.1', 
        'psutil>=2.1.3', 'filechunkio>=1.6.0', 'boto>=2.38.0',
        'msgpack>=0.6.1', 'awscli>=1.17.1', 'boto3>=1.11.1', 
        'backoff>=1.3.1', 'protobuf>=3.1.0.post1', 'google-cloud>=0.22.0', 
        'google-cloud-monitoring>=0.22.0', 'osaka>=0.0.1', 
        'prov_es>=0.2.0', 'hysds_commons>=0.1', 'atomicwrites>=1.1.5',
        'future>=0.17.1', 'greenlet>=0.4.15', 'fabric3', 'pytz',
        'pytest', 'tabulate>=0.8.6'
    ],
    setup_requires=[
        'pytest-runner'
    ],
    tests_require=[
        'pytest'
    ]
)
