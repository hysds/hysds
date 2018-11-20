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
        'redis>=3.0.1', 'celery==3.1.25.pqueue', 'requests>=2.20.0',
        'supervisor>=3.1.3', 'flower>=0.8.2', 'eventlet>=0.17.2',
        'easywebdav>=1.2.0', 'fabric>=1.10.1', 'lxml>=3.4.0',
        'httplib2>=0.9', 'gevent>=1.0.1', 'psutil>=2.1.3',
        'filechunkio>=1.6.0', 'boto>=2.38.0', 'msgpack-python>=0.4.6', 
        'boto3>=1.2.6', 'backoff>=1.3.1', 'protobuf>=3.1.0.post1', 
        'google-cloud>=0.22.0', 'google-cloud-monitoring>=0.22.0', 
        'osaka>=0.0.1', 'prov_es==0.1.1', 'hysds_commons>=0.1',
        'atomicwrites>=1.1.5'
    ]
)
