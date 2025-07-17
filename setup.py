from setuptools import find_packages, setup

import hysds

setup(
    name="hysds",
    version=hysds.__version__,
    long_description=hysds.__description__,
    url=hysds.__url__,
    python_requires=">=3.12",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "redis>=3.2.1,<4.5.2",  # https://github.com/redis/redis-py/issues/2629
        "celery>=5.3.1,<6.0.0",
        "prompt-toolkit==1.0.18",  # TODO: celery uses new verison of click which broke this, wil remove later
        "requests>=2.31.0",
        "flower>=2.0.1",
        "eventlet>=0.33.3",
        "easywebdav>=1.2.0",
        "lxml>=3.4.0,<5.0.0",
        "httplib2>=0.9",
        'gevent>=1.1.1,<25.4.1',
        "psutil>=5.8.0",
        "filechunkio>=1.6.0",
        "boto>=2.38.0",
        "msgpack>=1.0.0",
        "awscli>=1.17.1",
        "boto3>=1.11.1",
        "backoff>=1.3.1",
        "protobuf>=3.1.0.post1",
        "google-cloud>=0.22.0",
        "google-cloud-monitoring>=0.22.0",
        "osaka>=0.0.1",
        "prov_es>=0.2.0",
        "hysds_commons>=0.1",
        "atomicwrites>=1.1.5",
        "greenlet>=0.4.15",
        "fab-classic>=1.19.2",
        "pytz",
        "pytest",
        "tabulate>=0.8.6",
        "pyyaml",
        "pottery",
    ],
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
)
