#!/bin/sh

TARGET=celery-v4.2.1/celery/backends
cd $TARGET
if [ -e async.py ]
then
    mv async.py asynchronous.py
    sed -i 's/async/asynchronous/g' redis.py
    sed -i 's/async/asynchronous/g' rpc.py
fi
