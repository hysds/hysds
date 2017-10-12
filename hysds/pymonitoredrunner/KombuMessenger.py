#!/usr/bin/env python

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#                        NASA Jet Propulsion Laboratory
#                      California Institute of Technology
#                        (C) 2008  All Rights Reserved
#
# <LicenseText>
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import logging
logger = logging.getLogger()

from datetime import datetime
from kombu import Connection, Exchange, Queue
from kombu.utils.debug import setup_logging
from kombu.common import maybe_declare
import json


class KombuMessenger:
    """
    Sends messages via Kombu.
    """

    def __init__(self, queueHost, queueName, id, hostname, pid, type):
        """
        Initializer.
        """
        self._queueHost = queueHost
        self._queueName = queueName
        self._id = id
        self._hostname = hostname
        self._pid = pid
        self._type = type

        self._connection = Connection('pyamqp://guest:guest@%s:5672//' % self._queueHost)
        self._connection.ensure_connection()
        self._exchange = Exchange(self._queueName, type='direct')
        self._queue = Queue(self._queueName, self._exchange, routing_key=self._queueName)
        self._producer = self._connection.Producer()
        self._publish = self._connection.ensure(self._producer, self._producer.publish, max_retries=3)

    # end def


    def __del__(self):
        """
        Finalizer.
        """
        self._connection.close()
    # end def


    def __str__(self):
        """
        Gets the string representation of this object.
        @return: the string representation of this object.
        @rtype: str
        """
        return 'connection: "%s", id: "%s", queueName: "%s", hostname: "%s", pid: "%s", type: "%s"' % (self._connection, self._id, self._queueName, self._hostname, self._pid, self._type)
    # end def


    def send(self, chunk):
        """
        Send stream chunk with JSON descriptor.
        """
        context = {
                'id': self._id,
                'datetime': datetime.isoformat( datetime.now() ),
                'hostname': self._hostname,
                'pid': self._pid,
                'type': self._type,
                'chunk': chunk
        }
        #contextStr = json.dumps(context)

        self._publish(context, routing_key=self._queueName, declare=[self._queue])
        #with self._connection.Producer() as producer:
        #    publish = self._connection.ensure(producer, producer.publish, max_retries=3)
        #    publish(context, routing_key=self._queueName, declare=[self._queue])
            #print 'channel.basic_publish(): %s' % contextStr
    # end def


# end class

