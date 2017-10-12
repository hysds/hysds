#!/usr/bin/env python

# logger singleton configured in driver
import logging
logger = logging.getLogger()

#import threading
import Queue

import billiard
from billiard import JoinableQueue
from multiprocessing import Event

#from hysds.pymonitoredrunner.commons.thread.AbstractInterruptableThread import AbstractInterruptableThread
from hysds.pymonitoredrunner.commons.process.AbstractInterruptableProcess import AbstractInterruptableProcess

class MessagingThread(AbstractInterruptableProcess):
    '''
    Periodically sends the full contents of the queue to messaging.
    Stops when a None is popped from the queue.
    '''

    def __init__(self, queue, sendInterval, messenger=None):
        """
        Initializer.
        """
        AbstractInterruptableProcess.__init__(self)
        self._queue = queue
        self._sendInterval = sendInterval
        self._messenger = messenger
    # end def


    def __del__(self):
        """
        Finalizer.
        """
        AbstractInterruptableProcess.__del__(self)
    # end def


    def run(self):
        """
        Thread loop that consumes the queue.
        """
        # ----------------------------------------------------------------------
        # thread loop

        # reset to true
        self._isRunnable = True

        # buffer queue items into list
        items = list()

        event = Event()

        while self._isRunnable:

            # get all items in queue to send together

            while True:
                try:
                    item = self._queue.get_nowait()
                    
                    # stop thread when done with queue
                    if item == None:
                        self._isRunnable = False
                    else:
                        items.append(item)
                    # end if

                    self._queue.task_done()
                except Queue.Empty, e:
                    break # done getting all items from queue
                # end try-catch
            # end while

            if len(items) > 0:
                chunk = ''.join(items)

                # send the chunk
                if self._messenger is not None:
                    self._messenger.send(chunk)

                # empty items for next iteration
                del items[:]
            # end if

            # thread sleep
            event.wait(self._sendInterval)
        # end while

    # end def

# end class
