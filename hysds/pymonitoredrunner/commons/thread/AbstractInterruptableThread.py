#!/usr/bin/env python
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#                        NASA Jet Propulsion Laboratory
#                      California Institute of Technology
#                      (C) 2008-2011  All Rights Reserved
#
# <LicenseText>
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import logging
logger = logging.getLogger()

from threading import Thread
import time

class AbstractInterruptableThread(Thread):
    """
    A thread class that defines interruptable methods.
    """

    def __init__(self):
        """
        Initializer.
        """
        Thread.__init__(self)
        self._isRunnable = True
    # end def


    def __del__(self):
        """
        Finalizer.
        """
        pass
        #Thread.__del__(self) does not exists
    # end def


    def stop(self):
        """
        Stops this thread if it is running.
        """
        #raise NotImplementedError('Subclasses should provide a concrete implementation.')
        self._isRunnable = False
    # end def


    def run(self):
        """
        Thread loop.
        """
        raise NotImplementedError('Subclasses should provide a concrete implementation.')

#        # recommended essentials for implementation:
#
#        # reset to true
#        self._isRunnable = True
#
#        # ----------------------------------------------------------------------
#        # thread loop
#
#        while self._isRunnable:
#
#            # ------------------------------------------------------------------
#            # sleep a little
#            self.interruptableSleep(seconds)
#
#        # end while

    # end def


    def interruptableJoin(self, timeout=3):
        """
        Waits for the thread to end before returning. However, the current
        version of Thread.join() does not respond to KeyboardInterrupt while
        it is blocking. This version is interruptable to signals like the
        KeyboardInterrupt Ctrl-C. It does so by calling Thread.join() in a
        loop with a timeout.
        @see: U{http://mail.python.org/pipermail/python-bugs-list/2007-July/039156.html}
        "Threading.Thread.join() uses a lock to put itself on the "wait_queue".
        It then calls thread.Lock.acquire(), which blocks indefinitely.
        PyThread_acquire_lock() will not return until the lock is acquired, even
        if a signal is sent.   Effectively, Ctrl-C is "masked" until the lock is
        released, (the joined thread is done), and the main thread comes back
        to the interpreter and handles the signal, producing a KeyboardInterrupt Exception."
        @param timeout: number of float seconds to wait in the join() before
        checking again. The time in between waits IS interruptable.
        @raise KeyboardInterrupt: raised when user presses CTRL-C to send interrupt signal.
        """
        try:
            while self.isAlive():
                self.join(timeout) # seconds
                # this area and the while check is interruptable.
            # end while
        except KeyboardInterrupt, e:
            logger.debug('=> Thread interrupted. %s' % (str(e)) )

            # raised when user presses CTRL-C
            self.stop()

            # wait a little more for the thread to fully stop
            while self.isAlive():
                self.join(timeout) # seconds
                # this area and the while check is interruptable.
            # end while

            # propagate exception
            raise e
        # end try-except
    # end def


    def interruptableSleep(self, seconds):
        """
        Sleep for the given seconds, but can respond to stop().
        """
        remainingSeconds = seconds
        try:
            while self._isRunnable and (remainingSeconds > 0):
                time.sleep(1)
                remainingSeconds -= 1
                # this area and the while check is interruptable.
            # end while
        except KeyboardInterrupt, e:
            logger.debug('=> Sleep interrupted. %s' % (str(e)) )

            # raised when user presses CTRL-C
            self.stop()

            # wait a little more for the thread to fully stop
            while self.isAlive():
                self.join(1) # seconds
                # this area and the while check is interruptable.
            # end while

            # propagate exception
            raise e
        # end try-except
    # end def


    def sleep(self, seconds):
        """
        Sleep for the given seconds.
        """
        time.sleep(seconds)
    # end def


# end class
