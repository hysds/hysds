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

from billiard import Process
import time

class AbstractInterruptableProcess(Process):
    """
    A process class that defines interruptable methods.
    """

    def __init__(self):
        """
        Initializer.
        """
        Process.__init__(self)
        self._isRunnable = True
    # end def


    def __del__(self):
        """
        Finalizer.
        """
        pass
        #Process.__del__(self) does not exists
    # end def


    def stop(self):
        """
        Stops this process if it is running.
        """
        #raise NotImplementedError('Subclasses should provide a concrete implementation.')
        self._isRunnable = False
    # end def


    def run(self):
        """
        Process loop.
        """
        raise NotImplementedError('Subclasses should provide a concrete implementation.')

#        # recommended essentials for implementation:
#
#        # reset to true
#        self._isRunnable = True
#
#        # ----------------------------------------------------------------------
#        # process loop
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
        Waits for the process to end before returning. However, the current
        version of Process.join() does not respond to KeyboardInterrupt while
        it is blocking. This version is interruptable to signals like the
        KeyboardInterrupt Ctrl-C. It does so by calling Process.join() in a
        loop with a timeout.
        """
        try:
            while self.is_alive():
                self.join(timeout) # seconds
                # this area and the while check is interruptable.
            # end while
        except KeyboardInterrupt, e:
            logger.debug('=> Process interrupted. %s' % (str(e)) )

            # raised when user presses CTRL-C
            self.stop()

            # wait a little more for the process to fully stop
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

            # wait a little more for the process to fully stop
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
