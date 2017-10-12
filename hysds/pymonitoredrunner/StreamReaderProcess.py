#!/usr/bin/env python

# logger singleton configured in driver
import logging
logger = logging.getLogger()

from hysds.pymonitoredrunner.commons.process.AbstractInterruptableProcess import AbstractInterruptableProcess

class StreamReaderProcess(AbstractInterruptableProcess):

    def __init__(self, stream, streamSubject):
        """
        Initializer.
        """
        AbstractInterruptableProcess.__init__(self)
        self._stream = stream
        self._streamSubject = streamSubject
    # end def


    def __del__(self):
        """
        Finalizer.
        """
        AbstractInterruptableProcess.__del__(self)
    # end def


    def run(self):
        """
        Process loop that consumes the stream content.
        """
        # ----------------------------------------------------------------------
        # process loop

        # reset to true
        self._isRunnable = True

        while self._isRunnable:

            line = self._stream.readline()

            if line:
                self._streamSubject.notifyLine(line)
            else: # stop process if read EOF
                self._streamSubject.notifyEOF()
                self._isRunnable = False
            # end if

        # end while

    # end def

# end class
