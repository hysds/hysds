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

import os

class StreamObserverFileWriter:
    """
    writes stream to local file.
    """

    def __init__(self, filepath):
        """
        Initializer.
        """
        self._filepath = filepath
        self._file = open(filepath, 'w')
    # end def


    def __del__(self):
        """
        Finalizer.
        """
        self._file.close()
    # end def


    def __str__(self):
        """
        Gets the string representation of this object.
        @return: the string representation of this object.
        @rtype: str
        """
        return 'filepath: "%s"' % (self._filepath)
    # end def


    def notifyLine(self, line):
        """
        Invoked after a new line of data is read from the stream.
        Note that lines include the line separator.
        """
        try:
            self._file.write(line)
            self._file.flush() # TODO: doesn't seem to write lines unless flush after every line here. this shouldn't be needed.
        except IOError, e:
            logger.warning('Unable to write output to "%s": %s' % (self._filepath, str(e)))
    # end def


    def notifyEOF(self):
        """
        Invoked when end of stream is reached.
        """
        self._file.close()
    # end def

# end class

