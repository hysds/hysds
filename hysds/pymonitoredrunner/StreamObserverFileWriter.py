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

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import open
from builtins import str
from future import standard_library
standard_library.install_aliases()
from builtins import object
import os
import logging
logger = logging.getLogger()


class StreamObserverFileWriter(object):
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
            # TODO: doesn't seem to write lines unless flush after every line here. this shouldn't be needed.
            self._file.flush()
        except IOError as e:
            logger.warning('Unable to write output to "%s": %s' %
                           (self._filepath, str(e)))
    # end def

    def notifyEOF(self):
        """
        Invoked when end of stream is reached.
        """
        self._file.close()
    # end def

# end class
