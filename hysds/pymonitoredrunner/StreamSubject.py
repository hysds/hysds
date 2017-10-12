#!/usr/bin/env python

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#                        NASA Jet Propulsion Laboratory
#                      California Institute of Technology
#                        (C) 2011  All Rights Reserved
#
# <LicenseText>
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from hysds.pymonitoredrunner.commons.patterns.AbstractSubject import AbstractSubject

class StreamSubject(AbstractSubject):
    """
    An abstract class implementation of the Observer design pattern where this is
    the Subject. Observers can be registered with this class
    to receive notifications.
    Subclasses should implement the thread-safe notification methods.
    @note: the registration methods are thread-safe.
    """

    def __init__(self):
        """
        Initializer.
        """
        AbstractSubject.__init__(self)
    # end def


    def __del__(self):
        """
        Finalizer.
        """
        AbstractSubject.__del__(self)
    # end def


    def __str__(self):
        """
        Gets the string representation of this object.
        @return: the string representation of this object.
        @rtype: str
        """
        return '%s' % ( AbstractSubject.__str__(self) )
    # end def


    # -------------------------------------------------------------------------
    # Observer design pattern notification methods
    # -------------------------------------------------------------------------

    def notifyLine(self, line):
        """
        Invoked after a new line of data is read from the stream.
        """
        for observer in self._observers:
            observer.notifyLine(line)
        # end for
    # end def


    def notifyEOF(self):
        """
        Invoked when end of stream is reached.
        """
        for observer in self._observers:
            observer.notifyEOF()
        # end for
    # end def



# end class

