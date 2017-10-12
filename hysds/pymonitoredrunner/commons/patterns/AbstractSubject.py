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

from __future__ import with_statement
import threading

class AbstractSubject:
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
        self._observers = list()
        self._lock = threading.RLock()
    # end def


    def __del__(self):
        """
        Finalizer.
        """
        pass
    # end def


    def __str__(self):
        """
        Gets the string representation of this object.
        @return: the string representation of this object.
        @rtype: str
        """
        return 'observers: %s' % (self._observers)
    # end def


    # -------------------------------------------------------------------------
    # Observer design pattern registration methods
    # -------------------------------------------------------------------------


    def getObservers(self):
        """
        Gets the observers for this subject.
        """
        return self._observers
    # end def


    def addObserver(self, observer):
        """
        Adds an observer for this subject.
        """
        with self._lock:
            self._observers.append(observer)
        # end with
    # end def


    def removeObserver(self, observer):
        """
        Removes an observer for this subject.
        """
        with self._lock:
            self._observers.remove(observer)
        # end with
    # end def


# end class

