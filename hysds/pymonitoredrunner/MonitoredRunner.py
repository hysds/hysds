#!/usr/bin/env python

# logger singleton configured in driver
import os, logging, traceback
logger = logging.getLogger()

import subprocess
import socket


from hysds.pymonitoredrunner.commons.process.AbstractInterruptableProcess import AbstractInterruptableProcess
#from hysds.pymonitoredrunner.commons.thread.AbstractInterruptableThread import AbstractInterruptableThread
from hysds.pymonitoredrunner.StreamReaderProcess import StreamReaderProcess
from hysds.pymonitoredrunner.StreamSubject import StreamSubject

import billiard
from billiard import JoinableQueue

class MonitoredRunner(AbstractInterruptableProcess):
    '''
    Execs a process while monitoring its stdout/stderr.
    Observers can be registered that handle the output stream:
    1. saves stdout/stderr to local file.
    2. sends stdout/stderr chunks to remote rabbitmq queue.
    ''' 

    def __init__(self, command, cwd, env, settings, id=None):
        """
        Initializer.
        @param command: the full command to exec. contains all options and arguments.
        @param cwd: a current working directory for the execed process.
        @param env: dict of environment variables to use for the execed process.
        @param settings: configuration from "conf/settings/json".
        @param id: an identifier for the process to run. e.g. a job id.
        """
        AbstractInterruptableProcess.__init__(self)

        self._command = command
        self._cwd = cwd
        self._env = env
        self._settings = settings
        self._id = id

        self._process = None
        self._exitCode = 1 # default to error

        # needed to store to file as work-around for billiard not correctly storing exit code into variable.
        self._exitCodeFile = os.path.join(self._cwd, "_exit_code")
        self._exitCodeHold = os.path.join(self._cwd, ".exit_code_hold")
        self._pidFile = os.path.join(self._cwd, "_pid")

        # create cwd if not exists
        if not os.path.isdir(self._cwd): os.makedirs(self._cwd)

    # end def


    def __del__(self):
        """
        Finalizer.
        """
        AbstractInterruptableProcess.__del__(self)
    # end def


    def run(self):
        """
        Executes the command and monitors the stdout/stderr.
        """

        # write out file to hold enough disk space for exit code file
        ec_fh = open(self._exitCodeHold, 'w')
        ec_fh.write("255")
        ec_fh.close()

        try:
            self._process = subprocess.Popen(self._command,
                                             stdin=subprocess.PIPE, 
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE,
                                             cwd=self._cwd,
                                             env=self._env,
                                             preexec_fn=os.setsid)
        except (OSError, ValueError) as e:
            # dump out exit code to hold file
            ec_fh = open(self._exitCodeHold, 'w')
            ec_fh.write("%d" % self._exitCode)
            ec_fh.close()

            # rename exit code hold file
            os.rename(self._exitCodeHold, self._exitCodeFile)

            # dump out empty stdout; dump error to stderr
            settingsStreamObserverFileWriter = self._settings['StreamObserverFileWriter']
            stdoutFilepath = os.path.join(self._cwd, settingsStreamObserverFileWriter['stdout_filepath'])
            with open(stdoutFilepath, 'w') as f:
                f.write("")
            stderrFilepath = os.path.join(self._cwd, settingsStreamObserverFileWriter['stderr_filepath'])
            with open(stderrFilepath, 'w') as f:
                f.write('Unable to exec "%s": %s' % (self._command, str(e)))

            # if invalid command or if Popen is called with invalid arguments.
            logger.error('Unable to exec "%s": %s' % (self._command, str(e)))
            raise RuntimeError('Unable to exec "%s": %s' % (self._command, str(e)))

        # end try-except

        # ---------------------------------------------------------------------
        # extract JSON data structure

        rabbitmq = self._settings['rabbitmq']
        queueName = rabbitmq['queue']
        queueHost = rabbitmq['hostname']

        settingsStreamObserverFileWriter = self._settings['StreamObserverFileWriter']
        stdoutFilepath = os.path.join(self._cwd, settingsStreamObserverFileWriter['stdout_filepath'])
        stderrFilepath = os.path.join(self._cwd, settingsStreamObserverFileWriter['stderr_filepath'])

        settingsStreamObserverMessenger = self._settings['StreamObserverMessenger']
        sendInterval = settingsStreamObserverMessenger['send_interval']

        # ---------------------------------------------------------------------
        # get additional info

        hostname = socket.gethostname()
        pid = self._process.pid
        pid_fh = open(self._pidFile, 'w')
        pid_fh.write("%d" % pid)
        pid_fh.close()

        # ---------------------------------------------------------------------
        # create notification subject-observers

        stdoutStreamSubject = StreamSubject()
        stderrStreamSubject = StreamSubject()

        # ----------------
        # observer to write stdout/stderr to local files
        from hysds.pymonitoredrunner.StreamObserverFileWriter import StreamObserverFileWriter
        stdoutStreamObserverFileWriter = StreamObserverFileWriter(stdoutFilepath)
        stdoutStreamSubject.addObserver(stdoutStreamObserverFileWriter)
        stderrStreamObserverFileWriter = StreamObserverFileWriter(stderrFilepath)
        stderrStreamSubject.addObserver(stderrStreamObserverFileWriter)

        # ----------------
        # observer to store lines to queue.
        # start thread that sends queue contents

        #from hysds.pymonitoredrunner.KombuMessenger import KombuMessenger
        #stdoutMessenger = KombuMessenger(queueHost, queueName, self._id, hostname, pid, 'stdout')
        #stderrMessenger = KombuMessenger(queueHost, queueName, self._id, hostname, pid, 'stderr')
        stdoutMessenger = None
        stderrMessenger = None

        stdoutQueue = JoinableQueue()
        stderrQueue = JoinableQueue()

        from hysds.pymonitoredrunner.MessagingThread import MessagingThread
        stdoutMessagingThread = MessagingThread(stdoutQueue, sendInterval, stdoutMessenger)
        stdoutMessagingThread.start()
        stderrMessagingThread = MessagingThread(stderrQueue, sendInterval, stderrMessenger)
        stderrMessagingThread.start()

        from hysds.pymonitoredrunner.StreamObserverQueue import StreamObserverQueue
        stdoutStreamObserverQueue = StreamObserverQueue(stdoutQueue)
        stdoutStreamSubject.addObserver(stdoutStreamObserverQueue)
        stderrStreamObserverQueue = StreamObserverQueue(stderrQueue)
        stderrStreamSubject.addObserver(stderrStreamObserverQueue)

        # ---------------------------------------------------------------------
        # start STDOUT/STDERR streams monitoring processes

        stdoutReaderProcess = StreamReaderProcess(self._process.stdout, stdoutStreamSubject)
        stdoutReaderProcess.start()
        stderrReaderProcess = StreamReaderProcess(self._process.stderr, stderrStreamSubject)
        stderrReaderProcess.start()

        # ---------------------------------------------------------------------
        # wait for processes to stop. still will occur when subprocess exits.

        try:
            self._exitCode = self._process.wait()

            # dump out exit code to hold file
            ec_fh = open(self._exitCodeHold, 'w')
            ec_fh.write("%d" % self._exitCode)
            ec_fh.close()

            # rename exit code hold file
            os.rename(self._exitCodeHold, self._exitCodeFile)


            logger.debug("After calling wait() on process, got status: %s" % self._exitCode)
        except Exception, e:
            logger.warn("Got %s exception waiting for process: %s\n%s" % 
                        (type(e), str(e), traceback.format_exc()))
        # end try-except

        # ---------------------------------------------------------------------
        # wait for threads to stop

        # the reader process threads stops by itself when EOF is reached in stdout/stderr.
        stdoutReaderProcess.join()
        stderrReaderProcess.join()

        # need to manually tell the messenger stop its threads
        stdoutQueue.join()
        stderrQueue.join()
        stdoutMessagingThread.stop()
        stderrMessagingThread.stop()

    # end def


    def stop(self):
        """
        Stops the subprocess.
        """
        self._process.terminate()
    # end def


    def getExitCodeFile(self):
        """
        Returns the path to the subprocess' exit code file.
        """
        return self._exitCodeFile


    def getExitCode(self):
        """
        Gets the subprocess' exit code. Returns None if process has not terminated yet.
        @see: U{http://docs.python.org/2/library/subprocess.html}
        """
        # TODO: when using billiard, exit code is None for some reason
        #return self._exitCode # set by subprocess.wait()
        return int(open(self._exitCodeFile).read())


    def getPid(self):
        """
        Gets the subprocess' PID. Returns None if process file is not found.
        @see: U{http://docs.python.org/2/library/subprocess.html}
        """
        # TODO: when using billiard, exit code is None for some reason
        #return self._exitCode # set by subprocess.wait()
        if os.path.exists(self._pidFile):
            return int(open(self._pidFile).read())
        else: return None
    # end def
    

# end class
