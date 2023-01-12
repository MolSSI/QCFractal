"""
Running external processes, with graceful shutdown
"""

import abc
import logging
import multiprocessing
import signal
import sys
import threading
import traceback
import weakref
from typing import Union, Any


class ProcessBase(abc.ABC):
    """
    A class to define functionality to be run in another process

    Classes that inherit this class define three functions. All these functions are
    run in a separate process via ProcessRunner.

    All major initialization should take place in the setup function and not the __init__ function.
    If a signal such as SIGTERM is received during setup, the interrupt function will be called immediately after
    setup, bypassing run(). The derived class must be tolerant of this.

    After run is called (and is blocking), interrupt() will be called if the process receives a SIGTERM or SIGINT.

    If 'spawn' is used (rather than 'fork'), it is likely that the derived class
    must also be pickleable (but I haven't checked this)
    """

    @abc.abstractmethod
    def setup(self) -> None:
        pass

    @abc.abstractmethod
    def run(self) -> None:
        pass

    @abc.abstractmethod
    def interrupt(self) -> None:
        pass


class ProcessRunner:
    """
    A class for running and controlling a subprocess using python multiprocessing

    This class takes a class deriving from ProcessBase, and runs it inside another process.

    Logging
    -------

    This class uses the global python logging system. It will use the name given to the initialization
    function in the name field of the logger.


    Graceful Stopping
    -----------------

    Stopping gracefully is implemented via signal handling.

    First, a cleanup function is registered to handle SIGINT (interrupt) and SIGTERM (terminate)
    signals. When one of these signals is sent to the subprocess, execution will be interrupted and
    this cleanup function will be called. This function will then call the interrupt() function of the provided class
    (derived from ProcessBase).

    If a signal is received during setup (ie, during ProcessBase.setup()) then the interrupt function is run
    immediately after setup (ie, run() is not used). The ProcessBase-derived class must be tolerant of this.

    Note that some things being run in the subprocess (like Gunicorn) set up their own signal handling, so
    the cleanup function above may not be always run.

    Error Handling
    --------------

    If an unhandled exception occurs, the subprocess exits with non-zero error code after logging the exception.
    """

    def __init__(
        self,
        name: str,
        proc_class: ProcessBase,
        start: bool = True,
    ):
        """
        Set up a process to run in the background.

        Parameters
        ----------
        name: str
            A name for this process (will be used for logging)
        proc_class: ProcessBase
            An instantiated class with functions to be run
        start: bool
            Automatically start the process after this class is initialized
        """

        # Use fork. Spawn may also work, but I prefer starting with fork and then
        # seeing if there are any issues
        # By default, some OSs and python versions use 'spawn' as the default. So we explicitly
        # set this to fork
        self._mp_ctx = multiprocessing.get_context("fork")
        self._subproc: Any = None

        self._name = name
        self._proc_class = proc_class

        # A logger for use when finalizing/destructing
        self._finalize_logger = logging.getLogger(f"ProcessRunner.stop[{name}]")

        if start:
            self.start()

    @classmethod
    def _stop(cls, subproc, logger) -> None:
        ####################################################################################
        # This is written as a class method so that it can be called by a weakref finalizer
        ####################################################################################

        if subproc is None:
            return

        # send SIGTERM 3 times, then kill
        for i in range(3):
            logger.debug(f"Sending terminate signal [Try #{i+1}]")
            subproc.terminate()
            subproc.join(5)

            if not subproc.is_alive():
                break

        # If still alive, send kill signal
        if subproc.is_alive():
            logger.warning(f"Process seems to be hanging. Sending KILL signal")
            subproc.kill()
            subproc.join()

        logger.debug("Process ended")

    def start(self) -> None:
        """
        Starts the subprocess

        If it is already running, an exception is raised
        """

        if self._subproc is None or not self._subproc.is_alive():
            self._subproc = self._mp_ctx.Process(
                name=self._name, target=self._run_process, args=(self._name, self._proc_class)
            )
            self._subproc.start()

            # Add a finalizer to shutdown when the object is deleted
            self._finalizer = weakref.finalize(self, self._stop, self._subproc, self._finalize_logger)

        elif self._subproc.is_alive():
            raise RuntimeError(f"Process {self._name} is already running")

    def is_alive(self) -> bool:
        """
        Return true if the subprocess is running, otherwise false
        """

        return self._subproc is not None and self._subproc.is_alive()

    def stop(self):
        """
        Terminate the subprocess
        """

        # Call self._finalizer. Will call _stop and also detach the finalizer
        if self._subproc is not None:
            self._finalizer()

    def exitcode(self) -> Union[int, None]:
        """
        Returns the exit code of the subprocess
        """

        return self._subproc.exitcode

    @staticmethod
    def _run_process(name: str, proc_class: ProcessBase):
        logger = logging.getLogger(f"_run_process[{name}]")

        # Have the process received a signal?
        received_signal = threading.Event()

        # Are we currently doing setup?
        doing_setup = True

        # get the current signal handlers
        sigint_handler = signal.getsignal(signal.SIGINT)
        sigterm_handler = signal.getsignal(signal.SIGTERM)

        # Use the following function to handle signals SIGINT and SIGKILL
        # Note: Some process (Gunicorn) may use their own signal handlers, so these
        #       may not run for them
        def _cleanup(sig, frame):
            signame = signal.Signals(sig).name
            logger.debug(f"In cleanup of _run_process. Received " + signame)

            received_signal.set()

            if doing_setup:
                logger.info(f"Being asked to terminate during setup. Will wait until setup is completed")
            else:
                proc_class.interrupt()

        signal.signal(signal.SIGINT, _cleanup)
        signal.signal(signal.SIGTERM, _cleanup)

        try:
            proc_class.setup()
            doing_setup = False
            if received_signal.is_set():
                # reset the signal handler to prevent any infinite loops
                # (where interrupt() may raise another signal)
                signal.signal(signal.SIGINT, sigint_handler)
                signal.signal(signal.SIGTERM, sigterm_handler)

                logger.info(f"Signal received during setup, so now interrupting/exiting")
                proc_class.interrupt()
            else:
                proc_class.run()
        except Exception as e:
            tb = "".join(traceback.format_exception(None, e, e.__traceback__))
            logger.critical(f"Exception while running {name}:\n{tb}")

            # Since this function is run within a new process, this will just exit the subprocess
            sys.exit(1)