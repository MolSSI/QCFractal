"""
Running external processes, with graceful shutdown
"""

import traceback
import signal
import abc
import multiprocessing
import logging
import threading
import sys

from typing import Union


class SleepInterrupted(BaseException):
    """
    Exception class used to signal that an InterruptableSleep was interrupted

    This (like KeyboardInterrupt) derives from BaseException to prevent
    it from being handled with "except Exception".
    """

    pass


class InterruptableSleep:
    """
    A class for sleeping, but interruptable

    This class uses threading Events to wake up from a sleep before the entire sleep
    duration has run. If the sleep is interrupted, then an SleepInterrupted exception is raised.

    This class is a functor, so an instance can be passed as the delay function to a python
    sched.scheduler
    """
    def __init__(self):
        self._event = threading.Event()

    def __call__(self, delay: float):
        interrupted = self._event.wait(delay)
        if interrupted:
            raise SleepInterrupted()

    def interrupt(self):
        self._event.set()

    def clear(self):
        self._event.clear()


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
        mp_ctx = multiprocessing.get_context("fork")

        self._name = name
        self._subproc = mp_ctx.Process(name=name, target=self._run_process, args=(name, proc_class))
        if start:
            self.start()

    def start(self) -> None:
        if self._subproc.is_alive():
            raise RuntimeError(f"Process {self._name} is already running")
        else:
            self._subproc.start()

    def is_alive(self) -> bool:
        return self._subproc.is_alive()

    def stop(self) -> None:
        logger = logging.getLogger(f"ProcessRunner.stop[{self._name}]")

        # Skip everything if already dead
        if not self._subproc.is_alive():
            return

        # send SIGTERM 3 times, then kill
        for i in range(3):
            logger.debug(f"Sending terminate signal [Try #{i+1}]")
            self._subproc.terminate()
            self._subproc.join(5)

            if not self._subproc.is_alive():
                break

        # If still alive, send kill signal
        if self._subproc.is_alive():
            logger.warning(f"Process seems to be hanging. Sending KILL signal")
            self._subproc.kill()
            self._subproc.join()

        logger.debug("Process ended")


    def exitcode(self) -> Union[int, None]:
        return self._subproc.exitcode

    def __del__(self):
        self.stop()

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