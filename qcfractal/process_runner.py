"""
Running external processes, with graceful shutdown
"""

import traceback
import signal
import multiprocessing
import logging
import abc


class EndProcess(BaseException):
    """
    Exception class used to signal that the process should end

    This (like KeyboardInterrupt) derives from BaseException to prevent
    it from being handled with "except Exception". Without this, sometimes
    exceptions wouldn't really interrupt a running process if it is thrown while
    the process is running certain codes
    """

    pass


class ProcessBase(abc.ABC):
    """
    A class to define functionality to be run in another process

    Classes that inherit this class define two functions. Both these functions are
    run in a separate process via ProcessRunner.

    Since both functions are run in a separate function, any major initialization
    should take place there and not in an __init__ function.

    If 'spawn' is used (rather than 'fork'), it is likely that the derived class
    must also be pickleable (but I haven't checked this)
    """

    @abc.abstractmethod
    def run(self) -> None:
        pass

    @abc.abstractmethod
    def finalize(self) -> None:
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

    Stopping gracefully is implemented via signal handling and exceptions.

    First, a cleanup function is registered to handle SIGINT (interrupt) and SIGTERM (terminate)
    signals. When one of these signals is sent to the subprocess, execution will be interrupted and
    this function will be called. This function then raises an EndProcess exception. When control returns
    to whatever was executing when the signal was received, the exception handling procedure for python will be
    started.

    The run function defined by the ProcessBase-derived class will then end semi-gracefully due to the exception
    (similarly to how it would end with KeyboardInterrupt).

    The EndProcess exception is then handled by calling the finalize function of the ProcessBase-derived class.

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
        if self._subproc.is_alive():
            self._subproc.terminate()
            self._subproc.join()

    def __del__(self):
        self.stop()

    @staticmethod
    def _run_process(name: str, proc_class: ProcessBase):
        logger = logging.getLogger(f"_run_process[{name}]")

        # Use the following function to handle signals SIGINT and SIGKILL
        # Note: Some process (Gunicorn) may use their own signal handlers, so these
        #       may not run for them
        def _cleanup(sig, frame):
            signame = signal.Signals(sig).name
            logger.debug(f"In cleanup of _run_process. Received " + signame)
            raise EndProcess(signame)

        signal.signal(signal.SIGINT, _cleanup)
        signal.signal(signal.SIGTERM, _cleanup)

        try:
            proc_class.run()
        except EndProcess as e:
            logger.debug(f"_run_process received EndProcess: " + str(e))
            proc_class.finalize()
            exit(0)
        except Exception as e:
            tb = "".join(traceback.format_exception(None, e, e.__traceback__))
            logger.critical(f"Exception while running {name}:\n{tb}")

            # Since this function is run within a new process, this will just exit the subprocess
            exit(1)
