import traceback
import signal
import multiprocessing
import logging
import abc


class EndProcess(RuntimeError):
    """
    Exception class used to signal that the process should end
    """
    pass


class FractalProcessBase(abc.ABC):

    @abc.abstractmethod
    def run(self) -> None:
        pass

    @abc.abstractmethod
    def finalize(self) -> None:
        pass


class FractalProcessRunner:
    """
    A class for running and controlling a subprocess using python multiprocessing
    """

    def __init__(
            self,
            name: str,
            mp_ctx: multiprocessing.context.BaseContext,
            proc_class: FractalProcessBase,
            start: bool = True,
    ):
        """
        Set up a process to run in the background

        Parameters
        ----------
        name: str
            A name for this process (will be used for logging)
        mp_ctx: str
            Multiprocessing context under which to create the process
        proc_class: FractalProcessBase
            An instantiated class with functions to be run
        start: bool
            Automatically start the process
        """

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
        self._subproc.terminate()
        self._subproc.join()

    def __del__(self):
        self.stop()

    @staticmethod
    def _run_process(name: str, proc_class: FractalProcessBase):
        logger = logging.getLogger(f'_run_process[{name}]')

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
            logger.debug(f"_run_process for {name} received EndProcess: " + str(e))
        except Exception as e:
            tb = ''.join(traceback.format_exception(None, e, e.__traceback__))
            logger.critical(f"Exception while running {name}:\n{tb}")
        finally:
            proc_class.finalize()