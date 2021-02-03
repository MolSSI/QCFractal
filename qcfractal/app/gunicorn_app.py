from __future__ import annotations
import logging
import logging.handlers
import signal
import multiprocessing
import gunicorn.app.base
from gunicorn.glogging import Logger as GLogger
from .flask_app import create_qcfractal_flask_app

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..config import FractalConfig, Optional

# Signalling of process termination via an exception
class EndProcess(RuntimeError):
    pass

#####################################################
# See https://docs.gunicorn.org/en/stable/custom.html
#####################################################
class FractalGunicornLogger(GLogger):
    def setup(self, cfg):
        # The base constructor messes with logging. So undo the important parts
        # of those changes. That way, all logging will be unified
        self.error_log = logging.getLogger("gunicorn.error")
        self.access_log = logging.getLogger("gunicorn.access")
        self.error_log.handlers = []
        self.access_log.handlers = []
        self.error_log.propagate = True
        self.access_log.propagate = True

class FractalGunicornApp(gunicorn.app.base.BaseApplication):

    def __init__(self,  qcfractal_config: FractalConfig):
        self.qcfractal_config = qcfractal_config
        self.application = create_qcfractal_flask_app(qcfractal_config)

        super().__init__()

    def load(self):
        return self.application

    def load_config(self):
        # This must be provided. It is called from the superclass, and we need to
        # populate self.cfg
        bind = self.qcfractal_config.flask.bind
        port = self.qcfractal_config.flask.port

        config = {'bind': f'{bind}:{port}',
                  'workers': self.qcfractal_config.flask.num_workers,
                  'loglevel': self.qcfractal_config.loglevel,
                  'logger_class': FractalGunicornLogger
                  }

        for key, value in config.items():
            self.cfg.set(key.lower(), value)


class FractalGunicornProcess:
    """
    Creates a gunicorn app in a separate process, and allows for control
    """

    def __init__(
            self,
            qcf_config: FractalConfig,
            mp_context: multiprocessing.context.BaseContext,
            start: bool = True
    ):
        self._qcf_config = qcf_config
        self._mp_ctx = mp_context

        self._gunicorn_process = self._mp_ctx.Process(name="gunicorn_proc", target=FractalGunicornProcess._run_gunicorn, args=(self._qcf_config,))
        if start:
            self.start()

    def start(self):
        if self._gunicorn_process.is_alive():
            raise RuntimeError("Gunicorn process is already running")
        else:
            self._gunicorn_process.start()


    def stop(self):
        self._gunicorn_process.terminate()
        self._gunicorn_process.join()

    def __del__(self):
        self.stop()

    @staticmethod
    def _run_gunicorn(qcf_config: FractalConfig):
        logger = logging.getLogger()

        def _cleanup(sig, frame):
            signame = signal.Signals(sig).name
            logger.debug("In cleanup of _run_gunicorn. Received " + signame)
            raise EndProcess(signame)

        signal.signal(signal.SIGINT, _cleanup)
        signal.signal(signal.SIGTERM, _cleanup)

        # Start the gunicorn/flask server
        try:
            gunicorn_app = FractalGunicornApp(qcf_config)
        except Exception as e:
            logger.critical(f"Exception while starting gunicorn app: str{e}")
            exit(1)

        try:
            gunicorn_app.run()
        except EndProcess as e:
            logger.debug("_run_gunicorn received EndProcess: " + str(e))
            exit(0)
        except Exception as e:
            logger.critical(f"Exception while running gunicorn app: str{e}")
            exit(1)
