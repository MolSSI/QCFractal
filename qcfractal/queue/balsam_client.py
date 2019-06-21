"""
Client for interfacing with Balsam
"""

import logging
from pydantic import BaseModel, validator
from qcelemental.util import which

logger = logging.getLogger("qcfractal.BalsamClient")


class BalsamClient(BaseModel):
    """
    Configuration interface layer between Balsam and the Fractal Compute Manager interface

    Mock client-like object to hold information with respect to
    """
    name: str = "QCArchive Job"
    wall_time_minutes: int = 6 * 60
    ranks_per_node: int = 1
    threads_per_core: int = 1
    application: str = "qcfractal_manager_balsam_interface"

    class Config:
        extra = "allow"

    @validator("wall_time_minutes", pre=True)
    def walltime_str_to_minutes(cls, v):
        if isinstance(v, str):
            #                    s  m  h   d
            converters_to_min = (0, 1, 60, 3600)
            split = tuple([int(x) for x in reversed(v.split(":"))])
            v = 0
            for multiplier, time_block in zip(converters_to_min, split):
                v += multiplier * time_block
        return v

    def __init__(self, fractal_add_balsam_app: bool = True, **kwargs):
        """

        Parameters
        ----------
        fractal_add_engine_app
        kwargs : dict
            Additional keyword arguments to add to the
        """
        from balsam import setup as balsam_setup
        from balsam.core.models import ApplicationDefinition
        super().__init__(**kwargs)
        # Mimic the CLI type invocation of Balsam
        balsam_setup()
        app_def = ApplicationDefinition.objects.filter(name=self.application)

        def get_qcfractal_balsam(raise_error=False):
            return which("qcabalsam-interface", raise_error=raise_error)

        if not app_def.exists():
            # Add app if it does not exist
            if not fractal_add_balsam_app:
                raise RuntimeError(f"Unable to find Balsam App '{self.application}' and the "
                                   f"`fractal_add_engine_app` was set to False. QCFractal's balsam interface will not "
                                   f"be auto-added to the Balsam database for safety")
            interface_bin = get_qcfractal_balsam(raise_error=True)
            interface_app = ApplicationDefinition()
            interface_app.name = self.application
            interface_app.description = "QCFractal's interface to Balsam as located bt the Fractal Manager"
            interface_app.executable = interface_bin
            # engine_app.preprocess = py_app_path(args.preprocess)
            # engine_app.postprocess = py_app_path(args.postprocess)
            interface_app.save()
            logger.debug(f"Registered the QCFractal/Balsam interface at {interface_bin} as App '{self.application}'")
        else:
            registered_fractal = app_def.values()[0]['executable']
            found_fractal = get_qcfractal_balsam(raise_error=False)
            if found_fractal != registered_fractal:
                logger.warning(f"Path of registered QCFractal/Balsam Interface App at {registered_fractal} does not "
                               f"match the path found by this client at {found_fractal}. This may be a problem, but "
                               f"something to watch for.")
            else:
                logger.debug(f"Found registered QCFractal/Balsam interface at: {found_fractal}")
