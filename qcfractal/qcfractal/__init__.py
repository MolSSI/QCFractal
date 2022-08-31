###################################################
# The version stuff must be handled first.
# Other packages that we import later will need it
###################################################

import os
from importlib.metadata import version

__version__ = version("qcfractal")

# The location of this file
qcfractal_topdir = os.path.abspath(os.path.dirname(__file__))
