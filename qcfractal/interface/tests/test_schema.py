"""
Tests the various schema involved in the project that are not tested elsewhere.
"""
import numpy as np
import pytest

from ... import interface as dqm

def test_options():
    opts = dqm.data.get_options("psi_default")

    dqm.schema.validate(opts, "options")
