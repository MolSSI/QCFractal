"""
Init file for tests, blank to avoid automatic imports.
"""

try:
    # QCFractal based import
    from ... import interface as portal
except ImportError:
    # QCPortal based import
    from ... import qcportal as portal