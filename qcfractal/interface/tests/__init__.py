"""
Init file for tests, blank to avoid automatic imports.
"""

try:
    # QCFractal based imports
    from ... import interface as portal
except (ImportError, ValueError):  # Catches not importable, and importing too high
    # QCPortal based import
    import qcportal as portal
