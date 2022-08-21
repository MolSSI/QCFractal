from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import List, Optional, Tuple
    from qcportal.base_models import ProjURLParameters


def prefix_projection(proj_params: ProjURLParameters, prefix: str) -> Tuple[Optional[List[str]], Optional[List[str]]]:
    """
    Prefixes includes and excludes with a string

    This is used for mapping a set of includes/excludes to a relationship of an ORM. For example,
    you may have an endpoint for molecules of a computation (/record/1/molecule) which contains
    include/exclude in its url parameters. This function is used to map those includes/excludes to
    the "molecule" relationship of the record.
    """

    ch_includes = proj_params.include
    ch_excludes = proj_params.exclude

    base = prefix.strip(".")
    p = base + "."

    if ch_includes is None:
        # If nothing is specified, include the defaults of the child
        ch_includes = [base]
    else:
        # Otherwise, prefix all entries with whatever was specified
        ch_includes = [p + x for x in ch_includes]

    if ch_excludes:
        ch_excludes = [p + x for x in ch_excludes]

    return ch_includes, ch_excludes
