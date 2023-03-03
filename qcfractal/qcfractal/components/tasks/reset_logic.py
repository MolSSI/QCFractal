import logging

from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.config import AutoResetConfig
from qcportal.compression import decompress

# Map from specific errors to the general error classes
error_map = {
    "unknown": "unknown_error",
    "random_error": "random_error",
    "KilledWorker": "compute_lost",
    "ManagerLost": "compute_lost",
}

logger = logging.getLogger(__name__)


def should_reset(record_orm: BaseRecordORM, config: AutoResetConfig) -> bool:
    """
    Determine if a record should be automatically reset

    Parameters
    ----------
    record_orm
        ORM of the record to inspect
    config
        Configuration of the auto-reset logic

    Returns
    -------
    :
        True if the record should be reset automatically, False if it should remain errored.
    """

    history = record_orm.compute_history

    # Kinda wrote myself into a corner with all this compression stuff...
    error_orm = [x.outputs.get("error", None) for x in history]
    error_dict = [decompress(x.data, x.compression_type) for x in error_orm]

    error_types = [x["error_type"] for x in error_dict]

    # Internal error - never restart automatically
    if "internal_fractal_error" in error_types:
        return False

    # all unique error types
    unique_errors = set(error_types)

    error_counts = {x: error_types.count(x) for x in unique_errors}

    # Map to more general error categories
    error_counts = {error_map.get(k, "unknown_error"): v for k, v in error_counts.items()}

    # Are we beyond any of the max on any?
    for err, count in error_counts.items():
        if count > getattr(config, err, 0):
            logger.debug(f"Not auto-resetting record {record_orm.id} - has {count} errors of type {err}")
            return False

    # All good I guess
    return True
