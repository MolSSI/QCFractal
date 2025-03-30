from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Tuple, Dict, Any

from qcportal.compression import CompressionEnum, compress, decompress
from qcportal.exceptions import MissingDataError
from qcportal.record_models import RecordStatusEnum, OutputTypeEnum
from qcportal.utils import now_at_utc
from .outputstore.utils import create_output_orm
from .record_db_models import (
    RecordComputeHistoryORM,
    BaseRecordORM,
    OutputStoreORM,
    NativeFileORM,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcportal.all_results import AllResultTypes
    from typing import Dict, Tuple, Any


def build_extras_properties(result: AllResultTypes) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    # Gets rid of numpy arrays
    # Include any of these fields - not all may exist, but pydantic is lenient
    result_dict = result.dict(include={"return_result", "properties", "extras"}, encoding="json")

    new_prop = {}

    return_result = result_dict.get("return_result", None)
    if return_result is not None:
        new_prop["return_result"] = return_result

    extras = result_dict["extras"]

    # Other properties stored in qcvars in extras
    # Store them with the rest of the properties, and remove them from extras
    qcvars = extras.pop("qcvars", {})
    new_prop.update({k.lower(): v for k, v in qcvars.items()})

    properties = result_dict.get("properties", {})
    new_prop.update(properties)

    return extras, new_prop


def upsert_output(session, record_orm: BaseRecordORM, new_output_orm: OutputStoreORM) -> None:
    """
    Insert or replace an output in a records history

    Given a new output orm, if it doesn't exist, add it. If an
    output of the same type already exists, then delete that one and
    insert the new one.
    """
    if len(record_orm.compute_history) == 0:
        raise MissingDataError(f"Record {record_orm.id} does not have any compute history")

    output_type = new_output_orm.output_type
    compute_history = record_orm.compute_history[-1]

    if output_type in compute_history.outputs:
        # TODO - not sure why this is needed. Should be handled by delete-orphan
        old_orm = compute_history.outputs.pop(output_type)
        session.delete(old_orm)
        session.flush()

    compute_history.outputs[output_type] = new_output_orm


def append_output(session: Session, record_orm: BaseRecordORM, output_type: OutputTypeEnum, to_append: str):
    if not to_append:
        return

    if len(record_orm.compute_history) == 0:
        raise MissingDataError(f"Record {record_orm.id} does not have any compute history")

    compute_history = record_orm.compute_history[-1]
    if output_type in compute_history.outputs:
        out_orm = compute_history.outputs[output_type]
        out_str = decompress(out_orm.data, out_orm.compression_type)
        out_str += to_append

        new_data, new_ctype, new_clevel = compress(out_str, CompressionEnum.zstd)
        out_orm.data = new_data
        out_orm.compression_type = new_ctype
        out_orm.compression_level = new_clevel
    else:
        compute_history.outputs[output_type] = create_output_orm(output_type, to_append)

    session.flush()


def compute_history_orms_from_schema_v1(result: AllResultTypes) -> RecordComputeHistoryORM:
    """
    Retrieves status and (possibly compressed) outputs from a result, and creates
    a record computation history entry
    """
    history_orm = RecordComputeHistoryORM()
    history_orm.status = RecordStatusEnum.complete if result.success else RecordStatusEnum.error
    history_orm.provenance = result.provenance.dict()
    history_orm.modified_on = now_at_utc()

    # Get the compressed outputs if they exist
    compressed_output = result.extras.pop("_qcfractal_compressed_outputs", None)

    if compressed_output is not None:
        for output_type, data_dict in compressed_output.items():
            out_orm = OutputStoreORM(
                output_type=output_type,
                compression_type=data_dict["compression_type"],
                compression_level=data_dict["compression_level"],
                data=data_dict["data"],
            )

            history_orm.outputs[output_type] = out_orm

    else:
        if result.stdout is not None:
            stdout_orm = create_output_orm(OutputTypeEnum.stdout, result.stdout)
            history_orm.outputs["stdout"] = stdout_orm
        if result.stderr is not None:
            stderr_orm = create_output_orm(OutputTypeEnum.stderr, result.stderr)
            history_orm.outputs["stderr"] = stderr_orm
        if result.error is not None:
            error_orm = create_output_orm(OutputTypeEnum.error, result.error.dict())
            history_orm.outputs["error"] = error_orm

    return history_orm


def native_files_orms_from_schema_v1(result: AllResultTypes) -> Dict[str, NativeFileORM]:
    """
    Convert the native files stored in a QCElemental result to an ORM
    """

    compressed_nf = result.extras.pop("_qcfractal_compressed_native_files", None)

    if compressed_nf is not None:
        native_files = {}
        for name, nf_data in compressed_nf.items():
            # nf_data is a dictionary with keys 'data', 'compression_type', "compression_level"
            nf_orm = NativeFileORM(
                name=name,
                compression_type=nf_data["compression_type"],
                compression_level=nf_data["compression_level"],
                data=nf_data["data"],
            )
            native_files[name] = nf_orm

        return native_files
    elif "native_files" in result.__fields__:  # Not compressed, but part of result
        native_files = {}
        for name, nf_data in result.native_files.items():

            compressed_data, compression_type, compression_level = compress(nf_data, CompressionEnum.zstd)
            nf_orm = NativeFileORM(
                name=name,
                compression_type=compression_type,
                compression_level=compression_level,
                data=compressed_data,
            )

            native_files[name] = nf_orm

        return native_files
    else:
        return {}
