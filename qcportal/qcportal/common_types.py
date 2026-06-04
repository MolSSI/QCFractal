import base64
from typing import Any, Dict

from pydantic import StringConstraints, GetCoreSchemaHandler
from pydantic_core import core_schema
from typing_extensions import Annotated

LowerStr = Annotated[str, StringConstraints(to_lower=True)]
Max128Str = Annotated[str, StringConstraints(max_length=128)]


class BytesAnnotation:
    @classmethod
    def __get_pydantic_core_schema__(
            cls,
            _source_type: Any,
            _handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        """
        An annotation for bytes

        This is used to match the previous versions of qcportal, where bytes were serialized as base64 strings
        with a tag of "_bytes_base64_". Newer versions of pydantic support bytes natively, but we
        want to be backwards compatible.

        This also supports bytes coming in, which msgpack supports
        """

        def _from_input(value: dict[str, str] | bytes) -> bytes:
            if isinstance(value, bytes):
                return value
            if "_bytes_base64_" in value:
                return base64.b64decode(value["_bytes_base64_"])
            raise ValueError(f"Expected bytes or dict with _bytes_base64_ key, got {value}")

        def _serialize(value: bytes) -> dict[str, str]:
            return {"_bytes_base64_": base64.b64encode(value).decode("ascii")}

        return core_schema.no_info_plain_validator_function(
            _from_input,
            serialization=core_schema.plain_serializer_function_ser_schema(_serialize, when_used="json"),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, _core_schema, handler) -> Dict[str, Any]:
        return {}


QCPortalBytes = Annotated[Any, BytesAnnotation]
