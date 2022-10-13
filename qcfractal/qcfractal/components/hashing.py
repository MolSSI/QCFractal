from __future__ import annotations

import json
from hashlib import sha256
from typing import Dict, Any

from qcportal.serialization import _JSONEncoder


def hash_dict(d: Dict[str, Any]) -> str:
    j = json.dumps(d, ensure_ascii=True, sort_keys=True, cls=_JSONEncoder).encode("utf-8")
    return sha256(j).hexdigest()
