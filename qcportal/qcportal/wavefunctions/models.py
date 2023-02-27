from pydantic import Extra
from qcelemental.models.results import WavefunctionProperties

from qcportal.largebinary import LargeBinary


class Wavefunction(LargeBinary):
    """
    Storage of native files, with compression
    """

    class Config:
        extra = Extra.forbid

    record_id: int

    def propagate_client(self, client, record_base_url):
        self._client = client
        self.data_url_ = f"{record_base_url}/wavefunction/data"
