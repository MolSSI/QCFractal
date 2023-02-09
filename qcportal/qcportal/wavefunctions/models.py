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

    def fetch(self):
        url = f"v1/records/singlepoint/{self.record_id}/wavefunction/data"
        self._fetch_from_url(url)
