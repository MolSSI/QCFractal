from pydantic import Field, Extra

from qcportal.largebinary import LargeBinary


class NativeFile(LargeBinary):
    """
    Storage of native files, with compression
    """

    class Config:
        extra = Extra.forbid

    record_id: int
    name: str = Field(..., description="Name of the file")

    def propagate_client(self, client, record_base_url):
        self._client = client
        self.data_url_ = f"{record_base_url}/native_files/{self.name}/data"

    # def save_file(
    #    self, directory: str, new_name: Optional[str] = None, keep_compressed: bool = False, overwrite: bool = False
    # ):
    #    """
    #    Saves the file to the given directory
    #    """

    #    if new_name is None:
    #        name = self.name
    #    else:
    #        name = new_name

    #    if keep_compressed:
    #        name += get_compressed_ext(self.compression)

    #    full_path = os.path.join(directory, name)
    #    if os.path.exists(full_path) and not overwrite:
    #        raise RuntimeError(f"File {full_path} already exists. Not overwriting")

    #    with open(full_path, "wb") as f:
    #        if keep_compressed:
    #            f.write(self.data)
    #        else:
    #            # Ok if text. We won't decode into a string
    #            f.write(decompress_old_bytes(self.data, self.compression))
