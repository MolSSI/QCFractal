name: qcarchive
channels:
  - defaults
  - conda-forge
dependencies:
  - python
  - pip

  # Core dependencies
  - msgpack-python >=0.6.1
  - numpy
  - pyyaml >=5.1
  - pydantic >=1.4.0
  - requests
  - tornado

  # Security dependencies
  - bcrypt
  - cryptography

  # Storage dependencies
  - alembic
  - psycopg2 >=2.7
  - postgresql
  - sqlalchemy >=1.3

  # QCPortal dependencies
  - double-conversion >=3.0.0
  - h5py
  - pandas
  - plotly >=4.0.0
  - pyarrow >=0.13.0
  - tqdm

  # Test depends
  - codecov
  - pytest
  - pytest-cov
  - requests-mock

#   Environment specific includes
  - rdkit
  - ipyparallel
  - ipykernel
  - parsl >=0.9.0

#   QCArchive includes
  - qcengine >=0.11.0
  - qcelemental >=0.13.1

#   Pip includes
  - pip:
    - fireworks
