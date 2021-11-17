"""
Automatically generates the QCArchive environments
"""
import copy

from ruamel.yaml import YAML

yaml = YAML()
yaml.indent(mapping=2, sequence=2, offset=2)

template = """
name: qcarchive
channels:
  - defaults
  - conda-forge
dependencies:
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
  - pyarrow >=0.15.0
  - tqdm

  # Test depends
  - codecov
  - pytest
  - pytest-cov
  - requests-mock
"""
qca_ecosystem_template = ["qcengine >=0.17.0", "qcelemental >=0.17.0"]

pip_depends_template = []


def generate_yaml(filename=None, channels=None, dependencies=None, pip_dependencies=None, qca_ecosystem=None):
    """
    Builds out a specific template, quite limited in scope.
    """

    if filename is None:
        raise KeyError("Must have a filename")

    # Handle channels
    env = yaml.load(template)
    if channels is not None:
        for c in channels:
            env["channels"].insert(1, c)
    offset = len(env["channels"])
    env["channels"].yaml_set_comment_before_after_key(offset, before="\n")

    # General conda depends
    if dependencies is not None:
        offset = len(env["dependencies"])
        env["dependencies"].yaml_set_comment_before_after_key(offset, before="\n  Environment specific includes")
        env["dependencies"].extend(dependencies)

    # Add in QCArchive ecosystem
    offset = len(env["dependencies"])
    env["dependencies"].yaml_set_comment_before_after_key(offset, before="\n  QCArchive includes")
    if qca_ecosystem is None:
        env["dependencies"].extend(qca_ecosystem_template)
    else:
        env["dependencies"].extend(qca_ecosystem)

    # Add in pip
    pip_env = copy.deepcopy(pip_depends_template)
    if pip_dependencies is not None:
        pip_env.extend(pip_dependencies)
    if len(pip_env):
        offset = len(env["dependencies"])
        env["dependencies"].yaml_set_comment_before_after_key(offset, before="\n  Pip includes")
        env["dependencies"].extend([{"pip": pip_env}])

    with open(filename, "w") as handle:
        yaml.dump(env, handle)


environs = [
    {
        # No extra dependancies, the base env
        "filename": "base.yaml",
    },
    {
        # Tools to test out dask adapter
        "filename": "adapter_dask.yaml",
        "dependencies": ["rdkit", "dask", "distributed", "dask-jobqueue >=0.5.0"],
    },
    {
        # Tools to test out parsl adapter
        "filename": "adapter_parsl.yaml",
        "dependencies": ["rdkit", "ipyparallel", "ipykernel", "parsl >=0.9.0"],
    },
    {
        # Tools to test out fireworks adapter
        "filename": "adapter_fireworks.yaml",
        "pip_dependencies": ["fireworks"],
    },
    {
        # Tests for the OpenFF toolchain (geometric and torsiondrive)
        "filename": "openff.yaml",
        "channels": ["psi4/label/dev", "omnia"],
        "dependencies": [
            "psi4>1.4a2.dev700,<1.4a2",
            "rdkit",
            "geometric >=0.9.3",
            "torsiondrive",
            "dftd3",
            "openforcefield >=0.7.1",
            "openforcefields >=1.2.0",
            "openmm >=7.4.2",
            "openmmforcefields >=0.8.0",
        ],
    },
    {
        # Tests for the current development heads
        "filename": "dev_head.yaml",
        "dependencies": ["rdkit"],
        "qca_ecosystem": [],
        "pip_dependencies": [
            "git+git://github.com/MolSSI/QCEngine#egg=qcengine",
            "git+git://github.com/MolSSI/QCElemental#egg=qcelemental",
            "git+git://github.com/leeping/geomeTRIC#egg=geometric",
            "git+git://github.com/lpwgroup/torsiondrive.git#egg=torsiondrive",
        ],  # yapf: disable
    },
]

for envdata in environs:
    generate_yaml(**envdata)
