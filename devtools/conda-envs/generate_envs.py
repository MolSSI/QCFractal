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
  - python

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
"""
qca_ecosystem_template = ["qcengine >=0.11.0", "qcelemental >=0.13.1"]

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

        # Tools to test out all available adapters, ipy is for Parsl
        "filename":
        "adapters.yaml",
        "dependencies":
        ["rdkit", "dask", "distributed", "dask-jobqueue >=0.5.0", "ipyparallel", "ipykernel", "parsl >=0.9.0"],
        "pip_dependencies": ["fireworks"]
    },
    {

        # Tests for the OpenFF toolchain (geometric and torsiondrive)
        "filename": "openff.yaml",
        "channels": ["psi4/label/dev"],
        "dependencies": ["psi4>=1.3", "rdkit", "geometric >=0.9.3", "torsiondrive", "dftd3"],
    },
    {

        # Tests for the current development heads
        "filename":
        "dev_head.yaml",
        "dependencies": ["rdkit"],
        "qca_ecosystem": [],
        "pip_dependencies": [
            "git+git://github.com/MolSSI/QCEngine#egg=qcengine",
            "git+git://github.com/MolSSI/QCElemental#egg=qcelemental",
            "git+git://github.com/leeping/geomeTRIC#egg=geometric",
            "git+git://github.com/lpwgroup/torsiondrive.git#egg=torsiondrive",
        ]  # yapf: disable
    }
]

for envdata in environs:
    generate_yaml(**envdata)
