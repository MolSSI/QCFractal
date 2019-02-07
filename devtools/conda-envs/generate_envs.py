"""
Automatically generates the QCArchive environments
"""
from ruamel.yaml import YAML
import glob
import copy

yaml = YAML()
yaml.indent(mapping=2, sequence=2, offset=2)

template = """
name: qcarchive
channels:
  - defaults
  - conda-forge
dependencies:
  - python
  - numpy
  - pandas
  - mongodb
  - pymongo
  - tornado
  - requests
  - bcrypt
  - cryptography
  - pydantic

# Test depends
  - pytest
  - pytest-cov
  - codecov
"""
qca_ecosystem_template = ["qcengine>=0.5.1", "qcelemental>=0.2.3"]

# Note we temporarily duplicate mongoengine as conda-forge appears to be broken
pip_depends_template = ["mongoengine"]


def generate_yaml(filename=None, channels=None, dependencies=None, pip_dependencies=None, qca_ecosystem=None):
    """
    Builds out a specific template, quite limited in scope.
    """

    if filename is None:
        raise KeyError("Must have a filename")

    # Handle channels
    env = yaml.load(template)
    if channels is not None:
        env["channels"][1:1] = channels
    offset = len(env["channels"])
    env["channels"].yaml_set_comment_before_after_key(offset, before="\n")

    # General conda depends
    if dependencies is not None:
        offset = len(env["dependencies"])
        env["dependencies"].yaml_set_comment_before_after_key(offset, before="\nEnvironment specific includes")
        env["dependencies"].extend(dependencies)

    # Add in QCArchive ecosystem
    offset = len(env["dependencies"])
    env["dependencies"].yaml_set_comment_before_after_key(offset, before="\nQCArchive includes")
    if qca_ecosystem is None:
        env["dependencies"].extend(qca_ecosystem_template)
    else:
        env["dependencies"].extend(qca_ecosystem)

    # Add in pip
    pip_env = copy.deepcopy(pip_depends_template)
    if pip_dependencies is not None:
        pip_env.extend(pip_dependencies)
    offset = len(env["dependencies"])
    env["dependencies"].yaml_set_comment_before_after_key(offset, before="\nPip includes")
    env["dependencies"].extend([{"pip": pip_env}])

    with open(filename, "w") as handle:
        yaml.dump(env, handle)


environs = [{
    "filename": "base.yaml",
}, {
    "filename": "adapters.yaml",
    "dependencies": ["rdkit", "dask", "distributed"],
    "pip_dependencies": ["fireworks", "parsl"]
}, {
    "filename": "openff.yaml",
    "channels": ["psi4"],
    "dependencies": ["psi4", "rdkit", "geometric", "torsiondrive"],
}, {
    "filename":
    "dev_head.yaml",
    "dependencies": ["rdkit"],
    "qca_ecosystem": [],
    "pip_dependencies": [
        "git+git://github.com/MolSSI/QCEngine#egg=qcengine",
        "git+git://github.com/MolSSI/QCElemental#egg=qcelemental",
        "git+git://github.com/lpwgroup/torsiondrive.git#egg=torsiondrive",
        "git+git://github.com/leeping/geomeTRIC#egg=geometric"
    ] # yapf: disable
}]

for envdata in environs:
    generate_yaml(**envdata)
