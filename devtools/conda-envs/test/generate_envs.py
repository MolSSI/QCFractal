"""
Automatically generates the QCArchive environments
"""
import yaml
import glob
import copy

template = {
    "name":
    "qcarchive",
    "channels": ["conda-forge"],
    "dependencies": [
        # Base
        "python",
        "numpy",
        "pandas",
        "mongodb",
        "pymongo",
        "mongoengine",
        "tornado",
        "requests",
        "jsonschema",
        "bcrypt",
        "cryptography",

        # Test
        "pytest",
        "pytest-cov",
        "codecov",
    ],
}
qca_ecosystem_template = ["qcengine>=0.4.0", "qcelemental>=0.1.3"]
pip_depends_template = ["pydantic"]


def generate_yaml(filename=None, channels=None, dependencies=None, pip_dependencies=None, qca_ecosystem=None):
    """
    Builds out a specific template, quite limited in scope.
    """

    if filename is None:
        raise KeyError("Must have a filename")

    env = copy.deepcopy(template)
    if channels is not None:
        env["channels"].extend(channels)

    # General conda depends
    if dependencies is not None:
        env["dependencies"].extend(dependencies)

    # Add in QCArchive ecosystem
    if qca_ecosystem is None:
        env["dependencies"].extend(qca_ecosystem_template)
    else:
        env["dependencies"].extend(qca_ecosystem)

    # Add in pip
    pip_env = copy.deepcopy(pip_depends_template)
    if pip_dependencies is not None:
        pip_env.extend(pip_dependencies)
    env["dependencies"].append({"pip": pip_env})

    with open(filename, "w") as handle:
        yaml.dump(env, handle, default_flow_style=False, indent=2)

environs = [{
    "filename": "base.yaml",
}, {
    "filename": "fireworks.yaml",
    "channels": ["psi4"],
    "dependencies": ["psi4"],
    "pip_dependencies": ["fireworks"]
}, {
    "filename": "dask.yaml",
    "channels": ["psi4"],
    "dependencies": ["psi4", "dask", "distributed"]
}, {
    "filename": "parsl.yaml",
    "dependencies": ["rdkit"],
    "pip_dependencies": ["parsl"]
}, {
    "filename": "openff.yaml",
    "channels": ["psi4", "rdkit"],
    "dependencies": ["dask", "distributed", "psi4", "rdkit", "torsiondrive", "geometric"]
}, {
    "filename":
    "dev_head.yaml",
    "dependencies": ["rdkit"],
    "qca_ecosystem": [],
    "pip_dependencies": [
        "git+git://github.com/MolSSI/QCEngine#egg=qcengine",
        "git+git://github.com/MolSSI/QCElemental#egg=qcengine",
        "git+git://github.com/lpwgroup/torsiondrive.git#egg=torsiondrive",
        "git+git://github.com/leeping/geomeTRIC#egg=geometric"
    ]
}]

for envdata in environs:
    generate_yaml(**envdata)
