Production Environments
=======================

This file contains production environments for the QCArchive ecosystem. These environments are for a mix
of general compute and specific to an individual organization.

Once environments can be upload, they can be created from anywhere using the following line:
```bash
conda env create qcarchive/env -n env_name
```

To update an environment the current environment needs to be removed:
```bash
conda env remove -n env_name
```
It should be noted that this does not remove the packages, and creating a new environment should be very quick
as most required packages should already be installed.

To upload an environment:
```bash
anaconda upload --user qcarchive env.yaml
```

