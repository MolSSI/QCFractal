# QCFractal Pre-built Conda Environments

The QCFractal program has few requirements on its own `meta.yaml` file, however,
you may want to emulate the server side of things on your own. To help make that 
possible, we have provided the various YAML files here which can be used 
to quickly and mostly automatically build a working environment for to emulate 
the server. 

These use the `conda env create` commands (examples below) instead of the 
more common `conda create` (the commands are slightly different as of writting, 
circa Conda 4.3), so note the difference in commands.

## Requirements to use Environments

1. `git`
2. `conda`
3. Network access

## Setup/Install

Run the following command to configure a new environment with the replacements:

* `{name}`: Replace with whatever you want to call the new env
* `{file}`: Replace with target file

```bash
conda env create -n {name} -f {file}
```

To access the new environment:
```bash
conda activate {name}
```
