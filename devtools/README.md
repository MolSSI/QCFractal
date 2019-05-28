# Development, testing, and deployment tools

This directory contains a collection of tools for running Continuous Integration (CI) tests, 
conda installation, and other development tools not directly related to the coding process.


## Manifest

### Continuous Integration

You should test your code, but do not feel compelled to use these specific programs. You also may not need Unix and 
Windows testing if you only plan to deploy on specific platforms. These are just to help you get started

* `travis-ci`: Linux and OSX based testing through [Travis-CI](https://about.travis-ci.com/) 
  * `before_install.sh`: Pip/Miniconda installation script for Travis

### Conda Environment:

These directories contain the files to build [Conda](https://conda.io/) environments for rapid setup and testing

* `scripts`: Helper and utility scripts kept here to keep other folders and their content within the same context
  * `conda_env.py`: Helper script with some specific CLI flags for the `conda env` command to install Conda Environment files quickly
* `conda-envs`: Developer environments for quickly setting up different testing and software stacks to be installed 
  alongside Fractal. These do *not* install Fractal on their own and are expected to be used with either `setup.py develop` 
  or `pip install -e` installs of Fractal from source.
* `prod-envs`: End-user production install files for Fractal. These install Fractal from the Anaconda Cloud for use 
  at production level sites. No further installations needed from these files. 


## How to contribute changes
- Fork the repository
  * We do not generally permit anyone make direct changes to the main QCFractal repository, even core developers. 
    Exceptions to this rule are rare and considered on a case-by-case basis, and thus far have only been to fix
    previous mistaken merges. 
- Clone your fork
- Make a new branch with `git checkout -b {your branch name}`
- Make changes and test your code (contribute new tests or modify old ones as needed)
- Push the branch to the repo (either the main or your fork) with `git push -u origin {your branch name}`
  * Note that `origin` is the default name assigned to the remote, yours may be different
- Make a PR on GitHub with your changes
  * We recommend setting the `Allow edits from maintainers` tick box as it makes it easier for other maintainers to 
    directly make changes to the PR as needed. Although not often used, it is helpful for large PR's where many commits 
    or contributors are expected.
- We'll review the changes and get your code into the repo after lively discussion!


## Checklist for updates and releases
- [ ] Update the changelog for both Fractal and Portal
- [ ] Ensure the minimum and maximum allowed Client versions the Server reports are up to date, including 
  what version you are about to bump up to.
- [ ] Create the PR with the changelog updates
- [ ] Debug the PR as needed until tests pass
- [ ] Get the PR merged in
- [ ] Create a GitHub Release and use the version formatted as `vXX.YY.ZZ` for the tag name.
- [ ] When ready, create a PyPi release from the checkout of the tag you made above
- [ ] Create a new release on conda forge, updating to the version you just uploaded on PyPi

## Versioneer Auto-version
[Versioneer](https://github.com/warner/python-versioneer) will automatically infer what version 
is installed by looking at the `git` tags and how many commits ahead this version is. The format follows 
[PEP 440](https://www.python.org/dev/peps/pep-0440/) and has the regular expression of:
```regexp
\d+.\d+.\d+(?\+\d+-[a-z0-9]+)
```
If the version of this commit is the same as a `git` tag, the installed version is the same as the tag, 
e.g. `qcfractal-0.1.2`, otherwise it will be appended with `+X` where `X` is the number of commits 
ahead from the last tag, and then `-YYYYYY` where the `Y`'s are replaced with the `git` commit hash.
