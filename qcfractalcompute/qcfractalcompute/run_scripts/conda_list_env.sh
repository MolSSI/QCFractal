#!/bin/bash

set -eu

if command -v conda &>/dev/null 2>&1
then
    conda list --json
elif command -v mamba &>/dev/null 2>&1
then
    mamba list --json
elif command -v micromamba &>/dev/null 2>&1
then
    micromamba list --json
fi