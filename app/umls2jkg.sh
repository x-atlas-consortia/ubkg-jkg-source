#!/bin/bash
# 2026
# umls2jkg.sh establishes the Python virtual environment for the umls2jkg

# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.
VENV=./venv

# Check for install of Python
which python3
status=$?
if [[ $status != 0 ]] ; then
    echo '*** Python3 must be installed.'
    exit
fi

if [[ -d ${VENV} ]] ; then
    echo "*** Using Python venv in ${VENV}"
    source ${VENV}/bin/activate
else
    echo "*** Installing Python venv to ${VENV}"
    python3 -m venv ${VENV}
    source ${VENV}/bin/activate
    python -m pip install --upgrade pip
    echo "*** Installing required packages..."
    pip install -r requirements.txt
    echo "*** Done installing python venv"
fi

# Ensure the virtual environment is activated
if [[ -z "$VIRTUAL_ENV" ]]; then
    echo "Virtual environment is NOT active."
    exit 1
else
    echo "Virtual environment is active: $VIRTUAL_ENV"
fi

echo "Python binary being used:"
which python

# Ensure pythonjsonlogger is installed
if ! (pip show python-json-logger > /dev/null); then
    echo 'Installing python-json-logger...'
    pip install python-json-logger
fi

echo "Running ingest_sab.py in venv..."p
python umls2jkg.py "$@"

