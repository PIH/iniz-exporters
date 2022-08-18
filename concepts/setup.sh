#! /bin/bash

# creates a virtual Python environment in "env" directory  (google: venv) and installed dependencies there
python3 -m venv env
./env/bin/pip install --upgrade pip
./env/bin/pip install -r requirements.txt
./env/bin/pre-commit install
