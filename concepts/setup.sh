#! /bin/bash

python3 -m venv env
./env/bin/pip install --upgrade pip
./env/bin/pip install -r requirements.txt
./env/bin/pre-commit install
