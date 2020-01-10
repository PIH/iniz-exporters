#!/usr/bin/env python3

import argparse
import subprocess as sp

parser = argparse.ArgumentParser()
parser.add_argument("--watch", action="store_true", help="Watches files and runs tests when they change.")
args, unknownargs = parser.parse_known_args()

if args.watch:
    sp.run("./env/bin/pytest-watch " + " ".join(unknownargs), shell=True)
else:
    sp.run("./env/bin/pytest " + " ".join(unknownargs), shell=True)
