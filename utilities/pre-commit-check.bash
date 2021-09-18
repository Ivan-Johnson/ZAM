#!/bin/bash
#exit on Error (even within a pipeline) and treat Unset variables as errors
set -euo pipefail
#for tracing
#set -x

cd "$(dirname "$0")"
cd ..

mypy *.py --disallow-any-expr
black *.py
