#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# The repository's checks, in a container (test/run.sh checks).
set -euo pipefail
python3 -I "$LT_SRC/checks/abi.py" "$LT_SRC"
