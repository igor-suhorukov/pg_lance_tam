#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# The S3 leg (docs/design.md §6): lance tables in a bucket of an S3 API --
# LocalStack, the compose project's s3 service, on the legs' network --
# through a storage server and its user mapping, and pg_regress over
# test/s3.
set -euo pipefail
. "$LT_SRC/test/server.sh"

python3 - "$LT_S3_ENDPOINT" <<'PY'
import sys, time, urllib.request
endpoint = sys.argv[1]
for _ in range(120):
    try:
        urllib.request.urlopen(endpoint + "/_localstack/health", timeout=2).read()
        break
    except Exception:
        time.sleep(1)
else:
    sys.exit("the S3 service never answered")
req = urllib.request.Request(endpoint + "/lance-test", method="PUT")
try:
    urllib.request.urlopen(req, timeout=10).read()
except urllib.error.HTTPError as e:
    if e.code != 409:
        raise
print("bucket lance-test ready")
PY

regress "$LT_SRC/test/s3" ${LT_TESTS:-s3}
