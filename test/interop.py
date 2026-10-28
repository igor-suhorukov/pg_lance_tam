# SPDX-License-Identifier: Apache-2.0
# Lance's Python package beside the server (test/interop.sh): reads what
# PostgreSQL wrote, writes what it reads.
import sys
from datetime import datetime, timezone

import lance
import pyarrow as pa


def main(step, uri):
    if step == "read":
        ds = lance.dataset(uri)
        t = ds.to_table(columns=["id", "name", "emb"]).sort_by("id")
        print("python rows:", t.num_rows)
        print("python first:", t.slice(0, 2).to_pylist())
        hits = ds.to_table(nearest={"column": "emb", "q": [1.0, 0.0, 0.0], "k": 2}, columns=["id"])
        print("python nearest:", sorted(hits.column("id").to_pylist()))
    elif step == "append":
        ds = lance.dataset(uri)
        schema = ds.schema
        rows = pa.table(
            {
                "id": pa.array([1001, 1002], pa.int64()),
                "name": pa.array(["from python", "from python too"], pa.utf8()),
                "emb": pa.FixedSizeListArray.from_arrays(pa.array([0.5, 0.5, 0.5, 9, 9, 9], pa.float32()), 3),
            }
        ).cast(schema)
        lance.write_dataset(rows, uri, mode="append")
        print("python appended; rows now:", lance.dataset(uri).count_rows())
    elif step == "index":
        ds = lance.dataset(uri)
        ds.create_scalar_index("id", index_type="BTREE")
        print("python indices:", sorted(i["name"] for i in ds.list_indices()))
    elif step == "columns":
        ds = lance.dataset(uri)
        print("python columns:", ds.schema.names)
        t = ds.to_table(columns=["id", "tag"], filter="id IN (1, 3000)").sort_by("id")
        print("python tags:", t.to_pylist())
    elif step == "count":
        print("python rows:", lance.dataset(uri).count_rows())
    elif step == "create":
        t = pa.table(
            {
                "k": pa.array([1, 2, 3], pa.int32()),
                "tag": pa.array(["a", "b", None], pa.utf8()),
                "score": pa.array([0.5, 1.5, 2.5], pa.float64()),
            }
        )
        lance.write_dataset(t, uri, enable_stable_row_ids=True)
        print("python wrote", uri)
    elif step == "stamps":
        # the same instants as the server's own types and as others
        inst = [datetime(2024, 1, 1, 0, 0, 0, 1000), datetime(2024, 1, 1, 0, 0, 0, 2000),
                datetime(2024, 1, 1, 3), datetime(2024, 1, 2)]
        utc = [i.replace(tzinfo=timezone.utc) for i in inst]
        t = pa.table(
            {
                "k": pa.array([1, 2, 3, 4], pa.int32()),
                "us": pa.array(inst, pa.timestamp("us")),
                "utc": pa.array(utc, pa.timestamp("us", tz="UTC")),
                "ms": pa.array(inst, pa.timestamp("ms")),
                "ns": pa.array(inst, pa.timestamp("ns")),
                "msk": pa.array(utc, pa.timestamp("us", tz="Europe/Moscow")),
            }
        )
        lance.write_dataset(t, uri, enable_stable_row_ids=True)
        print("python wrote", uri)
    elif step == "plain":
        t = pa.table({"k": pa.array([7, 8], pa.int64())})
        lance.write_dataset(t, uri)
        print("python wrote", uri)
    else:
        sys.exit(f"unknown step {step}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
