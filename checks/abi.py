# SPDX-License-Identifier: Apache-2.0
"""The C ABI declared twice -- include/lance_engine.h for the extension,
engine/src/lib.rs and engine/src/ffi.rs for the library -- must agree member
for member: the structs the extension fills and the library reads, and the
table of entry points.  Run by test/run.sh checks, in a container."""
import re
import sys

root = sys.argv[1] if len(sys.argv) > 1 else "."
header = open(f"{root}/include/lance_engine.h").read()
lib = open(f"{root}/engine/src/lib.rs").read()
ffi = open(f"{root}/engine/src/ffi.rs").read()


def c_struct(name):
    m = re.search(r"typedef struct %s\s*\{(.*?)\}\s*%s;" % (name, name), header, re.S)
    if not m:
        sys.exit(f"abi: no struct {name} in the header")
    body = re.sub(r"/\*.*?\*/", "", m.group(1), flags=re.S)
    names = []
    for decl in body.split(";"):
        decl = decl.strip()
        if not decl:
            continue
        fn = re.search(r"\(\*\s*(\w+)\)", decl)
        if fn:
            names.append(fn.group(1))
        else:
            names.append(re.findall(r"(\w+)\s*(?:\[[^\]]*\])?$", decl)[0])
    return names


def rust_struct(source, name):
    m = re.search(r"pub struct %s \{(.*?)\n\}" % name, source, re.S)
    if not m:
        sys.exit(f"abi: no struct {name} in the library")
    body = re.sub(r"//[^\n]*", "", m.group(1))
    return re.findall(r"^\s*(?:pub\s+)?(\w+):", body, re.M)


failed = False
for name, source in [("LeError", ffi), ("LeConfig", ffi), ("LeDatasetInfo", ffi),
                     ("LeScanSpec", ffi), ("LeWriteSpec", ffi), ("LeApi", lib)]:
    c, r = c_struct(name), rust_struct(source, name)
    if c != r:
        failed = True
        print(f"abi: {name} differs")
        for i in range(max(len(c), len(r))):
            a = c[i] if i < len(c) else "-"
            b = r[i] if i < len(r) else "-"
            print(f"   {'  ' if a == b else '!='} {a:28} {b}")
    else:
        print(f"abi: {name}: {len(c)} members agree")
sys.exit(1 if failed else 0)
