#!/usr/bin/env python3
"""Diff coverage: intersect git diff added lines with a Go cover profile.

Usage: diffcover.py <repo> <base>..<head> <cover.out> [module-prefix]
Reports, per changed file, how many added lines that belong to coverable
statements are covered, and lists uncovered added lines.
"""
import subprocess, sys, re, collections

repo, rng, cover = sys.argv[1], sys.argv[2], sys.argv[3]
modprefix = sys.argv[4] if len(sys.argv) > 4 else "github.com/go-ble/ble"

EXCLUDE = ("examples/", "_test.go", "tools/codegen")

# 1. Added lines per file from git diff -U0
diff = subprocess.run(["git", "-C", repo, "diff", "-U0", rng],
                      capture_output=True, text=True).stdout
added = collections.defaultdict(set)  # file -> set of line numbers
cur = None
for line in diff.splitlines():
    m = re.match(r"\+\+\+ b/(.*)", line)
    if m:
        cur = m.group(1)
        continue
    m = re.match(r"@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@", line)
    if m and cur and cur.endswith(".go") and not any(x in cur for x in EXCLUDE):
        start = int(m.group(1))
        count = int(m.group(2)) if m.group(2) is not None else 1
        for ln in range(start, start + count):
            added[cur].add(ln)

# 2. Cover profile: file -> list of (start,end,count)
blocks = collections.defaultdict(list)
with open(cover) as f:
    for line in f:
        if line.startswith("mode:"):
            continue
        m = re.match(r"(.+?):(\d+)\.\d+,(\d+)\.\d+ (\d+) (\d+)", line)
        if not m:
            continue
        fn, s, e, stmts, cnt = m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4)), int(m.group(5))
        if fn.startswith(modprefix + "/"):
            fn = fn[len(modprefix) + 1:]
        blocks[fn].append((s, e, cnt))

# 3. For each added line, determine: coverable? covered?
tot_cov = tot_all = 0
report = []
for fn in sorted(added):
    fb = blocks.get(fn)
    if fb is None:
        report.append((fn, 0, 0, ["<file not in cover profile>"]))
        continue
    # line -> max count over blocks containing it
    cov_lines = {}
    for s, e, cnt in fb:
        for ln in range(s, e + 1):
            cov_lines[ln] = max(cov_lines.get(ln, 0), cnt)
    coverable = [ln for ln in added[fn] if ln in cov_lines]
    covered = [ln for ln in coverable if cov_lines[ln] > 0]
    uncovered = sorted(set(coverable) - set(covered))
    tot_all += len(coverable)
    tot_cov += len(covered)
    # compress uncovered into ranges
    ranges = []
    for ln in uncovered:
        if ranges and ln == ranges[-1][1] + 1:
            ranges[-1][1] = ln
        else:
            ranges.append([ln, ln])
    rs = ",".join(f"{a}" if a == b else f"{a}-{b}" for a, b in ranges)
    report.append((fn, len(covered), len(coverable), [rs] if rs else []))

for fn, cov, tot, unc in report:
    pct = f"{100*cov/tot:5.1f}%" if tot else "  n/a "
    print(f"{pct} {cov:4d}/{tot:<4d} {fn}" + (f"  UNCOVERED: {unc[0]}" if unc else ""))
print()
if tot_all:
    print(f"TOTAL diff coverage: {100*tot_cov/tot_all:.1f}% ({tot_cov}/{tot_all} added coverable lines)")
