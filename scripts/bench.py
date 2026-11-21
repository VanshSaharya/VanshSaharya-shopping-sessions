#!/usr/bin/env python3
"""Run the pipeline N times; report the minimum duration of every logged phase. Usage: bench.py DATA N [ENV=VAL ...]"""
import os, re, shutil, subprocess, sys
data, n = sys.argv[1], int(sys.argv[2])
env = dict(os.environ, DATA_DIR=data, OUTPUT_DIR="/tmp/bench_out", NCPU=os.environ.get("NCPU", "2"))
for kv in sys.argv[3:]:
    k, v = kv.split("=", 1); env[k] = v
best = {}
for _ in range(n):
    shutil.rmtree("/tmp/bench_out", ignore_errors=True)
    err = subprocess.run(["target/release/pipeline"], env=env, capture_output=True, text=True).stderr
    prev = 0.0
    for t, name in re.findall(r"\[\s*([\d.]+)s\] (\w+)", err):
        t = float(t); best[name] = min(best.get(name, 9e9), t - prev); prev = t
    best["TOTAL"] = min(best.get("TOTAL", 9e9), prev)
print("  ".join(f"{k}={v:.2f}" for k, v in best.items()))
