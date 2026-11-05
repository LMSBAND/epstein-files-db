#!/usr/bin/env python3
"""Read-only count of the viral "code word" terms across the DB, with sample contexts.

Safe to run while ingest.py is writing. Usage: python viral_terms.py [--samples 5]
"""
import argparse, re, sqlite3, time
from collections import defaultdict
from pathlib import Path
from epstein_processor import VIRAL_TERMS

ap = argparse.ArgumentParser(); ap.add_argument("--samples", type=int, default=5); a = ap.parse_args()
c = sqlite3.connect("file:epstein_files/epstein.db?mode=ro", uri=True, timeout=120)
pats = {t: re.compile(r"\b" + re.escape(t) + r"\b", re.I) for t in VIRAL_TERMS}
hits, files, by_ds, samples = defaultdict(int), defaultdict(int), defaultdict(lambda: defaultdict(int)), defaultdict(list)
n = 0
for fname, ds, text in c.execute("SELECT f.filename, f.dataset, t.extracted_text FROM files f"
                                 " JOIN text_cache t ON t.file_id=f.id WHERE t.char_count>0"):
    n += 1
    for t, p in pats.items():
        m = p.findall(text)
        if m:
            hits[t] += len(m); files[t] += 1; by_ds[t][ds] += 1
            if len(samples[t]) < a.samples:
                x = p.search(text)
                samples[t].append((fname, ds, " ".join(text[max(0, x.start()-140):x.end()+140].split())))
out = Path("epstein_files/output"); out.mkdir(parents=True, exist_ok=True)
rp = out / f"viral_terms_{time.strftime('%Y%m%d_%H%M%S')}.txt"
with open(rp, "w") as f:
    f.write(f"Viral term counts over {n:,} files with text\n\n{'term':<28}{'matches':>9}{'files':>8}  files by dataset\n")
    for t in sorted(VIRAL_TERMS, key=lambda t: -hits[t]):
        f.write(f"{t:<28}{hits[t]:>9,}{files[t]:>8,}  {dict(sorted(by_ds[t].items()))}\n")
    for t in VIRAL_TERMS:
        f.write(f"\n=== {t} ===\n")
        for fname, ds, ctx in samples[t]:
            f.write(f"  [ds{ds}] {fname}: ...{ctx}...\n")
print(open(rp).read().split("\n=== ")[0]); print(rp)
