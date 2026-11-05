#!/usr/bin/env python3
"""Print text snippets around a person's name, with EFTA document IDs. Read-only.

Usage: python person_snippets.py "amanda kirby" [--n 25] [--width 300] [--grep "director|vice president"]
Snippets are taken from the documents where the name appears, preferring ones that look like
email signatures or headers (title/company words nearby).
"""
import argparse, re, sqlite3
ap = argparse.ArgumentParser(); ap.add_argument("name"); ap.add_argument("--n", type=int, default=25)
ap.add_argument("--width", type=int, default=300); ap.add_argument("--grep", default="")
a = ap.parse_args()
c = sqlite3.connect("file:epstein_files/epstein.db?mode=ro", uri=True, timeout=120)
toks = a.name.lower().split()
pat = re.compile(r"\b" + r"[\s.,]+(?:[a-z]\.?\s+)?".join(map(re.escape, toks)) + r"\b", re.I)
sig = re.compile(r"director|president|managing|associate|analyst|officer|counsel|attorney|partner|assistant|"
                 r"manager|banker|advisor|adviser|secretary|ceo|cfo|coo|head of|vp\b|agent|accountant|"
                 r"professor|pilot|chef|deutsche|jpmorgan|morgan|bank|llc|llp|inc\.?|tel\b|fax|mobile|\+1", re.I)
extra = re.compile(a.grep, re.I) if a.grep else None
like = "%" + "%".join(toks) + "%"
rows = c.execute("SELECT f.filename, f.dataset, t.extracted_text FROM files f JOIN text_cache t ON t.file_id=f.id"
                 " WHERE t.extracted_text LIKE ? LIMIT 4000", (like,)).fetchall()
out = []
for fn, ds, text in rows:
    for m in pat.finditer(text):
        s = text[max(0, m.start() - a.width // 3): m.end() + a.width]
        score = len(sig.findall(s)) + (5 if extra and extra.search(s) else 0)
        out.append((score, fn, ds, " ".join(s.split())))
        break
out.sort(key=lambda x: -x[0])
print(f"{len(rows)} documents contain all of: {toks}")
for score, fn, ds, s in out[: a.n]:
    print(f"\n[{fn.replace('.pdf','')} ds{ds}] {s}")
