#!/usr/bin/env python3
"""Best-guess date for a document, from its text.

doc_date(text) -> (year, month or None, method) or None
  header : first "Sent:" / "Date:" line carrying a full date (emails)
  body   : most common year among full dates in the text; month = median month in that year
  year   : most common bare year (1985-2025) appearing at least twice

Run directly for coverage stats: /usr/bin/python3.12 doc_dates.py
"""
import re
from collections import Counter

MON = {m: i + 1 for i, m in enumerate("jan feb mar apr may jun jul aug sep oct nov dec".split())}
M = r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?"
FULL = re.compile(
    rf"\b{M}\s+(\d{{1,2}})(?:st|nd|rd|th)?,?\s+((?:19|20)\d\d)\b"      # March 4, 2014
    rf"|\b(\d{{1,2}})\s+{M},?\s+((?:19|20)\d\d)\b"                    # 4 March 2014
    r"|\b(\d{1,2})/(\d{1,2})/((?:19|20)\d\d)\b"                       # 03/04/2014
    r"|\b((?:19|20)\d\d)-(\d\d)-(\d\d)\b", re.I)                      # 2014-03-04
HEADER = re.compile(r"^\s*(?:sent|date)\s*:\s*(.{0,80})$", re.I | re.M)
YEAR = re.compile(r"(?<![\d/.-])(19[89]\d|20[0-2]\d)(?![\d/.-])")
LO, HI = 1985, 2025


def _parse(m):
    g = m.groups()
    if g[0]:   y, mo = int(g[2]), MON[g[0][:3].lower()]
    elif g[3]: y, mo = int(g[5]), MON[g[4][:3].lower()]
    elif g[6]: y, mo = int(g[8]), int(g[6])
    else:      y, mo = int(g[9]), int(g[10])
    return (y, mo) if LO <= y <= HI and 1 <= mo <= 12 else None


def doc_date(text):
    for h in HEADER.finditer(text[:6000]):
        m = FULL.search(h.group(1))
        if m and (d := _parse(m)):
            return d[0], d[1], "header"
    full = [d for m in FULL.finditer(text) if (d := _parse(m))]
    if full:
        y = Counter(d[0] for d in full).most_common(1)[0][0]
        ms = sorted(d[1] for d in full if d[0] == y)
        return y, ms[len(ms) // 2], "body"
    yrs = Counter(int(y) for y in YEAR.findall(text) if LO <= int(y) <= HI)
    if yrs:
        y, n = yrs.most_common(1)[0]
        if n >= 2:
            return y, None, "year"
    return None


PLEA = (2008, 6)   # Epstein's Florida guilty plea, June 30, 2008


def era(d):
    """'pre' / 'post' relative to the June 2008 plea, or None if undated or ambiguous."""
    if not d:
        return None
    y, mo, _ = d
    if y != PLEA[0]:
        return "pre" if y < PLEA[0] else "post"
    if mo is None:
        return None
    return "pre" if mo < PLEA[1] else "post" if mo > PLEA[1] else None


if __name__ == "__main__":
    import sqlite3
    c = sqlite3.connect("file:epstein_files/epstein.db?mode=ro", uri=True, timeout=120)
    meth, eras, years = Counter(), Counter(), Counter()
    for (text,) in c.execute("SELECT extracted_text FROM text_cache WHERE char_count > 0"):
        d = doc_date(text)
        meth[d[2] if d else "none"] += 1; eras[era(d)] += 1
        if d: years[d[0]] += 1
    print("method:", dict(meth)); print("era:", dict(eras))
    print("years:", " ".join(f"{y}:{n}" for y, n in sorted(years.items())))
