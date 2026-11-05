#!/usr/bin/env python3
"""OCR the long documents ingest.py skipped (>60 pages, flagged needs_ocr=1, never OCR'd).

Re-downloads each PDF into /dev/shm, OCRs pages in parallel, replaces the stored text if OCR
found more, clears needs_ocr, deletes the PDF. Safe to run alongside ingest.py.

Usage: /usr/bin/python3.12 ocr_backlog.py [--procs 6] [--min-pages 61]
"""
import argparse, sqlite3, subprocess, time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import requests

ap = argparse.ArgumentParser()
ap.add_argument("--procs", type=int, default=6)
ap.add_argument("--min-pages", type=int, default=61)
a = ap.parse_args()

DB = Path("epstein_files/epstein.db")
TMP = Path("/dev/shm/efta_ocr_backlog"); TMP.mkdir(exist_ok=True)
URL = "https://www.justice.gov/epstein/files/DataSet%20{d}/{f}"


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def ocr_page(img):
    out = subprocess.run(["tesseract", str(img), "-", "--psm", "3"], capture_output=True, text=True,
                         timeout=300).stdout
    img.unlink()
    return out


c = sqlite3.connect(DB, timeout=300)
rows = c.execute(
    "SELECT f.id, f.filename, f.dataset, h.pages, t.char_count FROM files f"
    " JOIN ingest_hits h ON h.efta=cast(substr(f.filename,5,8) as int)"
    " LEFT JOIN text_cache t ON t.file_id=f.id"
    " WHERE f.needs_ocr=1 AND h.pages>=? ORDER BY h.pages", (a.min_pages,)).fetchall()
log(f"{len(rows)} documents, {sum(r[3] for r in rows)} pages")

s = requests.Session()
s.headers["User-Agent"] = "epstein-files-db ocr (+https://github.com/LMSBAND/epstein-files-db)"
s.cookies.set("justiceGovAgeVerified", "true", domain=".justice.gov")

for fid, fname, ds, pages, old_chars in rows:
    t0 = time.time()
    r = s.get(URL.format(d=ds, f=fname), timeout=300)
    if r.status_code != 200 or r.content[:5] != b"%PDF-":
        log(f"{fname}: HTTP {r.status_code}, skipped"); continue
    pdf = TMP / fname; pdf.write_bytes(r.content)
    try:
        subprocess.run(["pdftoppm", "-r", "150", "-gray", "-png", str(pdf), str(TMP / pdf.stem)],
                       capture_output=True, timeout=3600)
        imgs = sorted(TMP.glob(pdf.stem + "-*.png"))
        with ThreadPoolExecutor(a.procs) as ex:
            text = "\n\n".join(ex.map(ocr_page, imgs))
    finally:
        pdf.unlink(missing_ok=True)
        for img in TMP.glob(pdf.stem + "-*.png"):
            img.unlink()
    n = len(text.strip())
    if n > (old_chars or 0):
        c.execute("INSERT INTO text_cache (file_id, extracted_text, char_count, method) VALUES (?,?,?,?)"
                  " ON CONFLICT(file_id) DO UPDATE SET extracted_text=excluded.extracted_text,"
                  " char_count=excluded.char_count, method=excluded.method", (fid, text, len(text), "tesseract"))
        c.execute("UPDATE files SET has_text=1, needs_ocr=0 WHERE id=?", (fid,))
        verdict = f"stored {old_chars or 0:,} -> {len(text):,} chars"
    else:
        c.execute("UPDATE files SET needs_ocr=0 WHERE id=?", (fid,))
        verdict = f"OCR found less ({n:,} vs {old_chars:,}), kept original"
    c.commit()
    log(f"{fname} ({pages}p, {time.time()-t0:.0f}s): {verdict}")
log("done")
