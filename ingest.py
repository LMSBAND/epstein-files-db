#!/usr/bin/env python3
"""Stream-ingest DOJ EFTA files: download one PDF, extract text, store it, delete the PDF, move on.

Walks the EFTA number space. Each document occupies one number per page, so after a hit at N
with P pages the next document starts at N+P. A miss (404) steps +1.

Nothing but text and metadata is kept. PDFs and OCR page images live in /dev/shm only while
being processed.

Usage:
    python ingest.py run [--workers 8] [--chunk 20000] [--start N] [--end N]
    python ingest.py status
Stop gracefully: touch epstein_files/STOP
"""
import argparse, hashlib, os, random, sqlite3, subprocess, sys, threading, time
from pathlib import Path
import requests

BASE = Path("./epstein_files")
DB = BASE / "epstein.db"
STOP = BASE / "STOP"
LOG = BASE / "ingest.log"
TMP = Path("/dev/shm/efta_ingest")
URL = "https://www.justice.gov/epstein/files/DataSet%20{d}/EFTA{n:08d}.pdf"

# Approximate dataset boundaries (first EFTA number of each set). Near a boundary both sets are tried.
BOUNDS = [(1, 1), (3159, 2), (3858, 3), (5705, 4), (8409, 5), (8529, 6), (9016, 7), (9665, 8),
          (39025, 9), (1262782, 10), (2208000, 11), (2730265, 12), (2732000, None)]
FUZZ = 15000
WALK_START, WALK_END = 9665, 2730264   # data sets 8-11; 1-7 and 12 are already complete

lock = threading.Lock()
pause_until = [0.0]


def log(msg):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def candidates(n):
    """Dataset dirs to try for EFTA number n, most likely first."""
    out = []
    for i, (start, d) in enumerate(BOUNDS[:-1]):
        end = BOUNDS[i + 1][0]
        if start <= n < end:
            out.insert(0, d)
        elif start - FUZZ <= n < end + FUZZ:
            out.append(d)
    return out


def db():
    c = sqlite3.connect(DB, timeout=120, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


def init(c):
    c.executescript("""
        CREATE TABLE IF NOT EXISTS ingest_chunks (
            chunk_start INTEGER PRIMARY KEY, chunk_end INTEGER, pos INTEGER, done INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS ingest_hits (
            efta INTEGER PRIMARY KEY, dataset INTEGER, pages INTEGER, bytes INTEGER,
            status TEXT, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
    """)
    c.commit()


def fetch(sess, n):
    """Return (dataset, path) for a hit, None for a miss. Retries/backs off on throttling."""
    for d in candidates(n):
        url = URL.format(d=d, n=n)
        for attempt in range(8):
            wait = pause_until[0] - time.time()
            if wait > 0:
                time.sleep(wait)
            time.sleep(random.uniform(0.2, 0.6))
            try:
                r = sess.get(url, timeout=120)
            except Exception as e:
                time.sleep(5 * (attempt + 1)); continue
            if r.status_code == 404:
                break
            if r.status_code in (403, 429, 500, 502, 503, 504):
                back = min(600, 30 * 2 ** attempt)
                with lock:
                    pause_until[0] = max(pause_until[0], time.time() + back)
                log(f"HTTP {r.status_code} at {n} (ds{d}); all workers pause {back}s")
                continue
            if r.status_code == 200 and r.content[:5] == b"%PDF-":
                TMP.mkdir(exist_ok=True)
                p = TMP / f"EFTA{n:08d}.pdf"
                p.write_bytes(r.content)
                return d, p
            log(f"unexpected {r.status_code} {r.headers.get('content-type')} at {n} (ds{d})")
            break
    return None


def pages_of(p):
    out = subprocess.run(["pdfinfo", str(p)], capture_output=True, text=True, timeout=60).stdout
    for line in out.splitlines():
        if line.startswith("Pages:"):
            return int(line.split()[1])
    return 1


def extract(p, pages):
    txt = subprocess.run(["pdftotext", "-q", str(p), "-"], capture_output=True, text=True,
                         timeout=300).stdout
    if len(txt.strip()) >= 40 * pages:
        return txt, "pdftotext", 0
    if pages > 60:
        return txt, "pdftotext", 1          # too long to OCR inline; flag it
    stem = TMP / p.stem
    try:
        subprocess.run(["pdftoppm", "-r", "150", "-gray", "-png", str(p), str(stem)],
                       capture_output=True, timeout=600)
        parts = []
        for img in sorted(TMP.glob(p.stem + "-*.png")):
            parts.append(subprocess.run(["tesseract", str(img), "-", "--psm", "3"],
                                        capture_output=True, text=True, timeout=300).stdout)
            img.unlink()
        ocr = "\n\n".join(parts)
        return (ocr, "tesseract", 0) if len(ocr.strip()) > len(txt.strip()) else (txt, "pdftotext", 1)
    finally:
        for img in TMP.glob(p.stem + "-*.png"):
            img.unlink()


def store(c, n, d, p, pages, known):
    data = p.read_bytes()
    fname = f"EFTA{n:08d}.pdf"
    if known:
        status = "known"
    else:
        txt, method, needs_ocr = extract(p, pages)
        with lock:
            cur = c.execute(
                "INSERT OR IGNORE INTO files (filename, dataset, rel_path, file_size, sha256, has_text, needs_ocr)"
                " VALUES (?,?,?,?,?,?,?)",
                (fname, d, f"downloads/DataSet{d}/{fname}", len(data), hashlib.sha256(data).hexdigest(),
                 int(bool(txt.strip())), needs_ocr))
            if cur.rowcount and txt.strip():
                c.execute("INSERT OR IGNORE INTO text_cache (file_id, extracted_text, char_count, method)"
                          " VALUES (?,?,?,?)", (cur.lastrowid, txt, len(txt), method))
        status = "new"
    with lock:
        c.execute("INSERT OR REPLACE INTO ingest_hits (efta, dataset, pages, bytes, status) VALUES (?,?,?,?,?)",
                  (n, d, pages, len(data), status))
    return status


def worker(wid, c, known, stats):
    sess = requests.Session()
    sess.headers["User-Agent"] = "epstein-files-db ingest (+https://github.com/LMSBAND/epstein-files-db)"
    sess.cookies.set("justiceGovAgeVerified", "true", domain=".justice.gov")
    while not STOP.exists():
        with lock:
            row = c.execute("SELECT chunk_start, chunk_end, pos FROM ingest_chunks WHERE done=0 AND chunk_start"
                            " NOT IN (%s) ORDER BY chunk_start LIMIT 1" % (",".join(map(str, stats["claimed"])) or "-1")
                            ).fetchone()
            if not row:
                return
            stats["claimed"].add(row[0])
        start, end, n = row
        while n < end and not STOP.exists():
            hit = fetch(sess, n)
            if hit is None:
                stats["miss"] += 1
                n += 1
            else:
                d, p = hit
                try:
                    pages = pages_of(p)
                    st = store(c, n, d, p, pages, f"EFTA{n:08d}.pdf" in known)
                    stats[st] += 1
                finally:
                    p.unlink(missing_ok=True)
                n += max(1, pages)
            with lock:
                c.execute("UPDATE ingest_chunks SET pos=? WHERE chunk_start=?", (n, start))
        with lock:
            if n >= end:
                c.execute("UPDATE ingest_chunks SET done=1 WHERE chunk_start=?", (start,))
            c.commit()
            stats["claimed"].discard(start)


def run(a):
    c = db(); init(c)
    s, e = a.start or WALK_START, a.end or WALK_END
    for cs in range(s, e + 1, a.chunk):
        c.execute("INSERT OR IGNORE INTO ingest_chunks (chunk_start, chunk_end, pos) VALUES (?,?,?)",
                  (cs, min(cs + a.chunk, e + 1), cs))
    c.commit()
    known = {r[0] for r in c.execute("SELECT filename FROM files")}
    stats = {"new": 0, "known": 0, "miss": 0, "claimed": set()}
    STOP.unlink(missing_ok=True)
    log(f"run start {s}-{e}, workers={a.workers}, known files={len(known)}")
    ts = [threading.Thread(target=worker, args=(i, c, known, stats), daemon=True) for i in range(a.workers)]
    for t in ts: t.start()
    t0 = time.time()
    while any(t.is_alive() for t in ts):
        time.sleep(60)
        with lock:
            c.commit()
        h = (time.time() - t0) / 3600
        log(f"new={stats['new']} known={stats['known']} miss={stats['miss']} ({stats['new']/max(h,1e-9):.0f} new/h)")
    with lock:
        c.commit()
    log(f"run end: new={stats['new']} known={stats['known']} miss={stats['miss']}")


def status(_):
    c = db(); init(c)
    tot, done = c.execute("SELECT count(*), sum(done) FROM ingest_chunks").fetchone()
    span = c.execute("SELECT sum(pos-chunk_start), sum(chunk_end-chunk_start) FROM ingest_chunks").fetchone()
    print(f"chunks {done or 0}/{tot}, numbers walked {span[0] or 0:,}/{span[1] or 0:,}")
    for row in c.execute("SELECT dataset, status, count(*), sum(pages), sum(bytes)/1e9 FROM ingest_hits"
                         " GROUP BY dataset, status"):
        print("ds%s %-5s files=%d pages=%s GB=%.2f" % row)
    for row in c.execute("SELECT dataset, count(*), sum(has_text), sum(needs_ocr) FROM files GROUP BY dataset"):
        print("db ds%s files=%d text=%s needs_ocr=%s" % row)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run"); r.add_argument("--workers", type=int, default=8)
    r.add_argument("--chunk", type=int, default=20000)
    r.add_argument("--start", type=int); r.add_argument("--end", type=int)
    sub.add_parser("status")
    a = ap.parse_args()
    {"run": run, "status": status}[a.cmd](a)
