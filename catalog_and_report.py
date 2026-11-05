#!/usr/bin/env python3
"""
Catalog all Epstein files into SQLite DB and run keyword reports.

Usage:
    python catalog_and_report.py catalog    # Scan all dirs, populate DB
    python catalog_and_report.py report     # Extract text + keyword search
    python catalog_and_report.py status     # Show DB stats
"""

import os
import sys
import re
import json
import hashlib
import sqlite3
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import concurrent.futures
import signal

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

try:
    from pypdf import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

BASE_DIR = Path("./epstein_files")
DB_PATH = BASE_DIR / "epstein.db"
OUTPUT_DIR = BASE_DIR / "output"

# All locations where PDFs live
SCAN_DIRS = [
    # Extracted zips (DS 1-7, 12)
    (BASE_DIR / "extracted", "extracted"),
    # Bruteforced downloads (DS 8-11)
    (BASE_DIR / "downloads", "downloads"),
]

# Dataset detection from path
def detect_dataset(filepath: Path) -> int:
    """Detect dataset number from file path."""
    s = str(filepath)
    for pattern in [r'DataSet\s*(\d+)', r'DataSet(\d+)']:
        m = re.search(pattern, s)
        if m:
            return int(m.group(1))
    return 0


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(conn):
    """Ensure tables exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            dataset INTEGER,
            rel_path TEXT UNIQUE,
            file_size INTEGER,
            sha256 TEXT,
            has_text INTEGER DEFAULT 0,
            needs_ocr INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS text_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER REFERENCES files(id),
            extracted_text TEXT,
            char_count INTEGER,
            method TEXT
        );
        CREATE TABLE IF NOT EXISTS search_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER REFERENCES files(id),
            keyword TEXT,
            match_count INTEGER,
            context TEXT
        );
        CREATE TABLE IF NOT EXISTS production_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            dataset INTEGER,
            rel_path TEXT UNIQUE,
            file_size INTEGER,
            file_type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_files_filename ON files(filename);
        CREATE INDEX IF NOT EXISTS idx_files_dataset ON files(dataset);
        CREATE INDEX IF NOT EXISTS idx_text_cache_file_id ON text_cache(file_id);
        CREATE INDEX IF NOT EXISTS idx_search_results_keyword ON search_results(keyword);
    """)
    conn.commit()


def catalog(conn):
    """Scan all directories and catalog files into DB."""
    init_db(conn)

    print("\n=== CATALOGING FILES ===\n")

    pdf_count = 0
    prod_count = 0
    skipped = 0
    new_files = 0

    for scan_dir, label in SCAN_DIRS:
        if not scan_dir.exists():
            continue

        # Find all PDFs
        print(f"Scanning {scan_dir}...")
        for pdf_path in scan_dir.rglob("*.pdf"):
            pdf_count += 1
            rel_path = str(pdf_path.relative_to(BASE_DIR))
            filename = pdf_path.name
            dataset = detect_dataset(pdf_path)

            # Skip suffix dupes (EFTA*-1.pdf etc)
            if re.match(r'EFTA\d+-\d+\.pdf', filename):
                skipped += 1
                continue

            # Check if already in DB
            existing = conn.execute(
                "SELECT id FROM files WHERE rel_path = ?", (rel_path,)
            ).fetchone()
            if existing:
                continue

            file_size = pdf_path.stat().st_size

            conn.execute(
                "INSERT INTO files (filename, dataset, rel_path, file_size) VALUES (?, ?, ?, ?)",
                (filename, dataset, rel_path, file_size)
            )
            new_files += 1

            if new_files % 5000 == 0:
                conn.commit()
                print(f"  Cataloged {new_files} new files...")

        # Find production files (TIF, JPG, WAV, MP4)
        for ext in ['*.tif', '*.jpg', '*.WAV', '*.MP4', '*.wav', '*.mp4']:
            for f in scan_dir.rglob(ext):
                rel_path = str(f.relative_to(BASE_DIR))
                filename = f.name
                dataset = detect_dataset(f)
                file_type = f.suffix.lower().lstrip('.')

                existing = conn.execute(
                    "SELECT id FROM production_files WHERE rel_path = ?", (rel_path,)
                ).fetchone()
                if existing:
                    continue

                file_size = f.stat().st_size
                conn.execute(
                    "INSERT INTO production_files (filename, dataset, rel_path, file_size, file_type) VALUES (?, ?, ?, ?, ?)",
                    (filename, dataset, rel_path, file_size, file_type)
                )
                prod_count += 1

    conn.commit()

    print(f"\n  New PDFs cataloged: {new_files}")
    print(f"  Production files cataloged: {prod_count}")
    print(f"  Suffix dupes skipped: {skipped}")

    # Print summary
    show_status(conn)


class _Timeout(Exception):
    pass

def _timeout_handler(signum, frame):
    raise _Timeout()

def extract_text_from_pdf(pdf_path: Path, timeout_sec: int = 30) -> tuple:
    """Extract text, return (text, method). Skips after timeout_sec."""
    text = ""
    method = ""

    old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(timeout_sec)

    try:
        if HAS_PDFPLUMBER:
            try:
                with pdfplumber.open(pdf_path) as pdf:
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text += page_text + "\n\n"
                if text:
                    method = "pdfplumber"
            except _Timeout:
                raise
            except Exception:
                pass

        if not text and HAS_PYPDF:
            try:
                reader = PdfReader(pdf_path)
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n\n"
                if text:
                    method = "pypdf"
            except _Timeout:
                raise
            except Exception:
                pass
    except _Timeout:
        text = ""
        method = "timeout"
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)

    return text, method


def run_text_extraction(conn, max_workers=4):
    """Extract text from all PDFs not yet in text_cache."""
    print("\n=== EXTRACTING TEXT ===\n")

    # Find files without text
    rows = conn.execute("""
        SELECT f.id, f.rel_path FROM files f
        LEFT JOIN text_cache tc ON tc.file_id = f.id
        WHERE tc.id IS NULL
        ORDER BY f.dataset, f.filename
    """).fetchall()

    print(f"  Files needing text extraction: {len(rows)}")
    if not rows:
        print("  All files already have text extracted.")
        return

    extracted = 0
    no_text = 0
    errors = 0

    for file_id, rel_path in rows:
        pdf_path = BASE_DIR / rel_path
        if not pdf_path.exists():
            errors += 1
            continue

        try:
            text, method = extract_text_from_pdf(pdf_path)

            if method == "timeout":
                conn.execute("UPDATE files SET needs_ocr = 1 WHERE id = ?", (file_id,))
                no_text += 1
            elif text:
                conn.execute(
                    "INSERT INTO text_cache (file_id, extracted_text, char_count, method) VALUES (?, ?, ?, ?)",
                    (file_id, text, len(text), method)
                )
                conn.execute("UPDATE files SET has_text = 1 WHERE id = ?", (file_id,))
                extracted += 1
            else:
                conn.execute("UPDATE files SET needs_ocr = 1 WHERE id = ?", (file_id,))
                no_text += 1
        except Exception:
            errors += 1

        total = extracted + no_text + errors
        if total % 5 == 0:
            conn.commit()
            print(f"  Progress: {total}/{len(rows)} ({extracted} text, {no_text} needs OCR, {errors} errors)")

    conn.commit()
    print(f"\n  Text extracted: {extracted}")
    print(f"  Needs OCR: {no_text}")
    print(f"  Errors: {errors}")


# Import keywords from epstein_processor
sys.path.insert(0, str(Path(__file__).parent))
try:
    from epstein_processor import DEFAULT_KEYWORDS
except ImportError:
    DEFAULT_KEYWORDS = ["Epstein", "Maxwell", "Trump", "Clinton", "Prince Andrew"]


def run_keyword_search(conn, keywords=None):
    """Search all extracted text for keywords."""
    if keywords is None:
        keywords = DEFAULT_KEYWORDS

    print(f"\n=== KEYWORD SEARCH ({len(keywords)} keywords) ===\n")

    # Clear old results
    conn.execute("DELETE FROM search_results")
    conn.commit()

    # Get all text
    rows = conn.execute("""
        SELECT f.id, f.filename, f.rel_path, tc.extracted_text
        FROM files f
        JOIN text_cache tc ON tc.file_id = f.id
        WHERE tc.char_count > 0
    """).fetchall()

    print(f"  Searching {len(rows)} files with text...\n")

    # Compile patterns
    patterns = {kw: re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE) for kw in keywords}

    total_hits = 0
    files_with_hits = set()
    keyword_counts = defaultdict(int)

    for i, (file_id, filename, rel_path, text) in enumerate(rows):
        if (i + 1) % 1000 == 0:
            print(f"  Processed {i+1}/{len(rows)} files...")

        for kw, pattern in patterns.items():
            matches = list(pattern.finditer(text))
            if matches:
                # Get first match context
                m = matches[0]
                start = max(0, m.start() - 150)
                end = min(len(text), m.end() + 150)
                context = ' '.join(text[start:end].split())

                conn.execute(
                    "INSERT INTO search_results (file_id, keyword, match_count, context) VALUES (?, ?, ?, ?)",
                    (file_id, kw, len(matches), context)
                )
                total_hits += len(matches)
                files_with_hits.add(file_id)
                keyword_counts[kw] += len(matches)

        if (i + 1) % 5000 == 0:
            conn.commit()

    conn.commit()

    # Generate report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUTPUT_DIR / f"keyword_report_{timestamp}.txt"

    with open(report_path, 'w') as f:
        f.write("EPSTEIN FILES KEYWORD SEARCH REPORT\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write(f"Files searched: {len(rows)}\n")
        f.write(f"Files with hits: {len(files_with_hits)}\n")
        f.write(f"Total keyword matches: {total_hits}\n")
        f.write("=" * 70 + "\n\n")

        # Sort keywords by hit count
        for kw, count in sorted(keyword_counts.items(), key=lambda x: -x[1]):
            f.write(f"\n{'='*70}\n")
            f.write(f"{kw}: {count} matches\n")
            f.write(f"{'='*70}\n")

            # Get file details for this keyword
            file_hits = conn.execute("""
                SELECT f.filename, f.dataset, sr.match_count, sr.context
                FROM search_results sr
                JOIN files f ON f.id = sr.file_id
                WHERE sr.keyword = ?
                ORDER BY sr.match_count DESC
                LIMIT 20
            """, (kw,)).fetchall()

            for fname, ds, mc, ctx in file_hits:
                f.write(f"\n  [{ds}] {fname} ({mc} hits)\n")
                f.write(f"    ...{ctx}...\n")

        # Keywords with zero hits
        zero_kws = [kw for kw in keywords if kw not in keyword_counts]
        if zero_kws:
            f.write(f"\n\n{'='*70}\n")
            f.write(f"KEYWORDS WITH ZERO HITS ({len(zero_kws)}):\n")
            f.write(f"{'='*70}\n")
            for kw in zero_kws:
                f.write(f"  - {kw}\n")

    print(f"\n  Total matches: {total_hits}")
    print(f"  Files with hits: {len(files_with_hits)}")
    print(f"  Keywords with hits: {len(keyword_counts)}/{len(keywords)}")
    print(f"\n  Report: {report_path}")

    # Print top 20 keywords
    print(f"\n  TOP KEYWORDS:")
    for kw, count in sorted(keyword_counts.items(), key=lambda x: -x[1])[:20]:
        print(f"    {count:6d}  {kw}")


def show_status(conn):
    """Show database stats."""
    print("\n=== DATABASE STATUS ===\n")

    rows = conn.execute("""
        SELECT dataset, COUNT(*), SUM(file_size), SUM(has_text), SUM(needs_ocr)
        FROM files GROUP BY dataset ORDER BY dataset
    """).fetchall()

    total_files = 0
    total_size = 0
    total_text = 0
    total_ocr = 0

    print(f"  {'DS':>4} {'Files':>8} {'Size':>10} {'HasText':>8} {'NeedOCR':>8}")
    print(f"  {'-'*4} {'-'*8} {'-'*10} {'-'*8} {'-'*8}")
    for ds, count, size, has_text, needs_ocr in rows:
        size = size or 0
        has_text = has_text or 0
        needs_ocr = needs_ocr or 0
        print(f"  {ds:>4} {count:>8,} {size/1024/1024:>9.1f}M {has_text:>8,} {needs_ocr:>8,}")
        total_files += count
        total_size += size
        total_text += has_text
        total_ocr += needs_ocr

    print(f"  {'-'*4} {'-'*8} {'-'*10} {'-'*8} {'-'*8}")
    print(f"  {'ALL':>4} {total_files:>8,} {total_size/1024/1024:>9.1f}M {total_text:>8,} {total_ocr:>8,}")

    # Production files
    prod = conn.execute("""
        SELECT file_type, COUNT(*), SUM(file_size)
        FROM production_files GROUP BY file_type ORDER BY COUNT(*) DESC
    """).fetchall()
    if prod:
        print(f"\n  Production files:")
        for ftype, count, size in prod:
            size = size or 0
            print(f"    {ftype}: {count:,} files ({size/1024/1024:.1f}M)")

    # Text cache stats
    tc = conn.execute("SELECT COUNT(*), SUM(char_count) FROM text_cache").fetchone()
    print(f"\n  Text cache: {tc[0]:,} files, {(tc[1] or 0)/1024/1024:.1f}M chars")

    # Search results
    sr = conn.execute("SELECT COUNT(DISTINCT keyword), COUNT(*), SUM(match_count) FROM search_results").fetchone()
    print(f"  Search results: {sr[0]} keywords, {sr[1]:,} file hits, {sr[2] or 0:,} total matches")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    conn = get_db()
    init_db(conn)
    command = sys.argv[1].lower()

    if command == "catalog":
        catalog(conn)
    elif command == "report":
        catalog(conn)  # Always re-catalog first
        run_text_extraction(conn)
        run_keyword_search(conn)
    elif command == "extract":
        catalog(conn)
        run_text_extraction(conn)
    elif command == "search":
        if len(sys.argv) > 2:
            keywords = sys.argv[2:]
            run_keyword_search(conn, keywords)
        else:
            run_keyword_search(conn)
    elif command == "status":
        show_status(conn)
    else:
        print(f"Unknown command: {command}")
        print(__doc__)

    conn.close()


if __name__ == "__main__":
    main()
