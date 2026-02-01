#!/usr/bin/env python3
"""
Epstein Files Processor
Extracts and searches through DOJ Epstein file releases.

Download the zip files manually from https://www.justice.gov/epstein
and place them in ./epstein_files/downloads/ before running.

Usage:
    python epstein_processor.py status             # Check what's downloaded
    python epstein_processor.py extract            # Extract all zips
    python epstein_processor.py search "Bill Gates"  # Search for a term
    python epstein_processor.py search --file keywords.txt  # Search from file
    python epstein_processor.py report             # Generate full report with all keywords
"""

import os
import sys
import re
import json
import zipfile
import shutil
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import concurrent.futures

# Try to import PDF libraries
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

try:
    import pytesseract
    from pdf2image import convert_from_path
    HAS_OCR = True
except ImportError:
    HAS_OCR = False


# Configuration
BASE_DIR = Path("./epstein_files")
DOWNLOAD_DIR = BASE_DIR / "downloads"
EXTRACT_DIR = BASE_DIR / "extracted"
OUTPUT_DIR = BASE_DIR / "output"
TEXT_CACHE_DIR = BASE_DIR / "text_cache"

# Default search keywords
DEFAULT_KEYWORDS = [
    # === POLITICIANS / PUBLIC FIGURES ===
    "Bill Clinton",
    "Hillary Clinton",
    "Clinton Foundation",
    "Donald Trump",
    "Prince Andrew",
    "Duke of York",
    "Dershowitz",
    "Alan Dershowitz",
    "Kevin Spacey",
    "Woody Allen",
    "Chris Tucker",
    "Naomi Campbell",
    "Tony Blair",
    "Ehud Barak",
    "Bill Richardson",
    "George Mitchell",
    "John Glenn",
    "Henry Kissinger",
    "John Kerry",
    "Rupert Murdoch",
    "Michael Jackson",
    "Mick Jagger",
    "George Bush",
    "George W. Bush",
    "George H.W. Bush",
    "Jeb Bush",
    "Joe Biden",
    "Biden",
    "Lindsey Graham",

    # === SILICON VALLEY / TECH ===
    "Bill Gates",
    "Melinda Gates",
    "Gates Foundation",
    "Elon Musk",
    "Reid Hoffman",
    "LinkedIn",
    "Sergey Brin",
    "Larry Page",
    "Eric Schmidt",
    "Peter Thiel",
    "PayPal",
    "Nathan Myhrvold",
    "Bill Joy",
    "Sheryl Sandberg",
    "Mark Zuckerberg",
    "Jeff Bezos",
    "Steve Jobs",
    "Carl Icahn",
    "Howard Lutnick",
    "Ron Burkle",
    "Linus Torvalds",  # hope not
    "Jaron Lanier",    # hope not  
    "Joscha Bach",     # hope not
    
    # === ACADEMIA / SCIENTISTS ===
    "Marvin Minsky",
    "Joi Ito",
    "Joichi Ito",      # full name
    "Nicholas Negroponte",
    "Larry Summers",
    "Lawrence Summers",
    "Stephen Hawking",
    "Noam Chomsky",
    "Lawrence Krauss",
    "George Church",
    "Danny Hillis",
    "W. Daniel Hillis",
    "Stewart Brand",
    "Kevin Kelly",
    "Steven Pinker",
    "Martin Nowak",
    "Leon Botstein",
    "Robert Trivers",
    "John Brockman",
    "Malcolm Gladwell",
    "Itzhak Perlman",
    "Deepak Chopra",
    "Peter Attia",
    
    # === KNOWN ALIASES / NICKNAMES ===
    "Voldemort",       # MIT Media Lab codename for Epstein
    "JE",              # common abbreviation in documents
    "Bear",            # Epstein's family nickname
    "Lolita Express",  # plane nickname
    
    # === INSTITUTIONS ===
    "Stanford",
    "MIT",
    "Media Lab",
    "M.I.T.",
    "Harvard",
    "Caltech",
    "Santa Fe Institute",
    "Edge Foundation",
    "Edge.org",
    "TED",
    "World Economic Forum",
    "Davos",
    "Interlochen",
    "Dalton School",
    
    # === VENTURE CAPITAL / FINANCE ===
    "Sequoia",
    "Andreessen",
    "Y Combinator",
    "Kleiner Perkins",
    "Wexner",
    "Les Wexner",
    "Leslie Wexner",
    "Leon Black",
    "Apollo",
    "Glenn Dubin",
    "Eva Dubin",
    "Eva Andersson",
    "Jes Staley",
    "Deutsche Bank",
    "JP Morgan",
    "JPMorgan",
    "David Koch",
    "Adnan Khashoggi",
    
    # === EPSTEIN INNER CIRCLE ===
    "Ghislaine Maxwell",
    "Ghislaine",
    "Maxwell",
    "GM",              # abbreviation
    "Jean-Luc Brunel",
    "Brunel",
    "MC2",             # Brunel's modeling agency
    "Sarah Kellen",
    "Kellen",
    "Nadia Marcinkova",
    "Nadia Marcinko",
    "Lesley Groff",
    "Leslie Groff",
    "Adriana Ross",
    "Alfredo Rodriguez",
    "Juan Alessi",
    "David Rodgers",   # pilot
    "Lawrence Visoski", # pilot
    
    # === LOCATIONS ===
    "Little St. James",
    "Little Saint James",
    "LSJ",
    "Zorro Ranch",
    "Stanley Ranch",
    "Palm Beach",
    "71st Street",
    "East 71st",
    "9 East 71st",
    "Paris apartment",
    "New Mexico",
    "Virgin Islands",
    "USVI",
    "Teterboro",
    "301 East 66th",
    
    # === ACTIONS / EVENTS (the dark stuff) ===
    "party",
    "parties",
    "massage",
    "masseuse",
    "flight",
    "flight log",
    "manifest",
    "recruit",
    "recruitment",
    "minor",
    "underage",
    "rape",
    "assault",
    "abuse",
    "victim",
    "trafficking",
    "sex trafficking",
    "young girl",
    "young woman",
    "teenager",
    "high school",
    "14 year",
    "15 year",
    "16 year",
    "17 year",
    "modeling",
    "model agency",
    "calendar girl",
    "blackmail",
    "videotape",
    "surveillance",
    "hidden camera",
    
    # === MONEY / DONATIONS ===
    "payment",
    "wire transfer",
    "donation",
    "foundation",
    "grant",
    "funding",
    "contribution",
    "anonymous donation",
    "directed gift",
]


def setup_directories():
    """Create necessary directories."""
    for d in [DOWNLOAD_DIR, EXTRACT_DIR, OUTPUT_DIR, TEXT_CACHE_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def extract_all():
    """Extract all downloaded zip files."""
    setup_directories()
    print("\n=== EXTRACTING FILES ===\n")

    zip_files = list(DOWNLOAD_DIR.glob("*.zip"))
    if not zip_files:
        print("No zip files found.")
        print(f"Download zips from https://www.justice.gov/epstein")
        print(f"and place them in: {DOWNLOAD_DIR.absolute()}")
        return
    
    for zip_path in zip_files:
        extract_to = EXTRACT_DIR / zip_path.stem
        if extract_to.exists() and any(extract_to.iterdir()):
            print(f"  [SKIP] {zip_path.name} already extracted")
            continue
        
        print(f"  [EXTRACTING] {zip_path.name}...")
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(extract_to)
            print(f"    Extracted to {extract_to}")
        except Exception as e:
            print(f"    [ERROR] Failed: {e}")
    
    # Copy non-zip files
    for f in DOWNLOAD_DIR.glob("*.pdf"):
        dest = EXTRACT_DIR / f.name
        if not dest.exists():
            shutil.copy(f, dest)
    
    print("\n=== EXTRACTION COMPLETE ===")
    
    # Count files
    pdf_count = len(list(EXTRACT_DIR.rglob("*.pdf")))
    print(f"Total PDFs found: {pdf_count}")


def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract text from a PDF file, with caching."""
    # Check cache first
    cache_file = TEXT_CACHE_DIR / f"{pdf_path.stem}_{hash(str(pdf_path))}.txt"
    if cache_file.exists():
        return cache_file.read_text(encoding='utf-8', errors='ignore')
    
    text = ""
    
    # Try pdfplumber first (best for text extraction)
    if HAS_PDFPLUMBER:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n\n"
        except Exception as e:
            pass
    
    # Fall back to pypdf
    if not text and HAS_PYPDF:
        try:
            reader = PdfReader(pdf_path)
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n\n"
        except Exception as e:
            pass
    
    # If still no text and we have OCR, try that (slow but works on scanned docs)
    if not text and HAS_OCR:
        try:
            images = convert_from_path(pdf_path, dpi=150)
            for i, image in enumerate(images):
                page_text = pytesseract.image_to_string(image)
                if page_text:
                    text += f"[Page {i+1}]\n{page_text}\n\n"
        except Exception as e:
            pass
    
    # Cache the result
    if text:
        cache_file.write_text(text, encoding='utf-8')
    
    return text


def search_text(text: str, keyword: str, context_chars: int = 200) -> list:
    """Search for keyword in text, return matches with context."""
    matches = []
    pattern = re.compile(re.escape(keyword), re.IGNORECASE)
    
    for match in pattern.finditer(text):
        start = max(0, match.start() - context_chars)
        end = min(len(text), match.end() + context_chars)
        
        # Get surrounding context
        context = text[start:end]
        
        # Clean up context (remove excessive whitespace)
        context = ' '.join(context.split())
        
        # Highlight the match
        highlighted = pattern.sub(f"**{match.group()}**", context)
        
        matches.append({
            'position': match.start(),
            'context': highlighted,
            'raw_match': match.group()
        })
    
    return matches


def search_files(keyword: str, max_workers: int = 4):
    """Search all extracted files for a keyword."""
    setup_directories()
    
    print(f"\n=== SEARCHING FOR: '{keyword}' ===\n")
    
    pdf_files = list(EXTRACT_DIR.rglob("*.pdf"))
    if not pdf_files:
        print("No PDF files found. Run 'extract' first.")
        return []
    
    print(f"Searching {len(pdf_files)} PDF files...")
    
    results = []
    processed = 0
    
    for pdf_path in pdf_files:
        processed += 1
        if processed % 100 == 0:
            print(f"  Processed {processed}/{len(pdf_files)} files...")
        
        try:
            text = extract_text_from_pdf(pdf_path)
            if not text:
                continue
            
            matches = search_text(text, keyword)
            if matches:
                results.append({
                    'file': str(pdf_path.relative_to(EXTRACT_DIR)),
                    'matches': matches,
                    'match_count': len(matches)
                })
        except Exception as e:
            continue
    
    # Sort by number of matches
    results.sort(key=lambda x: x['match_count'], reverse=True)
    
    # Print results
    total_matches = sum(r['match_count'] for r in results)
    print(f"\n=== RESULTS ===")
    print(f"Found {total_matches} matches in {len(results)} files\n")
    
    for r in results[:20]:  # Show top 20 files
        print(f"\n📄 {r['file']} ({r['match_count']} matches)")
        print("-" * 60)
        for m in r['matches'][:3]:  # Show first 3 matches per file
            print(f"  ...{m['context']}...")
        if r['match_count'] > 3:
            print(f"  ... and {r['match_count'] - 3} more matches")
    
    if len(results) > 20:
        print(f"\n... and {len(results) - 20} more files with matches")
    
    return results


def generate_report(keywords: list = None):
    """Generate a comprehensive report searching for all keywords.

    Reads each file once and searches all keywords against it (single-pass).
    """
    setup_directories()

    if keywords is None:
        keywords = DEFAULT_KEYWORDS

    print(f"\n=== GENERATING COMPREHENSIVE REPORT ===")
    print(f"Searching for {len(keywords)} keywords...\n")

    pdf_files = list(EXTRACT_DIR.rglob("*.pdf"))
    if not pdf_files:
        print("No PDF files found. Run 'extract' first.")
        return

    print(f"Scanning {len(pdf_files)} files for {len(keywords)} keywords...\n")

    # Compile all patterns once
    patterns = {}
    for kw in keywords:
        patterns[kw] = re.compile(re.escape(kw), re.IGNORECASE)

    # keyword -> list of {file, matches, match_count}
    all_results = defaultdict(list)

    for i, pdf_path in enumerate(pdf_files):
        if (i + 1) % 200 == 0:
            print(f"  Processed {i+1}/{len(pdf_files)} files...")

        try:
            text = extract_text_from_pdf(pdf_path)
            if not text:
                continue

            rel_path = str(pdf_path.relative_to(EXTRACT_DIR))

            for kw, pattern in patterns.items():
                matches = search_text(text, kw)
                if matches:
                    all_results[kw].append({
                        'file': rel_path,
                        'matches': matches,
                        'match_count': len(matches)
                    })
        except Exception:
            continue

    # Convert to summary format
    summary_results = {}
    for kw, file_results in all_results.items():
        file_results.sort(key=lambda x: x['match_count'], reverse=True)
        summary_results[kw] = {
            'total_matches': sum(r['match_count'] for r in file_results),
            'files_with_matches': len(file_results),
            'top_files': [r['file'] for r in file_results[:5]]
        }

    all_results = summary_results
    
    # Generate report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = OUTPUT_DIR / f"epstein_report_{timestamp}.json"
    
    report = {
        'generated': timestamp,
        'total_keywords': len(keywords),
        'keywords_with_hits': len(all_results),
        'results': all_results
    }
    
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    # Also generate human-readable summary
    summary_path = OUTPUT_DIR / f"epstein_summary_{timestamp}.txt"
    with open(summary_path, 'w') as f:
        f.write("EPSTEIN FILES SEARCH REPORT\n")
        f.write(f"Generated: {timestamp}\n")
        f.write("=" * 60 + "\n\n")
        
        # Sort by total matches
        sorted_results = sorted(
            all_results.items(),
            key=lambda x: x[1]['total_matches'],
            reverse=True
        )
        
        for keyword, data in sorted_results:
            f.write(f"\n{keyword}\n")
            f.write(f"  Total matches: {data['total_matches']}\n")
            f.write(f"  Files with matches: {data['files_with_matches']}\n")
            f.write(f"  Top files:\n")
            for file in data['top_files']:
                f.write(f"    - {file}\n")
    
    print(f"\n=== REPORT COMPLETE ===")
    print(f"JSON report: {report_path}")
    print(f"Summary: {summary_path}")
    
    return report


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return
    
    command = sys.argv[1].lower()
    
    if command == "extract":
        extract_all()
    
    elif command == "search":
        if len(sys.argv) < 3:
            print("Usage: python epstein_processor.py search <keyword>")
            print("       python epstein_processor.py search --file keywords.txt")
            return
        
        if sys.argv[2] == "--file":
            if len(sys.argv) < 4:
                print("Please provide a keywords file")
                return
            keywords_file = Path(sys.argv[3])
            keywords = keywords_file.read_text().strip().split('\n')
            for kw in keywords:
                search_files(kw.strip())
        else:
            keyword = ' '.join(sys.argv[2:])
            search_files(keyword)
    
    elif command == "report":
        generate_report()
    
    elif command == "status":
        setup_directories()
        print("\n=== STATUS ===\n")
        
        # Check downloads
        zips = list(DOWNLOAD_DIR.glob("*.zip"))
        pdfs_downloaded = list(DOWNLOAD_DIR.glob("*.pdf"))
        print(f"Downloaded: {len(zips)} zip files, {len(pdfs_downloaded)} PDFs")
        
        # Check extracts
        pdfs_extracted = list(EXTRACT_DIR.rglob("*.pdf"))
        print(f"Extracted: {len(pdfs_extracted)} PDF files")
        
        # Check cache
        cached = list(TEXT_CACHE_DIR.glob("*.txt"))
        print(f"Cached text: {len(cached)} files")
        
        # Check disk space
        stat = shutil.disk_usage(BASE_DIR)
        print(f"Disk space: {stat.free / (1024**3):.1f}GB free")

        # Check libraries
        print(f"\nLibraries:")
        print(f"  pdfplumber: {'✓' if HAS_PDFPLUMBER else '✗'}")
        print(f"  pypdf: {'✓' if HAS_PYPDF else '✗'}")
        print(f"  OCR (pytesseract): {'✓' if HAS_OCR else '✗'}")
    
    else:
        print(f"Unknown command: {command}")
        print(__doc__)


if __name__ == "__main__":
    main()
