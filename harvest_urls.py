#!/usr/bin/env python3
"""Harvest exact file URLs from DOJ data-set listing pages (resumable, one polite worker per dataset).

Usage: python harvest_urls.py 8 9 10 11
Writes epstein_files/url_lists/dataset{N}_urls.txt and dataset{N}_page.txt (next page to fetch).
"""
import random, re, sys, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import requests

OUT = Path("./epstein_files/url_lists")
BASE = "https://www.justice.gov"

def session():
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    s.cookies.set("justiceGovAgeVerified", "true", domain=".justice.gov")
    return s

def log(n, msg):
    print(f"[ds{n} {time.strftime('%H:%M:%S')}] {msg}", flush=True)

def harvest(n):
    s = session()
    urls_f, page_f = OUT / f"dataset{n}_urls.txt", OUT / f"dataset{n}_page.txt"
    seen = set(urls_f.read_text().split()) if urls_f.exists() else set()
    page = int(page_f.read_text()) if page_f.exists() else 0
    pat = re.compile(r'href="((?:https://www\.justice\.gov)?/epstein/files/DataSet%20' + str(n) + r'/[^"]+)"', re.I)
    empty = errs = 0
    log(n, f"start page {page}, {len(seen)} urls known")
    with open(urls_f, "a") as out:
        while True:
            time.sleep(1.0 + random.uniform(0, 1.0))
            try:
                r = s.get(f"{BASE}/epstein/doj-disclosures/data-set-{n}-files?page={page}", timeout=60)
            except Exception as e:
                errs += 1; log(n, f"page {page} error {e!r}, retry in {min(300, 10*errs)}s"); time.sleep(min(300, 10*errs)); continue
            if r.status_code != 200:
                errs += 1; log(n, f"page {page} HTTP {r.status_code}, retry in {min(300, 10*errs)}s"); time.sleep(min(300, 10*errs)); continue
            errs = 0
            new = [u if u.startswith("http") else BASE + u for u in pat.findall(r.text)]
            new = [u for u in dict.fromkeys(new) if u not in seen]
            if not pat.search(r.text):
                empty += 1
                if empty >= 3: log(n, f"done at page {page}, {len(seen)} urls"); return
            else:
                empty = 0
            for u in new: seen.add(u); out.write(u + "\n")
            out.flush(); page += 1; page_f.write_text(str(page))
            if page % 100 == 0: log(n, f"page {page}, {len(seen)} urls")

if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    ds = [int(a) for a in sys.argv[1:]]
    with ThreadPoolExecutor(len(ds)) as ex: list(ex.map(harvest, ds))
