#!/usr/bin/env python3
"""
Extract named entities from Epstein file text cache using spaCy NER.

Usage:
    python ner_extract.py extract     # Run NER on all text_cache entries
    python ner_extract.py status      # Show entity stats
    python ner_extract.py graph       # Generate co-occurrence graph HTML
"""

import sys
import sqlite3
import re
from pathlib import Path
from collections import defaultdict, Counter
from itertools import combinations

import spacy

BASE_DIR = Path("./epstein_files")
DB_PATH = BASE_DIR / "epstein.db"
OUTPUT_DIR = BASE_DIR / "output"


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER REFERENCES files(id),
            entity_text TEXT NOT NULL,
            entity_label TEXT NOT NULL,
            normalized TEXT NOT NULL,
            count INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS entity_cooccurrence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_a TEXT NOT NULL,
            entity_b TEXT NOT NULL,
            file_count INTEGER DEFAULT 1,
            label_a TEXT,
            label_b TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_entities_file ON entities(file_id);
        CREATE INDEX IF NOT EXISTS idx_entities_normalized ON entities(normalized);
        CREATE INDEX IF NOT EXISTS idx_entities_label ON entities(entity_label);
        CREATE INDEX IF NOT EXISTS idx_cooccur_a ON entity_cooccurrence(entity_a);
        CREATE INDEX IF NOT EXISTS idx_cooccur_b ON entity_cooccurrence(entity_b);
    """)
    conn.commit()


def normalize_entity(text):
    """Normalize entity text for deduplication."""
    text = re.sub(r'\s+', ' ', text).strip()
    # Remove trailing punctuation
    text = text.rstrip('.,;:!?')
    return text


# Filter out junk entities (legal boilerplate, numbers, single chars)
JUNK_PATTERNS = [
    r'^\d+$',           # pure numbers
    r'^[A-Z]$',         # single letters
    r'^(the|a|an|of|in|to|for|and|or|but|at|by|on|from|with)$',
    r'^page\s*\d*$',
    r'^exhibit',
    r'^document',
    r'^\W+$',           # only punctuation
]
JUNK_RE = [re.compile(p, re.IGNORECASE) for p in JUNK_PATTERNS]

def is_junk(text):
    if len(text) < 2 or len(text) > 100:
        return True
    for pat in JUNK_RE:
        if pat.match(text):
            return True
    return False


def extract_entities(conn, batch_size=100):
    """Run spaCy NER over all text_cache entries."""
    init_tables(conn)

    # Check what's already done
    done_ids = set(r[0] for r in conn.execute(
        "SELECT DISTINCT file_id FROM entities"
    ).fetchall())

    rows = conn.execute("""
        SELECT tc.file_id, tc.extracted_text
        FROM text_cache tc
        WHERE tc.char_count > 10
        ORDER BY tc.file_id
    """).fetchall()

    todo = [(fid, txt) for fid, txt in rows if fid not in done_ids]
    print(f"Files with text: {len(rows)}")
    print(f"Already processed: {len(done_ids)}")
    print(f"Remaining: {len(todo)}")

    if not todo:
        print("All files already processed.")
        return

    nlp = spacy.load("en_core_web_sm", disable=["tagger", "parser", "lemmatizer"])
    # Only keep NER
    nlp.max_length = 500_000  # bump limit for big docs

    processed = 0
    total_entities = 0

    for file_id, text in todo:
        # Truncate very long texts to first 50K chars for speed
        if len(text) > 50000:
            text = text[:50000]

        try:
            doc = nlp(text)
        except Exception:
            processed += 1
            continue

        # Count entities per file
        ent_counts = Counter()
        ent_labels = {}
        for ent in doc.ents:
            if ent.label_ not in ("PERSON", "ORG", "GPE", "NORP", "FAC", "EVENT", "LAW"):
                continue
            norm = normalize_entity(ent.text)
            if is_junk(norm):
                continue
            ent_counts[norm] += 1
            ent_labels[norm] = ent.label_

        # Insert
        for norm, count in ent_counts.items():
            conn.execute(
                "INSERT INTO entities (file_id, entity_text, entity_label, normalized, count) VALUES (?, ?, ?, ?, ?)",
                (file_id, norm, ent_labels[norm], norm.lower(), count)
            )
            total_entities += 1

        processed += 1
        if processed % 50 == 0:
            conn.commit()
            print(f"  {processed}/{len(todo)} files, {total_entities} entities extracted")

    conn.commit()
    print(f"\nDone: {processed} files, {total_entities} entities")


def build_cooccurrence(conn, min_docs=2):
    """Build entity co-occurrence from entities table."""
    print("\nBuilding co-occurrence edges...")

    conn.execute("DELETE FROM entity_cooccurrence")
    conn.commit()

    # Get entities grouped by file, only PERSON entities with 2+ mentions
    rows = conn.execute("""
        SELECT file_id, normalized, entity_label
        FROM entities
        WHERE entity_label = 'PERSON' AND count >= 1
        ORDER BY file_id
    """).fetchall()

    # Group by file
    file_entities = defaultdict(set)
    entity_labels = {}
    for fid, norm, label in rows:
        file_entities[fid].add(norm)
        entity_labels[norm] = label

    # Count co-occurrences
    pair_counts = Counter()
    for fid, ents in file_entities.items():
        ents = sorted(ents)
        for a, b in combinations(ents, 2):
            pair_counts[(a, b)] += 1

    # Filter and insert
    inserted = 0
    for (a, b), count in pair_counts.items():
        if count >= min_docs:
            conn.execute(
                "INSERT INTO entity_cooccurrence (entity_a, entity_b, file_count, label_a, label_b) VALUES (?, ?, ?, ?, ?)",
                (a, b, count, entity_labels.get(a, 'PERSON'), entity_labels.get(b, 'PERSON'))
            )
            inserted += 1

    conn.commit()
    print(f"  Edges with {min_docs}+ co-occurrences: {inserted}")


def show_status(conn):
    """Show entity extraction stats."""
    init_tables(conn)

    total = conn.execute("SELECT COUNT(DISTINCT file_id) FROM entities").fetchone()[0]
    ent_count = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\nFiles with entities: {total}")
    print(f"Total entity rows: {ent_count}")

    print("\nTop 30 PERSON entities:")
    rows = conn.execute("""
        SELECT normalized, SUM(count) as total, COUNT(DISTINCT file_id) as files
        FROM entities WHERE entity_label = 'PERSON'
        GROUP BY normalized ORDER BY files DESC LIMIT 30
    """).fetchall()
    for norm, total, files in rows:
        print(f"  {files:5d} files  {total:6d} mentions  {norm}")

    print("\nTop 20 ORG entities:")
    rows = conn.execute("""
        SELECT normalized, SUM(count) as total, COUNT(DISTINCT file_id) as files
        FROM entities WHERE entity_label = 'ORG'
        GROUP BY normalized ORDER BY files DESC LIMIT 20
    """).fetchall()
    for norm, total, files in rows:
        print(f"  {files:5d} files  {total:6d} mentions  {norm}")

    cooccur = conn.execute("SELECT COUNT(*) FROM entity_cooccurrence").fetchone()[0]
    print(f"\nCo-occurrence edges: {cooccur}")

    if cooccur > 0:
        print("\nTop 20 co-occurring pairs:")
        rows = conn.execute("""
            SELECT entity_a, entity_b, file_count
            FROM entity_cooccurrence
            ORDER BY file_count DESC LIMIT 20
        """).fetchall()
        for a, b, count in rows:
            print(f"  {count:5d} files  {a} <-> {b}")


def generate_graph(conn, min_edge_weight=3, max_nodes=150):
    """Generate interactive HTML graph with pyvis."""
    from pyvis.network import Network

    init_tables(conn)

    # Get top entities by file count
    top_entities = conn.execute("""
        SELECT normalized, entity_label, SUM(count) as total, COUNT(DISTINCT file_id) as files
        FROM entities
        WHERE entity_label IN ('PERSON', 'ORG')
        GROUP BY normalized
        HAVING files >= ?
        ORDER BY files DESC
        LIMIT ?
    """, (min_edge_weight, max_nodes)).fetchall()

    entity_set = {e[0] for e in top_entities}
    entity_info = {e[0]: (e[1], e[2], e[3]) for e in top_entities}

    # Get edges between these entities
    edges = conn.execute("""
        SELECT entity_a, entity_b, file_count
        FROM entity_cooccurrence
        WHERE file_count >= ?
        ORDER BY file_count DESC
    """, (min_edge_weight,)).fetchall()

    net = Network(height="800px", width="100%", bgcolor="#1a1a2e", font_color="white")
    net.barnes_hut(gravity=-3000, central_gravity=0.3, spring_length=200)

    # Color by type
    colors = {"PERSON": "#e74c3c", "ORG": "#3498db", "GPE": "#2ecc71", "NORP": "#f39c12"}

    added_nodes = set()
    for a, b, weight in edges:
        if a not in entity_set or b not in entity_set:
            continue
        for node in (a, b):
            if node not in added_nodes:
                label_type, total, files = entity_info.get(node, ("PERSON", 1, 1))
                color = colors.get(label_type, "#95a5a6")
                size = min(8 + files * 2, 50)
                net.add_node(node, label=node, color=color, size=size,
                           title=f"{node}\n{label_type}\n{files} files, {total} mentions")
                added_nodes.add(node)
        net.add_edge(a, b, value=weight, title=f"{weight} shared files")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "entity_graph.html"
    net.save_graph(str(out_path))
    print(f"\nGraph saved to {out_path}")
    print(f"  Nodes: {len(added_nodes)}, Edges: {len([e for e in edges if e[0] in entity_set and e[1] in entity_set])}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    conn = get_db()
    command = sys.argv[1].lower()

    if command == "extract":
        extract_entities(conn)
        build_cooccurrence(conn)
    elif command == "cooccur":
        min_docs = int(sys.argv[2]) if len(sys.argv) > 2 else 2
        build_cooccurrence(conn, min_docs=min_docs)
    elif command == "status":
        show_status(conn)
    elif command == "graph":
        min_w = int(sys.argv[2]) if len(sys.argv) > 2 else 3
        generate_graph(conn, min_edge_weight=min_w)
    else:
        print(f"Unknown command: {command}")
        print(__doc__)

    conn.close()


if __name__ == "__main__":
    main()
