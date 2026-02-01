#!/usr/bin/env python3
"""Streamlit app to explore Epstein files database."""

import sqlite3
import re
import pandas as pd
import streamlit as st
from pathlib import Path

DB_PATH = Path("./epstein_files/epstein.db")
BASE_DIR = Path("./epstein_files")
KEYWORDS_FILE = Path("./custom_keywords.txt")


@st.cache_resource
def get_db():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def main():
    st.set_page_config(page_title="Epstein Files Explorer", layout="wide")
    st.title("Epstein Files Explorer")
    st.caption("60,000+ DOJ files | 146M+ characters extracted | 178 keywords searched")

    conn = get_db()

    tab1, tab2, tab_bannon, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "Overview", "Trump Analysis", "The Bannon Situation", "Keyword Search", "Browse Files",
        "Full-Text Search", "Dataset Stats", "Relationship Graph"
    ])

    # ── TAB 1: OVERVIEW ──
    with tab1:
        col1, col2, col3, col4 = st.columns(4)

        total_files = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        total_text = conn.execute("SELECT COUNT(*) FROM text_cache").fetchone()[0]
        total_chars = conn.execute("SELECT SUM(char_count) FROM text_cache").fetchone()[0] or 0
        total_keywords = conn.execute("SELECT COUNT(DISTINCT keyword) FROM search_results").fetchone()[0]
        total_matches = conn.execute("SELECT SUM(match_count) FROM search_results").fetchone()[0] or 0
        needs_ocr = conn.execute("SELECT COUNT(*) FROM files WHERE needs_ocr = 1").fetchone()[0]

        col1.metric("Total PDF Files", f"{total_files:,}")
        col2.metric("Text Extracted", f"{total_text:,}")
        col3.metric("Total Characters", f"{total_chars / 1_000_000:.1f}M")
        col4.metric("Keyword Matches", f"{total_matches:,}")

        st.markdown("---")

        # Dataset breakdown
        st.subheader("Files by Dataset")
        df_ds = pd.read_sql_query("""
            SELECT dataset as 'Dataset',
                   COUNT(*) as 'Files',
                   ROUND(SUM(file_size) / 1024.0 / 1024.0, 1) as 'Size (MB)',
                   SUM(has_text) as 'Has Text',
                   SUM(needs_ocr) as 'Needs OCR'
            FROM files GROUP BY dataset ORDER BY dataset
        """, conn)
        st.dataframe(df_ds, use_container_width=True, hide_index=True)

        # Production files
        st.subheader("Production Files (DS10)")
        df_prod = pd.read_sql_query("""
            SELECT file_type as 'Type',
                   COUNT(*) as 'Count',
                   ROUND(SUM(file_size) / 1024.0 / 1024.0, 1) as 'Size (MB)'
            FROM production_files GROUP BY file_type ORDER BY COUNT(*) DESC
        """, conn)
        st.dataframe(df_prod, use_container_width=True, hide_index=True)

        st.subheader("The Gap")
        st.markdown(f"""
        - **DOJ claimed:** 3.5 million pages
        - **Actual files found:** {total_files:,}
        - **Files with extractable text:** {total_text:,}
        - **Files needing OCR:** {needs_ocr:,}
        - **EFTA ID slots checked:** ~2.7 million
        - **Fill rate:** ~0.08%
        - **99.92% of file slots are empty**
        """)

    # ── TAB 2: TRUMP ANALYSIS ──
    with tab2:
        st.subheader("Donald Trump - Epstein Files Analysis")

        # Key metrics
        trump_files = pd.read_sql_query("""
            SELECT DISTINCT f.id, f.filename, f.dataset, f.rel_path, tc.extracted_text, tc.char_count
            FROM text_cache tc
            JOIN files f ON f.id = tc.file_id
            WHERE tc.extracted_text LIKE '%Trump%'
        """, conn)

        trump_donald = pd.read_sql_query("""
            SELECT DISTINCT f.id, f.filename, f.dataset
            FROM text_cache tc JOIN files f ON f.id = tc.file_id
            WHERE tc.extracted_text LIKE '%Donald Trump%'
        """, conn)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Files mentioning 'Trump'", f"{len(trump_files):,}")
        col2.metric("Files with 'Donald Trump'", f"{len(trump_donald):,}")

        # Count total Trump mentions across all text
        trump_total = conn.execute("""
            SELECT SUM(length(extracted_text) - length(replace(lower(extracted_text), 'trump', ''))) / 5
            FROM text_cache WHERE extracted_text LIKE '%Trump%'
        """).fetchone()[0] or 0
        col3.metric("Total 'Trump' mentions", f"{trump_total:,}")

        # Mar-a-Lago mentions
        mal_count = conn.execute("SELECT COUNT(*) FROM text_cache WHERE extracted_text LIKE '%Mar-a-Lago%'").fetchone()[0]
        col4.metric("Mar-a-Lago mentions", f"{mal_count}")

        st.markdown("---")

        # ── Trump files by dataset ──
        st.subheader("Trump References by Dataset")
        df_trump_ds = trump_files.groupby('dataset').size().reset_index(name='Files')
        df_trump_ds.columns = ['Dataset', 'Files']
        st.bar_chart(df_trump_ds.set_index('Dataset'))

        # ── Co-occurrence: Who appears WITH Trump ──
        st.subheader("Who Appears in the Same Files as Trump")
        st.caption("Keywords that co-occur with 'Trump' in the same documents")

        trump_file_ids = trump_files['id'].tolist()
        if trump_file_ids:
            placeholders = ','.join(['?'] * len(trump_file_ids))
            df_cooccur = pd.read_sql_query(f"""
                SELECT sr.keyword as 'Name',
                       COUNT(DISTINCT sr.file_id) as 'Shared Files',
                       SUM(sr.match_count) as 'Total Mentions'
                FROM search_results sr
                WHERE sr.file_id IN ({placeholders})
                AND sr.keyword NOT IN ('Donald Trump', 'Trump')
                GROUP BY sr.keyword
                ORDER BY COUNT(DISTINCT sr.file_id) DESC
                LIMIT 30
            """, conn, params=trump_file_ids)

            if not df_cooccur.empty:
                st.bar_chart(df_cooccur.set_index('Name')['Shared Files'])
                st.dataframe(df_cooccur, use_container_width=True, hide_index=True)

        st.markdown("---")

        # ── Key categories ──
        st.subheader("Document Categories")

        categories = {
            'Rape/Sexual Assault Allegations': ['rape', 'Katie Johnson', 'sexual abuse', 'sexual assault'],
            'Deutsche Bank Connection': ['Deutsche Bank'],
            'Mar-a-Lago / Palm Beach': ['Mar-a-Lago', 'Palm Beach'],
            'Flight Records': ['flight', 'Lolita Express'],
            'Legal Proceedings': ['lawsuit', 'deposition', 'testimony', 'indicted'],
            'Virginia Roberts/Giuffre': ['Virginia Roberts', 'Virginia Giuffre', 'Giuffre'],
            'Ghislaine Maxwell': ['Ghislaine', 'Maxwell'],
            'Prince Andrew': ['Prince Andrew', 'Duke of York'],
            'Bill Clinton': ['Bill Clinton', 'Clinton'],
        }

        cat_data = []
        for cat_name, terms in categories.items():
            count = 0
            for term in terms:
                c = conn.execute("""
                    SELECT COUNT(DISTINCT tc.file_id) FROM text_cache tc
                    WHERE tc.file_id IN (SELECT id FROM files WHERE id IN (
                        SELECT DISTINCT f.id FROM text_cache t2
                        JOIN files f ON f.id = t2.file_id
                        WHERE t2.extracted_text LIKE '%Trump%'
                    ))
                    AND tc.extracted_text LIKE ?
                """, (f"%{term}%",)).fetchone()[0]
                count = max(count, c)
            cat_data.append({'Category': cat_name, 'Trump Files': count})

        df_cats = pd.DataFrame(cat_data).sort_values('Trump Files', ascending=False)
        st.bar_chart(df_cats.set_index('Category'))

        st.markdown("---")

        # ── Timeline: documents mentioning Trump by context ──
        st.subheader("Key Document Evidence")

        evidence = [
            ("Katie Johnson Rape Allegation",
             "accusing Epstein and real estate businessman Donald Trump of raping her in 1994, when she was 13 years old",
             "Multiple FBI/DOJ files document the April 2016 federal lawsuit"),
            ("Deutsche Bank - Trump Relationship",
             "As per KYCS account is part of DONALD TRUMP relationship. The purpose of this account will then be used to make payments to vendors",
             "Internal bank compliance notes linking Trump accounts to Epstein-related banking"),
            ("Mar-a-Lago Connection",
             "In 2000 she and Epstein were seen on holiday with Prince Andrew at Donald Trump's Mar-a-Lago Club in Palm Beach",
             "Maxwell, Epstein, and Prince Andrew socializing at Trump's property"),
            ("Social Circle",
             "A part-time Palm Beacher who has socialized with Donald Trump, Bill Clinton and Kevin Spacey was jailed",
             "Palm Beach Post reporting on Epstein's social connections"),
            ("jeevacation@gmail.com - Trump Email",
             "From: Thomas Jr., Landon To: Jeffrey E. jeevacation@gmail.com Subject: Re: Trump",
             "December 2015 email from Epstein's Gmail discussing Trump during presidential campaign"),
            ("NYMag Profile",
             "He's pals with...socialite Ghislaine Maxwell, even Donald Trump",
             "New York Magazine profile of Epstein's social network"),
        ]

        for title, quote, note in evidence:
            with st.expander(title):
                st.markdown(f"> {quote}")
                st.caption(note)

        st.markdown("---")

        # ── Browse all Trump files ──
        st.subheader("All Files Mentioning Trump")
        st.caption(f"{len(trump_files)} files found")

        df_display = trump_files[['filename', 'dataset', 'char_count', 'rel_path']].copy()
        df_display.columns = ['File', 'Dataset', 'Text Length', 'Path']
        df_display = df_display.sort_values('Dataset')
        st.dataframe(df_display, use_container_width=True, hide_index=True, height=400)

        # View individual Trump file
        st.subheader("Read a Trump File")
        trump_filenames = trump_files['filename'].tolist()
        if trump_filenames:
            selected_file = st.selectbox("Select file", trump_filenames)
            if st.button("Load full text", key="trump_load"):
                row = trump_files[trump_files['filename'] == selected_file].iloc[0]
                st.text_area("Full text", row['extracted_text'], height=500)

    # ── TAB: THE BANNON SITUATION ──
    with tab_bannon:
        st.subheader("The Bannon Situation")
        st.caption("All references to Steve Bannon, Bannon, and the 'Bubba' email in the Epstein files")

        # Search for Bannon mentions
        bannon_files = pd.read_sql_query("""
            SELECT DISTINCT f.id, f.filename, f.dataset, f.rel_path, tc.extracted_text, tc.char_count
            FROM text_cache tc
            JOIN files f ON f.id = tc.file_id
            WHERE tc.extracted_text LIKE '%Bannon%'
        """, conn)

        # Also search for Bubba (the email where Mark Epstein asks Jeffrey to ask Bannon about Putin/Trump photos)
        bubba_files = pd.read_sql_query("""
            SELECT DISTINCT f.id, f.filename, f.dataset, f.rel_path, tc.extracted_text, tc.char_count
            FROM text_cache tc
            JOIN files f ON f.id = tc.file_id
            WHERE tc.extracted_text LIKE '%Bubba%'
        """, conn)

        # Steve Bannon specific
        steve_bannon_files = pd.read_sql_query("""
            SELECT DISTINCT f.id, f.filename, f.dataset, f.rel_path, tc.extracted_text, tc.char_count
            FROM text_cache tc
            JOIN files f ON f.id = tc.file_id
            WHERE tc.extracted_text LIKE '%Steve Bannon%' OR tc.extracted_text LIKE '%Stephen Bannon%'
        """, conn)

        col1, col2, col3 = st.columns(3)
        col1.metric("Files mentioning 'Bannon'", f"{len(bannon_files):,}")
        col2.metric("Files with 'Steve/Stephen Bannon'", f"{len(steve_bannon_files):,}")
        col3.metric("Files mentioning 'Bubba'", f"{len(bubba_files):,}")

        st.markdown("---")

        # Context: The Bubba Email
        st.subheader("Key Context: The 'Bubba' Email")
        st.markdown("""
        From the House Oversight Committee release: Mark Epstein (Jeffrey's brother) emailed Jeffrey
        asking him to ask **Steve Bannon** *"if Putin has the photos of Trump blowing Bubba."*

        "Bubba" is a well-known nickname for **Bill Clinton**.

        This email suggests:
        1. Jeffrey Epstein had a direct line to Steve Bannon
        2. There were alleged compromising photos involving Trump and Clinton
        3. The implication that Russia/Putin may have possessed kompromat
        """)

        st.markdown("---")

        # Co-occurring entities in Bannon files
        if not bannon_files.empty:
            st.subheader("Who Appears in Bannon Files")
            bannon_ids = bannon_files['id'].tolist()
            if bannon_ids:
                ph = ','.join(['?'] * len(bannon_ids))
                df_cooccur = pd.read_sql_query(f"""
                    SELECT e.normalized as Entity, e.entity_label as Type,
                           COUNT(DISTINCT e.file_id) as Files,
                           SUM(e.count) as Mentions
                    FROM entities e
                    WHERE e.file_id IN ({ph})
                    AND e.normalized NOT LIKE '%bannon%'
                    GROUP BY e.normalized, e.entity_label
                    ORDER BY Files DESC
                    LIMIT 30
                """, conn, params=bannon_ids)

                if not df_cooccur.empty:
                    st.dataframe(df_cooccur, use_container_width=True, hide_index=True)

        st.markdown("---")

        # Browse all Bannon files with context snippets
        st.subheader("All Files Mentioning Bannon")
        st.caption(f"{len(bannon_files)} files found")

        if not bannon_files.empty:
            for _, row in bannon_files.iterrows():
                text = row['extracted_text']
                lower = text.lower()
                idx = lower.find('bannon')
                if idx >= 0:
                    start = max(0, idx - 300)
                    end = min(len(text), idx + 300)
                    snippet = text[start:end].strip()
                else:
                    snippet = text[:600]

                with st.expander(f"[DS{row['dataset']}] {row['filename']} ({row['char_count']:,} chars)"):
                    st.markdown(f"Path: `{row['rel_path']}`")
                    st.markdown(f"...{snippet}...")
                    if st.button("Show full text", key=f"bannon_full_{row['id']}"):
                        st.text_area("Full text", text, height=500, key=f"bannon_text_{row['id']}")

        # Same for Bubba if different files
        bubba_only = bubba_files[~bubba_files['id'].isin(bannon_files['id'])] if not bannon_files.empty else bubba_files
        if not bubba_only.empty:
            st.markdown("---")
            st.subheader("Additional Files Mentioning 'Bubba' (not in Bannon results)")
            for _, row in bubba_only.iterrows():
                text = row['extracted_text']
                lower = text.lower()
                idx = lower.find('bubba')
                if idx >= 0:
                    start = max(0, idx - 300)
                    end = min(len(text), idx + 300)
                    snippet = text[start:end].strip()
                else:
                    snippet = text[:600]

                with st.expander(f"[DS{row['dataset']}] {row['filename']} ({row['char_count']:,} chars)"):
                    st.markdown(f"Path: `{row['rel_path']}`")
                    st.markdown(f"...{snippet}...")
                    if st.button("Show full text", key=f"bubba_full_{row['id']}"):
                        st.text_area("Full text", text, height=500, key=f"bubba_text_{row['id']}")

    # ── TAB 3: KEYWORD SEARCH RESULTS ──
    with tab3:
        st.subheader("Keyword Hit Summary")

        # Get all keywords with counts
        df_kw = pd.read_sql_query("""
            SELECT keyword as 'Keyword',
                   COUNT(*) as 'Files',
                   SUM(match_count) as 'Total Matches'
            FROM search_results
            GROUP BY keyword
            ORDER BY SUM(match_count) DESC
        """, conn)

        # Filter controls
        col1, col2 = st.columns([1, 3])
        with col1:
            min_matches = st.number_input("Min matches", value=10, min_value=0)
        df_filtered = df_kw[df_kw["Total Matches"] >= min_matches]

        st.dataframe(df_filtered, use_container_width=True, hide_index=True, height=400)

        st.markdown("---")

        # Drill into a keyword
        st.subheader("Keyword Detail")
        keyword_list = df_kw["Keyword"].tolist()
        if keyword_list:
            selected_kw = st.selectbox("Select keyword", keyword_list)

            df_detail = pd.read_sql_query("""
                SELECT f.filename as 'File',
                       f.dataset as 'Dataset',
                       sr.match_count as 'Matches',
                       sr.context as 'Context'
                FROM search_results sr
                JOIN files f ON f.id = sr.file_id
                WHERE sr.keyword = ?
                ORDER BY sr.match_count DESC
                LIMIT 100
            """, conn, params=[selected_kw])

            st.write(f"**{selected_kw}**: {len(df_detail)} files (showing top 100)")
            st.dataframe(df_detail, use_container_width=True, hide_index=True, height=400)

    # ── TAB 4: BROWSE FILES ──
    with tab4:
        st.subheader("Browse Files")

        col1, col2, col3 = st.columns(3)
        with col1:
            datasets = conn.execute("SELECT DISTINCT dataset FROM files ORDER BY dataset").fetchall()
            ds_options = ["All"] + [str(d[0]) for d in datasets]
            selected_ds = st.selectbox("Dataset", ds_options)
        with col2:
            search_filename = st.text_input("Filename contains", "")
        with col3:
            text_filter = st.selectbox("Text status", ["All", "Has text", "Needs OCR", "No text"])

        query = "SELECT f.id, f.filename, f.dataset, f.file_size, f.has_text, f.needs_ocr, f.rel_path FROM files f WHERE 1=1"
        params = []

        if selected_ds != "All":
            query += " AND f.dataset = ?"
            params.append(int(selected_ds))
        if search_filename:
            query += " AND f.filename LIKE ?"
            params.append(f"%{search_filename}%")
        if text_filter == "Has text":
            query += " AND f.has_text = 1"
        elif text_filter == "Needs OCR":
            query += " AND f.needs_ocr = 1"
        elif text_filter == "No text":
            query += " AND f.has_text = 0 AND f.needs_ocr = 0"

        query += " ORDER BY f.dataset, f.filename LIMIT 500"

        df_files = pd.read_sql_query(query, conn, params=params)
        st.write(f"Showing {len(df_files)} files (limit 500)")
        st.dataframe(df_files, use_container_width=True, hide_index=True, height=400)

        # View file text
        st.markdown("---")
        st.subheader("View Extracted Text")
        file_id_input = st.number_input("Enter file ID to view text", min_value=1, value=1)

        if st.button("Load Text"):
            row = conn.execute("""
                SELECT f.filename, f.rel_path, tc.extracted_text, tc.char_count, tc.method
                FROM files f
                LEFT JOIN text_cache tc ON tc.file_id = f.id
                WHERE f.id = ?
            """, (file_id_input,)).fetchone()

            if row:
                fname, rel_path, text, char_count, method = row
                st.write(f"**{fname}** | Path: `{rel_path}` | Method: {method} | Chars: {char_count:,}" if text else f"**{fname}** - No text extracted")
                if text:
                    st.text_area("Extracted text", text, height=400)
            else:
                st.warning("File ID not found")

    # ── TAB 5: FULL-TEXT SEARCH ──
    with tab5:
        st.subheader("Search All Extracted Text")
        st.caption("Search across 146M+ characters of extracted text")

        search_term = st.text_input("Search term (case-insensitive)")

        if search_term and st.button("Search"):
            results_container = st.empty()
            status = st.status(f"Searching 146M+ chars for '{search_term}'...", expanded=True)
            results_area = st.container()

            # Stream results as they come in
            cursor = conn.execute("""
                SELECT f.id, f.filename, f.dataset, f.rel_path, tc.extracted_text
                FROM text_cache tc
                JOIN files f ON f.id = tc.file_id
                WHERE tc.extracted_text LIKE ?
                LIMIT 200
            """, (f"%{search_term}%",))

            hit_count = 0
            results = []
            batch_size = 10

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                results.extend(rows)
                hit_count += len(rows)
                status.update(label=f"Found {hit_count} files so far...", expanded=True)

            status.update(label=f"Done - {hit_count} files found", state="complete", expanded=False)

            for fid, fname, ds, rel_path, text in results:
                # Find context around match
                lower_text = text.lower()
                idx = lower_text.find(search_term.lower())
                if idx >= 0:
                    start = max(0, idx - 200)
                    end = min(len(text), idx + len(search_term) + 200)
                    context = text[start:end]
                    # Bold the match
                    match_start = idx - start
                    match_end = match_start + len(search_term)
                    highlighted = (
                        context[:match_start]
                        + "**" + context[match_start:match_end] + "**"
                        + context[match_end:]
                    )
                else:
                    highlighted = text[:400]

                with results_area.expander(f"[DS{ds}] {fname} (ID: {fid})"):
                    st.markdown(f"Path: `{rel_path}`")
                    st.markdown(f"...{highlighted}...")
                    if st.button(f"Show full text", key=f"full_{fid}"):
                        st.text_area("Full extracted text", text, height=500, key=f"text_{fid}")

    # ── TAB 6: DATASET STATS ──
    with tab6:
        st.subheader("Bruteforce Audit Results")

        st.markdown("""
        | Dataset | EFTA Range | Total Slots | Files Found | Fill Rate |
        |---------|-----------|-------------|-------------|-----------|
        | 8 | 00000001-00423792 | 423,792 | ~11 | ~0.003% |
        | 9 | 00423793-01262781 | 838,989 | 807 | 0.096% |
        | 10 | 01262782-02212882 | 950,101 | 54,987 | ~5.8%* |
        | 11 | 02212883-02730264 | 517,382 | 408 | 0.079% |

        *DS10 high count is from browser-scraped listing pages, not bruteforce alone (686 from bruteforce).
        """)

        st.subheader("File Size Distribution")
        df_sizes = pd.read_sql_query("""
            SELECT
                CASE
                    WHEN file_size < 10240 THEN '< 10KB'
                    WHEN file_size < 102400 THEN '10KB - 100KB'
                    WHEN file_size < 1048576 THEN '100KB - 1MB'
                    WHEN file_size < 10485760 THEN '1MB - 10MB'
                    ELSE '> 10MB'
                END as 'Size Range',
                COUNT(*) as 'Count'
            FROM files
            GROUP BY 1
            ORDER BY MIN(file_size)
        """, conn)
        st.bar_chart(df_sizes.set_index("Size Range"))

        st.subheader("Text Extraction Stats")
        df_text = pd.read_sql_query("""
            SELECT
                CASE
                    WHEN char_count < 100 THEN '< 100 chars'
                    WHEN char_count < 1000 THEN '100 - 1K chars'
                    WHEN char_count < 10000 THEN '1K - 10K chars'
                    WHEN char_count < 100000 THEN '10K - 100K chars'
                    ELSE '> 100K chars'
                END as 'Text Length',
                COUNT(*) as 'Files'
            FROM text_cache
            GROUP BY 1
            ORDER BY MIN(char_count)
        """, conn)
        st.bar_chart(df_text.set_index("Text Length"))

    # ── TAB 7: RELATIONSHIP GRAPH ──
    with tab7:
        st.subheader("Entity Relationship Graph")

        # Check if entities table exists and has data
        has_entities = False
        try:
            ent_count = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
            has_entities = ent_count > 0
        except Exception:
            pass

        if not has_entities:
            st.warning("No entities extracted yet. Run: `.venv/bin/python ner_extract.py extract`")
            st.code(".venv/bin/python ner_extract.py extract", language="bash")
        else:
            col1, col2, col3 = st.columns(3)
            total_ents = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
            unique_ents = conn.execute("SELECT COUNT(DISTINCT normalized) FROM entities").fetchone()[0]
            files_with_ents = conn.execute("SELECT COUNT(DISTINCT file_id) FROM entities").fetchone()[0]
            col1.metric("Total Entity Mentions", f"{total_ents:,}")
            col2.metric("Unique Entities", f"{unique_ents:,}")
            col3.metric("Files with Entities", f"{files_with_ents:,}")

            st.markdown("---")

            # Top people
            st.subheader("Top People Mentioned")
            df_people = pd.read_sql_query("""
                SELECT normalized as Name, entity_label as Type,
                       SUM(count) as Mentions, COUNT(DISTINCT file_id) as Files
                FROM entities WHERE entity_label = 'PERSON'
                GROUP BY normalized ORDER BY Files DESC LIMIT 50
            """, conn)
            st.dataframe(df_people, use_container_width=True)

            # Top orgs
            st.subheader("Top Organizations")
            df_orgs = pd.read_sql_query("""
                SELECT normalized as Name,
                       SUM(count) as Mentions, COUNT(DISTINCT file_id) as Files
                FROM entities WHERE entity_label = 'ORG'
                GROUP BY normalized ORDER BY Files DESC LIMIT 30
            """, conn)
            st.dataframe(df_orgs, use_container_width=True)

            # Co-occurrence graph
            st.markdown("---")
            st.subheader("Co-occurrence Network")

            has_cooccur = False
            try:
                cooccur_count = conn.execute("SELECT COUNT(*) FROM entity_cooccurrence").fetchone()[0]
                has_cooccur = cooccur_count > 0
            except Exception:
                pass

            if not has_cooccur:
                st.info("Run co-occurrence analysis: `.venv/bin/python ner_extract.py cooccur`")
            else:
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    min_weight = st.slider("Minimum shared files", 2, 20, 3)
                with col_b:
                    max_nodes = st.slider("Max nodes", 20, 300, 100)
                with col_c:
                    show_orgs = st.checkbox("Show organizations", value=True)

                # Build graph with pyvis
                graph_path = BASE_DIR / "output" / "entity_graph.html"

                if st.button("Generate Graph") or graph_path.exists():
                    try:
                        from pyvis.network import Network

                        # Key figures to always include regardless of threshold
                        vip_names = {
                            'jeffrey epstein', 'ghislaine maxwell', 'donald trump',
                            'donald j. trump', 'bill clinton', 'prince andrew',
                            'alan dershowitz', 'les wexner', 'jean-luc brunel',
                            'virginia roberts', 'virginia giuffre',
                        }

                        # Get top entities by file count
                        entity_types = ('PERSON', 'ORG') if show_orgs else ('PERSON',)
                        type_ph = ','.join(['?'] * len(entity_types))
                        top_entities = conn.execute(f"""
                            SELECT normalized, entity_label, SUM(count) as total, COUNT(DISTINCT file_id) as files
                            FROM entities WHERE entity_label IN ({type_ph})
                            GROUP BY normalized HAVING files >= ?
                            ORDER BY files DESC LIMIT ?
                        """, list(entity_types) + [min_weight, max_nodes]).fetchall()

                        entity_set = {e[0] for e in top_entities}
                        entity_info = {e[0]: (e[1], e[2], e[3]) for e in top_entities}

                        # Force-add VIPs even if below threshold
                        for vip in vip_names:
                            if vip not in entity_set:
                                row = conn.execute("""
                                    SELECT normalized, entity_label, SUM(count), COUNT(DISTINCT file_id)
                                    FROM entities WHERE normalized = ?
                                    GROUP BY normalized
                                """, (vip,)).fetchone()
                                if row:
                                    entity_set.add(row[0])
                                    entity_info[row[0]] = (row[1], row[2], row[3])

                        # Get edges - include any edge where at least one side is a VIP (min 1 shared file)
                        edges = conn.execute("""
                            SELECT entity_a, entity_b, file_count
                            FROM entity_cooccurrence WHERE file_count >= ?
                            ORDER BY file_count DESC
                        """, (min_weight,)).fetchall()

                        # Also grab VIP edges at lower threshold
                        vip_list = list(vip_names)
                        vip_ph = ','.join(['?'] * len(vip_list))
                        vip_edges = conn.execute(f"""
                            SELECT entity_a, entity_b, file_count
                            FROM entity_cooccurrence
                            WHERE file_count >= 1
                            AND (entity_a IN ({vip_ph}) OR entity_b IN ({vip_ph}))
                            ORDER BY file_count DESC
                        """, vip_list + vip_list).fetchall()

                        all_edges = {(a, b): w for a, b, w in edges}
                        for a, b, w in vip_edges:
                            if (a, b) not in all_edges:
                                all_edges[(a, b)] = w

                        net = Network(height="700px", width="100%", bgcolor="#0e1117", font_color="white")
                        net.barnes_hut(gravity=-3000, central_gravity=0.3, spring_length=200)

                        colors = {"PERSON": "#e74c3c", "ORG": "#3498db"}
                        epstein_names = {'jeffrey epstein', 'epstein', 'jeffrey'}
                        # VIPs get yellow/gold so they stand out
                        added = set()
                        edge_count = 0
                        for (a, b), w in all_edges.items():
                            if a not in entity_set or b not in entity_set:
                                continue
                            for node in (a, b):
                                if node not in added:
                                    lt, tot, files = entity_info.get(node, ("PERSON", 1, 1))
                                    if not show_orgs and lt == "ORG":
                                        continue
                                    if node in epstein_names:
                                        color = "#00ff41"  # matrix green
                                        size = 60
                                        shape = "diamond"
                                    elif node in vip_names:
                                        color = "#f1c40f"  # gold
                                        size = max(25, min(8 + files * 2, 50))
                                        shape = "star"
                                    else:
                                        color = colors.get(lt, "#95a5a6")
                                        size = min(8 + files * 2, 50)
                                        shape = "dot"
                                    net.add_node(node, label=node, color=color, size=size,
                                               shape=shape,
                                               title=f"{node}\n{lt}\n{files} files, {tot} mentions")
                                    added.add(node)
                            if a in added and b in added:
                                net.add_edge(a, b, value=w, title=f"{w} shared files")
                                edge_count += 1

                        graph_path.parent.mkdir(parents=True, exist_ok=True)
                        net.save_graph(str(graph_path))

                        st.caption(f"{len(added)} nodes, {edge_count} edges")
                        with open(graph_path, 'r') as f:
                            st.components.v1.html(f.read(), height=720, scrolling=True)

                        # Legend
                        st.markdown("""
                        **Legend:**
                        - :green[**Green Diamond**] — Jeffrey Epstein
                        - :orange[**Gold Star**] — Key figures (Trump, Clinton, Prince Andrew, Maxwell, Dershowitz, etc.)
                        - :red[**Red Dot**] — Other people
                        - :blue[**Blue Dot**] — Organizations
                        - **Line thickness** = number of shared files
                        """)
                    except ImportError:
                        st.error("pyvis not installed. Run: pip install pyvis")

            # Entity search
            st.markdown("---")
            st.subheader("Search Entities")
            entity_query = st.text_input("Search for a person or org", placeholder="e.g. Trump, Deutsche Bank")
            if entity_query:
                df_search = pd.read_sql_query("""
                    SELECT e.normalized as Entity, e.entity_label as Type,
                           f.filename as File, f.dataset as DS, e.count as Mentions
                    FROM entities e JOIN files f ON f.id = e.file_id
                    WHERE e.normalized LIKE ?
                    ORDER BY e.count DESC LIMIT 100
                """, conn, params=(f"%{entity_query.lower()}%",))
                st.dataframe(df_search, use_container_width=True)

    # ── SIDEBAR: KEYWORD MANAGEMENT ──
    with st.sidebar:
        st.header("Keyword Management")

        # Show current keyword count
        kw_count = conn.execute("SELECT COUNT(DISTINCT keyword) FROM search_results").fetchone()[0]
        st.metric("Keywords in DB", kw_count)

        st.markdown("---")
        st.subheader("Add New Keywords")
        st.caption("Add keywords and run search to index them permanently in the database")

        new_keywords = st.text_area(
            "New keywords (one per line)",
            placeholder="George Bush\nJoe Biden\nLindsey Graham",
            height=150
        )

        if st.button("Search & Add to Database"):
            keywords = [k.strip() for k in new_keywords.strip().split('\n') if k.strip()]
            if keywords:
                status = st.status(f"Searching {len(keywords)} keywords...", expanded=True)
                patterns = {kw: re.compile(re.escape(kw), re.IGNORECASE) for kw in keywords}

                # Get all text
                rows = conn.execute("""
                    SELECT f.id, f.filename, tc.extracted_text
                    FROM files f JOIN text_cache tc ON tc.file_id = f.id
                    WHERE tc.char_count > 0
                """).fetchall()

                total_hits = 0
                for kw, pattern in patterns.items():
                    # Clear old results for this keyword
                    conn.execute("DELETE FROM search_results WHERE keyword = ?", (kw,))
                    kw_hits = 0
                    for file_id, filename, text in rows:
                        matches = list(pattern.finditer(text))
                        if matches:
                            m = matches[0]
                            start = max(0, m.start() - 150)
                            end = min(len(text), m.end() + 150)
                            context = ' '.join(text[start:end].split())
                            conn.execute(
                                "INSERT INTO search_results (file_id, keyword, match_count, context) VALUES (?, ?, ?, ?)",
                                (file_id, kw, len(matches), context)
                            )
                            kw_hits += len(matches)
                    total_hits += kw_hits
                    status.update(label=f"'{kw}': {kw_hits} matches")

                conn.commit()

                # Also append to epstein_processor.py keyword list
                import sys
                sys.path.insert(0, '.')
                try:
                    from epstein_processor import DEFAULT_KEYWORDS
                    existing = set(DEFAULT_KEYWORDS)
                except ImportError:
                    existing = set()

                new_to_add = [kw for kw in keywords if kw not in existing]
                if new_to_add:
                    # Append to the file
                    with open('epstein_processor.py', 'r') as f:
                        content = f.read()
                    # Find the end of DEFAULT_KEYWORDS list
                    insert_point = content.rfind(']', 0, content.find('DEFAULT_KEYWORDS') + content[content.find('DEFAULT_KEYWORDS'):].find(']') + 1)
                    if insert_point == -1:
                        insert_point = content.find('\n]\n', content.find('DEFAULT_KEYWORDS'))
                    # Just save to custom file instead - safer
                    with open(str(KEYWORDS_FILE), 'a') as f:
                        for kw in new_to_add:
                            f.write(kw + '\n')

                status.update(label=f"Done - {total_hits} total matches for {len(keywords)} keywords", state="complete")
                st.rerun()

        st.markdown("---")

        # Show custom keywords file
        if KEYWORDS_FILE.exists():
            custom = KEYWORDS_FILE.read_text().strip()
            if custom:
                st.subheader("Custom Keywords Added")
                st.code(custom)


if __name__ == "__main__":
    main()
