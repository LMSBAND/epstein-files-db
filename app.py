#!/usr/bin/env python3
"""Streamlit app to explore Epstein files — graph-centered."""

import sqlite3
import re
import pandas as pd
import streamlit as st
from pathlib import Path

DB_PATH = Path("./epstein_files/epstein.db")
BASE_DIR = Path("./epstein_files")


@st.cache_resource
def get_db():
    if not DB_PATH.exists():
        st.error("Database not found. See README for setup instructions.")
        st.stop()
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def main():
    st.set_page_config(page_title="Epstein Files DB", layout="wide")
    conn = get_db()

    # Header stats
    total_files = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    total_ents = conn.execute("SELECT COUNT(DISTINCT normalized) FROM entities WHERE entity_label='PERSON'").fetchone()[0]
    cooccur_edges = conn.execute("SELECT COUNT(*) FROM entity_cooccurrence").fetchone()[0]

    st.title("Epstein Files DB")
    st.caption(f"{total_files:,} files | {total_ents:,} people identified | {cooccur_edges:,} relationship edges")

    st.link_button(
        "⬇ Download the database from GitHub Releases",
        "https://github.com/LMSBAND/epstein-files-db/releases/tag/v1.0",
        type="primary",
    )

    tab_graph, tab_search, tab_method = st.tabs(["Relationship Graph", "Search", "Methodology / Unknowns"])

    # ── TAB 1: RELATIONSHIP GRAPH ──
    with tab_graph:
        col_a, col_b = st.columns(2)
        with col_a:
            min_weight = st.slider("Minimum shared files", 2, 20, 3)
        with col_b:
            max_nodes = st.slider("Max nodes", 20, 300, 100)

        graph_path = BASE_DIR / "output" / "entity_graph.html"

        try:
            from pyvis.network import Network

            vip_names = {
                'jeffrey epstein', 'ghislaine maxwell', 'donald trump',
                'donald j. trump', 'bill clinton', 'prince andrew',
                'alan dershowitz', 'les wexner', 'jean-luc brunel',
                'virginia roberts', 'virginia giuffre',
            }

            # Get top PERSON entities
            top_entities = conn.execute("""
                SELECT normalized, entity_label, SUM(count) as total, COUNT(DISTINCT file_id) as files
                FROM entities WHERE entity_label = 'PERSON'
                GROUP BY normalized HAVING files >= ?
                ORDER BY files DESC LIMIT ?
            """, (min_weight, max_nodes)).fetchall()

            entity_set = {e[0] for e in top_entities}
            entity_info = {e[0]: (e[1], e[2], e[3]) for e in top_entities}

            # Force-add VIPs
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

            # Get edges
            edges = conn.execute("""
                SELECT entity_a, entity_b, file_count
                FROM entity_cooccurrence WHERE file_count >= ?
                ORDER BY file_count DESC
            """, (min_weight,)).fetchall()

            # VIP edges at lower threshold
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

            epstein_names = {'jeffrey epstein', 'epstein', 'jeffrey'}
            added = set()
            edge_count = 0

            for (a, b), w in all_edges.items():
                if a not in entity_set or b not in entity_set:
                    continue
                for node in (a, b):
                    if node not in added:
                        lt, tot, files = entity_info.get(node, ("PERSON", 1, 1))
                        if lt != "PERSON":
                            continue
                        if node in epstein_names:
                            color, size, shape = "#00ff41", 60, "diamond"
                        elif node in vip_names:
                            color = "#f1c40f"
                            size = max(25, min(8 + files * 2, 50))
                            shape = "star"
                        else:
                            color, size, shape = "#e74c3c", min(8 + files * 2, 50), "dot"
                        net.add_node(node, label=node, color=color, size=size, shape=shape,
                                     title=f"{node}\n{files} files, {tot} mentions")
                        added.add(node)
                if a in added and b in added:
                    net.add_edge(a, b, value=w, title=f"{w} shared files")
                    edge_count += 1

            graph_path.parent.mkdir(parents=True, exist_ok=True)
            net.save_graph(str(graph_path))

            st.caption(f"{len(added)} nodes, {edge_count} edges")
            with open(graph_path, 'r') as f:
                st.components.v1.html(f.read(), height=720, scrolling=True)

            st.markdown("""
            **Legend:**
            :green[**Green Diamond**] = Jeffrey Epstein  |
            :orange[**Gold Star**] = Key figures (Trump, Clinton, Prince Andrew, Maxwell, Dershowitz, etc.)  |
            :red[**Red Dot**] = Other people  |
            **Line thickness** = number of shared files
            """)

        except ImportError:
            st.error("pyvis not installed. Run: pip install pyvis")

        # ── Entity Detail Panel ──
        st.markdown("---")
        st.subheader("Explore a Person")

        # Build list of people in the graph for the selectbox
        people_in_graph = sorted(added) if 'added' in dir() and added else []
        if people_in_graph:
            selected_person = st.selectbox("Select person from graph", [""] + people_in_graph)

            if selected_person:
                # Stats
                stats = conn.execute("""
                    SELECT SUM(count), COUNT(DISTINCT file_id)
                    FROM entities WHERE normalized = ?
                """, (selected_person,)).fetchone()
                mentions, file_count = stats if stats[0] else (0, 0)

                col1, col2 = st.columns(2)
                col1.metric("Total mentions", f"{mentions:,}")
                col2.metric("Files appeared in", f"{file_count:,}")

                # Connections
                st.subheader(f"Connections: {selected_person}")
                df_connections = pd.read_sql_query("""
                    SELECT
                        CASE WHEN entity_a = ? THEN entity_b ELSE entity_a END as Connected_To,
                        file_count as Shared_Files
                    FROM entity_cooccurrence
                    WHERE entity_a = ? OR entity_b = ?
                    ORDER BY file_count DESC
                    LIMIT 50
                """, conn, params=[selected_person, selected_person, selected_person])

                if not df_connections.empty:
                    st.dataframe(df_connections, width='stretch', hide_index=True)
                else:
                    st.info("No co-occurrence connections found.")

                # Files
                st.subheader(f"Files mentioning {selected_person}")
                df_files = pd.read_sql_query("""
                    SELECT f.filename as File, f.dataset as DS, e.count as Mentions, f.rel_path as Path
                    FROM entities e JOIN files f ON f.id = e.file_id
                    WHERE e.normalized = ?
                    ORDER BY e.count DESC
                    LIMIT 100
                """, conn, params=[selected_person])

                if not df_files.empty:
                    st.dataframe(df_files, width='stretch', hide_index=True, height=400)

    # ── TAB 2: SEARCH ──
    with tab_search:
        search_mode = st.radio("Search mode", ["Person / Relationships", "Full-Text Search"], horizontal=True)

        if search_mode == "Person / Relationships":
            st.subheader("Search People & Relationships")
            person_query = st.text_input("Person name", placeholder="e.g. donald trump, les wexner, virginia")

            if person_query:
                query_lower = person_query.lower().strip()

                # Find matching entities
                df_matches = pd.read_sql_query("""
                    SELECT normalized as Name, SUM(count) as Mentions, COUNT(DISTINCT file_id) as Files
                    FROM entities WHERE entity_label = 'PERSON' AND normalized LIKE ?
                    GROUP BY normalized ORDER BY Files DESC LIMIT 20
                """, conn, params=[f"%{query_lower}%"])

                if df_matches.empty:
                    st.warning(f"No person matching '{person_query}' found in entities.")
                else:
                    st.dataframe(df_matches, width='stretch', hide_index=True)

                    # Pick the top match for relationship display
                    top_match = df_matches.iloc[0]['Name']
                    st.subheader(f"Relationships: {top_match}")

                    df_rels = pd.read_sql_query("""
                        SELECT
                            CASE WHEN entity_a = ? THEN entity_b ELSE entity_a END as Connected_To,
                            file_count as Shared_Files
                        FROM entity_cooccurrence
                        WHERE entity_a = ? OR entity_b = ?
                        ORDER BY file_count DESC
                        LIMIT 50
                    """, conn, params=[top_match, top_match, top_match])

                    if not df_rels.empty:
                        # Pie chart of connections
                        import plotly.express as px
                        fig = px.pie(
                            df_rels.head(20), values='Shared_Files', names='Connected_To',
                            title=f"Top connections for {top_match}",
                            hole=0.3,
                        )
                        fig.update_layout(
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(0,0,0,0)',
                            font_color='white',
                            height=500,
                        )
                        fig.update_traces(textinfo='label+value')
                        st.plotly_chart(fig, use_container_width=True)

                        # Select a connection to drill into
                        connection_names = df_rels['Connected_To'].tolist()
                        selected_connection = st.selectbox(
                            "Select a connection to see shared documents",
                            ["(all files for " + top_match + ")"] + connection_names,
                            key="connection_select"
                        )

                        if selected_connection.startswith("(all files"):
                            # Show docs for just the searched person
                            st.subheader(f"Documents mentioning {top_match}")
                            file_rows = conn.execute("""
                                SELECT f.id, f.filename, f.dataset, f.rel_path, tc.extracted_text
                                FROM entities e
                                JOIN files f ON f.id = e.file_id
                                JOIN text_cache tc ON tc.file_id = f.id
                                WHERE e.normalized = ?
                                ORDER BY e.count DESC
                                LIMIT 50
                            """, (top_match,)).fetchall()
                            search_highlight = query_lower
                        else:
                            # Show docs containing BOTH people
                            st.subheader(f"Documents mentioning both {top_match} & {selected_connection}")
                            file_rows = conn.execute("""
                                SELECT DISTINCT f.id, f.filename, f.dataset, f.rel_path, tc.extracted_text
                                FROM entities e1
                                JOIN entities e2 ON e1.file_id = e2.file_id
                                JOIN files f ON f.id = e1.file_id
                                JOIN text_cache tc ON tc.file_id = f.id
                                WHERE e1.normalized = ? AND e2.normalized = ?
                                LIMIT 50
                            """, (top_match, selected_connection)).fetchall()
                            search_highlight = selected_connection

                        st.caption(f"{len(file_rows)} documents found")
                        for fid, fname, ds, rel_path, text in file_rows:
                            idx = text.lower().find(search_highlight)
                            if idx >= 0:
                                start = max(0, idx - 200)
                                end = min(len(text), idx + len(search_highlight) + 200)
                                snippet = text[start:end].strip()
                            else:
                                snippet = text[:400]

                            with st.expander(f"[DS{ds}] {fname} (ID: {fid})"):
                                st.markdown(f"Path: `{rel_path}`")
                                st.markdown(f"...{snippet}...")
                                if st.button("Show full text", key=f"person_full_{fid}"):
                                    st.text_area("Full text", text, height=500, key=f"person_text_{fid}")
                    else:
                        st.info("No co-occurrence relationships found.")

        else:
            st.subheader("Full-Text Search")
            st.caption("Search across 146M+ characters of extracted text")

            search_term = st.text_input("Search term (case-insensitive)")

            if search_term and st.button("Search"):
                status = st.status(f"Searching for '{search_term}'...", expanded=True)
                results_area = st.container()

                cursor = conn.execute("""
                    SELECT f.id, f.filename, f.dataset, f.rel_path, tc.extracted_text
                    FROM text_cache tc
                    JOIN files f ON f.id = tc.file_id
                    WHERE tc.extracted_text LIKE ?
                    LIMIT 200
                """, (f"%{search_term}%",))

                hit_count = 0
                results = []
                while True:
                    rows = cursor.fetchmany(10)
                    if not rows:
                        break
                    results.extend(rows)
                    hit_count += len(rows)
                    status.update(label=f"Found {hit_count} files so far...")

                status.update(label=f"Done — {hit_count} files found", state="complete", expanded=False)

                for fid, fname, ds, rel_path, text in results:
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
                        if st.button("Show full text", key=f"full_{fid}"):
                            st.text_area("Full extracted text", text, height=500, key=f"text_{fid}")


    # ── TAB 3: METHODOLOGY / UNKNOWNS ──
    with tab_method:
        st.subheader("Brute-Force Audit Methodology")
        st.warning("**Disclaimer:** This was built quick and dirty. Some of the extracted text looks weird (OCR artifacts, encoding issues, etc.). Looking for help cleaning this up — PRs welcome on GitHub.\n\nThere are torrents circulating (check [r/DataHoarder](https://www.reddit.com/r/DataHoarder/)) that likely contain more complete collections of the Epstein files. If you have access to those, we'd welcome contributions to expand this database.")
        st.markdown("""
On January 30, 2026, the DOJ announced the release of **3.5 million pages** of Epstein files
under the Epstein Files Transparency Act. There is no manifest, no zip files for Datasets 8-11,
and no way to verify completeness unless you brute-force every possible URL.

**So that's exactly what we did.**
        """)

        st.markdown("---")
        st.subheader("How It Works")
        st.markdown("""
- Python scraper hitting every possible EFTA file ID across Datasets 8-11
- Every HTTP 200 response: download the PDF
- Every HTTP 404: logged as an empty slot
- Zero 403s across all runs = no rate limiting, these are **real gaps**
- Age gate bypass: `justiceGovAgeVerified=true` cookie
- ~40 requests/second sustained across parallel terminals
        """)

        st.markdown("---")
        st.subheader("ID Ranges")
        df_ranges = pd.DataFrame({
            'Dataset': ['8', '9', '10', '11'],
            'Start ID': ['EFTA00000001', 'EFTA00423793', 'EFTA01262782', 'EFTA02212883'],
            'End ID': ['EFTA00423792', 'EFTA01262781', 'EFTA02212882', 'EFTA02730264'],
            'Total Slots': ['423,792', '838,989', '950,101', '517,382'],
        })
        st.dataframe(df_ranges, width='stretch', hide_index=True)

        st.markdown("---")
        st.subheader("Brute-Force Results")

        df_results = pd.DataFrame({
            'Dataset': ['8', '9', '10', '11'],
            'Files Found': ['In progress', '807', '686', '408'],
            'Empty Slots (404)': ['In progress', '838,182', '895,514', '516,943'],
            'Total IDs Scanned': ['In progress', '838,989', '896,200', '517,351'],
            'Fill Rate': ['In progress', '0.096%', '0.077%', '0.079%'],
            'Status': ['Running', 'Complete (log lost)', 'Complete', 'Complete'],
        })
        st.dataframe(df_results, width='stretch', hide_index=True)

        st.markdown("---")
        st.subheader("What This Means")

        col1, col2, col3 = st.columns(3)
        col1.metric("Files Actually Found (DS 9-11)", "1,901")
        col2.metric("Total Slots Scanned (DS 9-11)", "2,252,540")
        col3.metric("Average Fill Rate", "0.084%")

        st.markdown("""
**99.92% of file slots are empty.** These aren't bad URLs — the pattern is consistent
across all completed datasets. Same sparse distribution. Same fill rate to three decimal places.

- **DOJ claimed:** 3.5 million pages
- **Actual files found so far:** 1,901 across DS 9-11 (complete), DS 8 still running
- **Projected total across DS 8-11:** ~1,900-2,000 files
        """)

        st.markdown("---")
        st.subheader("Unknowns & Open Questions")
        st.markdown("""
1. **No manifest exists** — Every legitimate data release has one. Why not this one?
2. **Dataset 8 zip is a 0-byte file, now removed** — On Jan 31, the DOJ site listed a zip download for DS8 (`DataSet 8.zip`). It downloaded successfully but was **0 bytes**. By Feb 1, even the 0-byte file was removed — the link now returns "Access Denied" in browsers and 404 via direct request. DS 9-11 have no zip links at all. Only DS 1-7 and 12 have working bulk downloads.
3. **Dataset 8 scan is running** — Re-started after terminal crash; results pending.
4. **Dataset 9 log was lost** — Terminal crashed overnight, but 807 files were downloaded successfully before it died.
5. **Active deletions documented** — MeidasTouch caught `EFTA01660679.pdf` (complaints naming Trump) going
   "Page not found" hours after release, later restored after outcry. Unknown how many others were removed.
6. **Fill rate is suspiciously uniform** — 0.077%, 0.079% across datasets. That's not random. Batch upload or batch deletion?
7. **Browser blocked, scraper not** — Mid-audit, DOJ/Akamai blocked browser access to justice.gov/epstein,
   but the Python script kept running at 39 req/s. The audit finished while the auditor was banned from the website.
        """)

        st.markdown("---")
        st.subheader("Evidence Preservation")
        st.markdown("""
- All downloaded files checksummed
- 404 patterns logged with timestamps
- Methodology documented and reproducible
- Independent verification: Reddit datahoarders hitting the same 404 wall
        """)


if __name__ == "__main__":
    main()
