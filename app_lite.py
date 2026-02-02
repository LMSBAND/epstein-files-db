#!/usr/bin/env python3
"""Streamlit lite app — relationship graph + person search (no full-text)."""

import sqlite3
import pandas as pd
import streamlit as st
from pathlib import Path
import tempfile

DB_PATH = Path(__file__).parent / "epstein_lite.db"


@st.cache_resource
def get_db():
    if not DB_PATH.exists():
        st.error("epstein_lite.db not found.")
        st.stop()
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    return conn



def main():
    st.set_page_config(page_title="Epstein Files DB", layout="wide")
    conn = get_db()

    total_files = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    total_ents = conn.execute("SELECT COUNT(DISTINCT normalized) FROM entities").fetchone()[0]
    cooccur_edges = conn.execute("SELECT COUNT(*) FROM entity_cooccurrence").fetchone()[0]

    st.title("Epstein Files DB")
    st.caption(f"{total_files:,} files | {total_ents:,} people identified | {cooccur_edges:,} relationship edges")
    st.caption("This is the lite version — graph & search only. [Full version + database download on GitHub](https://github.com/LMSBAND/epstein-files-db)")
    st.caption("*Incomplete dataset* — *still completely disgusting*")
    st.caption("For Minnesota, with love.")

    tab_graph, tab_search = st.tabs(["Relationship Graph", "Search People"])

    # ── TAB 1: RELATIONSHIP GRAPH ──
    with tab_graph:
        col_a, col_b = st.columns(2)
        with col_a:
            min_weight = st.slider("Minimum shared files", 2, 20, 3)
        with col_b:
            max_nodes = st.slider("Max nodes", 20, 300, 100)

        try:
            from pyvis.network import Network

            vip_names = {
                'jeffrey epstein', 'ghislaine maxwell', 'donald trump',
                'donald j. trump', 'bill clinton', 'prince andrew',
                'alan dershowitz', 'les wexner', 'jean-luc brunel',
                'virginia roberts', 'virginia giuffre',
            }

            top_entities = conn.execute("""
                SELECT normalized, entity_label, SUM(count) as total, COUNT(DISTINCT file_id) as files
                FROM entities
                GROUP BY normalized HAVING files >= ?
                ORDER BY files DESC LIMIT ?
            """, (min_weight, max_nodes)).fetchall()

            entity_set = {e[0] for e in top_entities}
            entity_info = {e[0]: (e[1], e[2], e[3]) for e in top_entities}

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

            edges = conn.execute("""
                SELECT entity_a, entity_b, file_count
                FROM entity_cooccurrence WHERE file_count >= ?
                ORDER BY file_count DESC
            """, (min_weight,)).fetchall()

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

            graph_file = Path(tempfile.gettempdir()) / "entity_graph.html"
            net.save_graph(str(graph_file))

            st.caption(f"{len(added)} nodes, {edge_count} edges")
            with open(graph_file, 'r') as f:
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

        # ── Explore a Person from graph ──
        st.markdown("---")
        st.subheader("Explore a Person")

        people_in_graph = sorted(added) if 'added' in dir() and added else []
        if people_in_graph:
            selected_person = st.selectbox("Select person from graph", [""] + people_in_graph)

            if selected_person:
                stats = conn.execute("""
                    SELECT SUM(count), COUNT(DISTINCT file_id)
                    FROM entities WHERE normalized = ?
                """, (selected_person,)).fetchone()
                mentions, file_count = stats if stats[0] else (0, 0)

                col1, col2 = st.columns(2)
                col1.metric("Total mentions", f"{mentions:,}")
                col2.metric("Files appeared in", f"{file_count:,}")

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

    # ── TAB 2: SEARCH PEOPLE ──
    with tab_search:
        st.subheader("Search People & Relationships")
        person_query = st.text_input("Person name", placeholder="e.g. donald trump, les wexner, virginia")

        if person_query:
            query_lower = person_query.lower().strip()

            df_matches = pd.read_sql_query("""
                SELECT normalized as Name, SUM(count) as Mentions, COUNT(DISTINCT file_id) as Files
                FROM entities WHERE normalized LIKE ?
                GROUP BY normalized ORDER BY Files DESC LIMIT 20
            """, conn, params=[f"%{query_lower}%"])

            if df_matches.empty:
                st.warning(f"No person matching '{person_query}' found.")
            else:
                st.dataframe(df_matches, width='stretch', hide_index=True)

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

                    # File list for the searched person
                    st.subheader(f"Files mentioning {top_match}")
                    df_files = pd.read_sql_query("""
                        SELECT f.filename as File, f.dataset as DS, e.count as Mentions, f.rel_path as Path
                        FROM entities e JOIN files f ON f.id = e.file_id
                        WHERE e.normalized = ?
                        ORDER BY e.count DESC
                        LIMIT 100
                    """, conn, params=[top_match])

                    if not df_files.empty:
                        st.dataframe(df_files, width='stretch', hide_index=True, height=400)

                        # Download as CSV
                        csv = df_files.to_csv(index=False)
                        st.download_button(
                            f"Download file list for {top_match} (.csv)",
                            csv, f"{top_match.replace(' ', '_')}_files.csv",
                            "text/csv"
                        )
                else:
                    st.info("No co-occurrence relationships found.")


if __name__ == "__main__":
    main()
