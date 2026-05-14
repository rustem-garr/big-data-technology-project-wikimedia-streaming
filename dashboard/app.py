import subprocess
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Wikimedia Streaming Dashboard", layout="wide")
st.caption("Implemented by Rustem & Thiha")

CONTAINER_NAME = "cs523bdt-lab"


@st.cache_data(ttl=300)
def run_hive_query(query: str) -> str:
    cmd = [
        "docker",
        "exec",
        CONTAINER_NAME,
        "hive",
        "-S",
        "-e",
        query
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()

    if result.returncode != 0:
        raise RuntimeError(stderr or "Hive query failed")

    return stdout


@st.cache_data(ttl=300)
def query_to_df(query: str, columns: list[str]) -> pd.DataFrame:
    output = run_hive_query(query)
    if not output:
        return pd.DataFrame(columns=columns)

    rows = []
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) < len(columns):
            parts += [""] * (len(columns) - len(parts))
        elif len(parts) > len(columns):
            parts = parts[:len(columns)]
        rows.append(parts)

    return pd.DataFrame(rows, columns=columns)


st.title("Real-Time Wikimedia Streaming Analytics")
st.caption("Kafka -> Spark Structured Streaming -> HDFS Parquet -> Hive -> Streamlit")

col1, col2 = st.columns(2)

with col1:
    try:
        total_rows_output = run_hive_query("""
        USE wikimedia_analytics;
        SELECT COUNT(*) FROM event_counts_external;
        """)
        total_rows = int(total_rows_output.splitlines()[-1]) if total_rows_output else 0
    except Exception as e:
        total_rows = 0
        st.error(f"Failed to fetch total rows: {e}")

    st.metric("Total Processed Aggregate Rows", total_rows)

with col2:
    try:
        latest_window_output = run_hive_query("""
        USE wikimedia_analytics;
        SELECT MAX(window_end) FROM event_counts_external;
        """)
        latest_window = latest_window_output.splitlines()[-1] if latest_window_output else "N/A"
    except Exception as e:
        latest_window = "N/A"
        st.error(f"Failed to fetch latest window: {e}")

    st.metric("Latest Window End", latest_window)

st.subheader("Latest Aggregated Rows")
try:
    latest_df = query_to_df("""
    USE wikimedia_analytics;
    SELECT window_start, window_end, wiki, type, bot, count
    FROM event_counts_external
    ORDER BY window_end DESC, count DESC
    LIMIT 20;
    """, ["window_start", "window_end", "wiki", "type", "bot", "count"])
    st.dataframe(latest_df, use_container_width=True)
except Exception as e:
    st.error(f"Failed to load latest aggregated rows: {e}")

col3, col4 = st.columns(2)

with col3:
    st.subheader("Top Wikis by Event Count")
    try:
        top_wikis_df = query_to_df("""
        USE wikimedia_analytics;
        SELECT wiki, SUM(count) AS total_count
        FROM event_counts_external
        GROUP BY wiki
        ORDER BY total_count DESC
        LIMIT 10;
        """, ["wiki", "total_count"])

        if not top_wikis_df.empty:
            top_wikis_df["total_count"] = pd.to_numeric(top_wikis_df["total_count"], errors="coerce").fillna(0)
            st.bar_chart(top_wikis_df.set_index("wiki"))
        else:
            st.info("No data available yet.")
    except Exception as e:
        st.error(f"Failed to load top wikis: {e}")

with col4:
    st.subheader("Bot vs Human Activity")
    try:
        bot_df = query_to_df("""
        USE wikimedia_analytics;
        SELECT CAST(bot AS STRING) AS bot_flag, SUM(count) AS total_count
        FROM event_counts_external
        GROUP BY bot
        ORDER BY total_count DESC;
        """, ["bot_flag", "total_count"])

        if not bot_df.empty:
            bot_df["total_count"] = pd.to_numeric(bot_df["total_count"], errors="coerce").fillna(0)
            st.bar_chart(bot_df.set_index("bot_flag"))
        else:
            st.info("No data available yet.")
    except Exception as e:
        st.error(f"Failed to load bot vs human activity: {e}")

st.subheader("Event Type Distribution")
try:
    type_df = query_to_df("""
    USE wikimedia_analytics;
    SELECT type, SUM(count) AS total_count
    FROM event_counts_external
    GROUP BY type
    ORDER BY total_count DESC;
    """, ["type", "total_count"])

    if not type_df.empty:
        type_df["total_count"] = pd.to_numeric(type_df["total_count"], errors="coerce").fillna(0)
        st.bar_chart(type_df.set_index("type"))
    else:
        st.info("No data available yet.")
except Exception as e:
    st.error(f"Failed to load event type distribution: {e}")

st.markdown("---")
st.write("Data source: Wikimedia recent changes stream processed through Kafka, Spark Structured Streaming, HDFS Parquet, and Hive.")
