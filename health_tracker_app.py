import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ─────────────────────────────────────────────
# Page configuration
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="🏥 Public Health News Tracker",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        text-align: center;
    }
    </style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="main-header">🏥 Public Health News Tracker</h1>', unsafe_allow_html=True)
st.markdown(
    "<p style='text-align: center; font-size: 1.2rem; color: #666;'>"
    "AI-Powered Disease Outbreak Monitoring System</p>",
    unsafe_allow_html=True,
)
st.markdown("---")

# ─────────────────────────────────────────────
# Database connection via Databricks SQL Connector
# This is the correct approach for Databricks Apps.
# Do NOT use PySpark / SparkSession in Apps — it pulls a
# standalone 455 MB PyPI PySpark with no cluster access.
#
# Required env vars (set in your Databricks App configuration):
#   DATABRICKS_HOST           — e.g. adb-xxxx.azuredatabricks.net
#   DATABRICKS_TOKEN          — personal access token (or use OAuth)
#   DATABRICKS_WAREHOUSE_ID   — SQL Warehouse ID (not cluster ID)
# ─────────────────────────────────────────────

def _get_connection():
    from databricks import sql  # noqa: PLC0415

    host      = os.environ["DATABRICKS_HOST"]
    token     = os.environ.get("DATABRICKS_TOKEN", "")
    warehouse = os.environ["DATABRICKS_WAREHOUSE_ID"]

    return sql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse}",
        access_token=token,
    )


def _query(sql_text: str) -> pd.DataFrame:
    """Run a SQL query and return a pandas DataFrame."""
    with _get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_text)
            rows    = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=300, show_spinner=False)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load all four tables from Unity Catalog / Delta Lake.
    Raises on failure so the caller can surface the real error.
    Only successful results are cached.
    """
    hotspots_df = _query("""
        SELECT *
        FROM default.disease_hotspots_geo
        ORDER BY severity_score DESC
    """)

    mentions_df = _query("""
        SELECT *
        FROM default.processed_disease_mentions
        ORDER BY mention_date DESC
    """)

    articles_df = _query("""
        SELECT *
        FROM default.raw_news_articles
        ORDER BY scraped_at DESC
        LIMIT 100
    """)

    clusters_df = _query("""
        SELECT *
        FROM default.outbreak_clusters
        ORDER BY severity_score DESC
    """)

    return hotspots_df, mentions_df, articles_df, clusters_df


# ─────────────────────────────────────────────
# Load data — surface real errors to the user
# ─────────────────────────────────────────────
with st.spinner("Loading data from Delta Lake…"):
    try:
        hotspots_df, mentions_df, articles_df, clusters_df = load_data()
    except KeyError as exc:
        st.error(
            f"**Missing environment variable:** `{exc}`\n\n"
            "Make sure `DATABRICKS_HOST` and `DATABRICKS_WAREHOUSE_ID` are set "
            "in your Databricks App configuration under **Environment Variables**."
        )
        st.stop()
    except Exception as exc:
        st.error(
            f"**Failed to load data from Delta Lake.**\n\n"
            f"```\n{exc}\n```\n\n"
            "Check that:\n"
            "- The pipeline notebook has been run and tables exist in the `default` schema\n"
            "- The SQL Warehouse is running\n"
            "- `DATABRICKS_WAREHOUSE_ID` points to a SQL Warehouse (not a cluster)"
        )
        st.stop()

# Guard: tables exist but are empty
if hotspots_df.empty:
    st.warning(
        "⚠️ The `disease_hotspots_geo` table is empty. "
        "Please run the data pipeline notebook to populate it."
    )
    st.stop()

# ─────────────────────────────────────────────
# Sidebar filters
# ─────────────────────────────────────────────
st.sidebar.header("🎛️ Filters")

all_diseases = ["All"] + sorted(hotspots_df["disease"].dropna().unique().tolist())
selected_disease = st.sidebar.selectbox("Select Disease", all_diseases)

min_severity = st.sidebar.slider(
    "Minimum Severity Score",
    min_value=0,
    max_value=100,
    value=0,
    step=5,
)

filtered_hotspots = hotspots_df.copy()
if selected_disease != "All":
    filtered_hotspots = filtered_hotspots[filtered_hotspots["disease"] == selected_disease]
filtered_hotspots = filtered_hotspots[filtered_hotspots["severity_score"] >= min_severity]

st.sidebar.markdown("---")
st.sidebar.info("""
**About this Dashboard**

This app tracks disease outbreaks in India by:
- 📰 Scraping health news articles
- 🔬 Extracting disease mentions with NLP
- 🗺️ Mapping hotspots geographically
- 📊 Analyzing outbreak severity

**Data Updates:** Every 24 hours
""")

# ─────────────────────────────────────────────
# Key Metrics
# ─────────────────────────────────────────────
st.header("📊 Key Metrics")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Hotspots", len(filtered_hotspots), delta=f"{len(hotspots_df)} total")
with col2:
    st.metric("Diseases Tracked", hotspots_df["disease"].nunique())
with col3:
    st.metric("Articles Analyzed", len(articles_df) if not articles_df.empty else 0)
with col4:
    avg_severity = filtered_hotspots["severity_score"].mean() if not filtered_hotspots.empty else 0
    st.metric("Avg Severity", f"{avg_severity:.1f}/100")

st.markdown("---")

# ─────────────────────────────────────────────
# Map — one trace per disease (no duplicate legend entries)
# ─────────────────────────────────────────────
st.header("🗺️ Disease Hotspot Map")

COLOR_MAP: dict[str, str] = {
    "tuberculosis": "#e74c3c",
    "dengue":       "#f39c12",
    "malaria":      "#9b59b6",
    "covid":        "#e67e22",
    "influenza":    "#3498db",
    "cholera":      "#1abc9c",
}
DEFAULT_COLOR = "#95a5a6"

if not filtered_hotspots.empty:
    fig_map = go.Figure()

    for disease, group in filtered_hotspots.groupby("disease", sort=False):
        color = COLOR_MAP.get(disease, DEFAULT_COLOR)
        fig_map.add_trace(go.Scattergeo(
            lon=group["longitude"],
            lat=group["latitude"],
            text=(
                disease.title()
                + "<br>Location: " + group["location"].astype(str)
                + "<br>Severity: " + group["severity_score"].astype(str) + "/100"
            ),
            mode="markers",
            name=disease.title(),
            marker=dict(
                size=group["severity_score"].clip(lower=16) / 2,
                color=color,
                line=dict(width=2, color="white"),
                opacity=0.8,
            ),
            hovertemplate="<b>%{text}</b><br>Lat: %{lat:.2f}<br>Lon: %{lon:.2f}<extra></extra>",
        ))

    fig_map.update_geos(
        scope="asia",
        center=dict(lat=20.5937, lon=78.9629),
        projection_scale=3,
        showcountries=True,
        countrycolor="lightgray",
        showland=True,
        landcolor="white",
        showcoastlines=True,
        coastlinecolor="gray",
    )
    fig_map.update_layout(
        height=600,
        showlegend=True,
        legend=dict(
            yanchor="top", y=0.99,
            xanchor="left", x=0.01,
            bgcolor="rgba(255,255,255,0.8)",
        ),
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )
    st.plotly_chart(fig_map, use_container_width=True)
    st.caption("💡 Marker size indicates severity score. Click legend items to show/hide diseases.")
else:
    st.info("No hotspots match the selected filters. Try adjusting the filters in the sidebar.")

st.markdown("---")

# ─────────────────────────────────────────────
# Distribution charts
# ─────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📈 Disease Distribution")
    if not filtered_hotspots.empty:
        disease_counts = filtered_hotspots["disease"].value_counts().reset_index()
        disease_counts.columns = ["Disease", "Count"]
        fig_disease = px.bar(
            disease_counts, x="Disease", y="Count", color="Disease",
            title="Hotspots by Disease",
            labels={"Count": "Number of Hotspots"},
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_disease.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig_disease, use_container_width=True)
    else:
        st.info("No data to display.")

with col_right:
    st.subheader("📍 Location Distribution")
    if not filtered_hotspots.empty:
        location_counts = filtered_hotspots["location"].value_counts().reset_index()
        location_counts.columns = ["Location", "Count"]
        fig_location = px.bar(
            location_counts, x="Location", y="Count", color="Location",
            title="Hotspots by Location",
            labels={"Count": "Number of Hotspots"},
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig_location.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig_location, use_container_width=True)
    else:
        st.info("No data to display.")

st.markdown("---")

# ─────────────────────────────────────────────
# Severity Analysis
# ─────────────────────────────────────────────
st.header("⚠️ Severity Analysis")

if not clusters_df.empty:
    st.subheader("Top 5 High-Risk Outbreak Clusters")

    display_cols = ["cluster_id", "disease", "location", "mention_count", "severity_score"]
    display_cols = [c for c in display_cols if c in clusters_df.columns]
    top_clusters = clusters_df.head(5)[display_cols].copy()

    if "severity_score" in top_clusters.columns:
        top_clusters["severity_score"] = top_clusters["severity_score"].apply(lambda x: f"{x}/100")

    col_config = {
        "cluster_id":    "Cluster ID",
        "disease":       "Disease",
        "location":      "Location",
        "mention_count": st.column_config.NumberColumn("Mentions", format="%d"),
        "severity_score":"Severity",
    }
    st.dataframe(top_clusters, hide_index=True, use_container_width=True, column_config=col_config)

    if "severity_score" in clusters_df.columns:
        fig_severity = px.histogram(
            clusters_df, x="severity_score", nbins=10,
            title="Severity Score Distribution",
            labels={"severity_score": "Severity Score", "count": "Number of Clusters"},
            color_discrete_sequence=["#e74c3c"],
        )
        fig_severity.update_layout(height=300)
        st.plotly_chart(fig_severity, use_container_width=True)
else:
    st.info("No cluster data available.")

st.markdown("---")

# ─────────────────────────────────────────────
# Recent Articles
# ─────────────────────────────────────────────
st.header("📰 Recent Health Articles")

if not articles_df.empty:
    st.subheader("Latest 10 Articles")

    article_cols = ["title", "source", "published_date", "url"]
    article_cols = [c for c in article_cols if c in articles_df.columns]
    recent_articles = articles_df.head(10)[article_cols].copy()

    if "published_date" in recent_articles.columns:
        recent_articles["published_date"] = (
            pd.to_datetime(recent_articles["published_date"], errors="coerce")
            .dt.strftime("%Y-%m-%d %H:%M")
        )

    for _, article in recent_articles.iterrows():
        title = article.get("title", "Untitled")
        with st.expander(f"📄 {title}"):
            if "source" in article:
                st.write(f"**Source:** {article['source']}")
            if "published_date" in article:
                st.write(f"**Published:** {article['published_date']}")
            if "url" in article:
                st.write(f"**URL:** [{article['url']}]({article['url']})")
else:
    st.info("No articles available.")

st.markdown("---")

st.markdown(
    f"""
    <div style='text-align: center; color: #666; padding: 2rem 0;'>
        <p>🏥 Public Health News Tracker | Built with Databricks &amp; Streamlit</p>
        <p>Data updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    """,
    unsafe_allow_html=True,
)
