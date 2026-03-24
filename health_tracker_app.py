import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(
    page_title="🏥 Public Health News Tracker",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .main-header { font-size: 3rem; font-weight: bold; color: #1f77b4; text-align: center; padding: 1rem 0; }
    .metric-card { background-color: #f0f2f6; padding: 1rem; border-radius: 0.5rem; text-align: center; }
    </style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="main-header">🏥 Public Health News Tracker</h1>', unsafe_allow_html=True)
st.markdown(
    "<p style='text-align: center; font-size: 1.2rem; color: #666;'>"
    "AI-Powered Disease Outbreak Monitoring System</p>",
    unsafe_allow_html=True,
)
st.markdown("---")


def _get_access_token() -> str:
    """
    Databricks Apps inject DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET for M2M OAuth.
    Exchange them for a bearer token via the OIDC token endpoint.
    Falls back to DATABRICKS_TOKEN (PAT) if OAuth creds aren't present.
    """
    host          = os.environ["DATABRICKS_HOST"]
    client_id     = os.environ.get("DATABRICKS_CLIENT_ID", "")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")
    pat_token     = os.environ.get("DATABRICKS_TOKEN", "")

    if client_id and client_secret:
        import requests as _req
        resp = _req.post(
            f"https://{host}/oidc/v1/token",
            data={
                "grant_type":    "client_credentials",
                "client_id":     client_id,
                "client_secret": client_secret,
                "scope":         "all-apis",
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

    if pat_token:
        return pat_token

    raise EnvironmentError(
        "No credentials found. DATABRICKS_CLIENT_ID+DATABRICKS_CLIENT_SECRET "
        "or DATABRICKS_TOKEN must be set."
    )


def _get_connection():
    from databricks import sql as dbsql
    host      = os.environ["DATABRICKS_HOST"]
    warehouse = os.environ["DATABRICKS_WAREHOUSE_ID"]
    return dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse}",
        access_token=_get_access_token(),
        _socket_timeout=60,
    )


def _query(sql_text: str) -> pd.DataFrame:
    with _get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_text)
            rows    = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=300, show_spinner=False)
def load_data():
    hotspots_df = _query("SELECT * FROM default.disease_hotspots_geo ORDER BY severity_score DESC")
    mentions_df = _query("SELECT * FROM default.processed_disease_mentions ORDER BY mention_date DESC")
    articles_df = _query("SELECT * FROM default.raw_news_articles ORDER BY scraped_at DESC LIMIT 100")
    clusters_df = _query("SELECT * FROM default.outbreak_clusters ORDER BY severity_score DESC")
    return hotspots_df, mentions_df, articles_df, clusters_df


with st.spinner("Loading data from Delta Lake…"):
    try:
        hotspots_df, mentions_df, articles_df, clusters_df = load_data()
    except KeyError as exc:
        st.error(f"**Missing environment variable:** `{exc}`")
        st.stop()
    except Exception as exc:
        import traceback
        st.error(f"**Failed to load data from Delta Lake.**\n\n```\n{traceback.format_exc()}\n```")
        st.stop()

if hotspots_df.empty:
    st.warning("⚠️ The `disease_hotspots_geo` table is empty. Run the data pipeline notebook first.")
    st.stop()

# ── Sidebar ──────────────────────────────────
st.sidebar.header("🎛️ Filters")
all_diseases = ["All"] + sorted(hotspots_df["disease"].dropna().unique().tolist())
selected_disease = st.sidebar.selectbox("Select Disease", all_diseases)
min_severity = st.sidebar.slider("Minimum Severity Score", 0, 100, 0, 5)

filtered_hotspots = hotspots_df.copy()
if selected_disease != "All":
    filtered_hotspots = filtered_hotspots[filtered_hotspots["disease"] == selected_disease]
filtered_hotspots = filtered_hotspots[filtered_hotspots["severity_score"] >= min_severity]

st.sidebar.markdown("---")
st.sidebar.info("""
**About this Dashboard**

Tracks disease outbreaks in India by:
- 📰 Scraping health news articles
- 🔬 Extracting disease mentions with NLP
- 🗺️ Mapping hotspots geographically
- 📊 Analysing outbreak severity

**Data Updates:** Every 24 hours
""")

# ── Metrics ──────────────────────────────────
st.header("📊 Key Metrics")
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Hotspots", len(filtered_hotspots), delta=f"{len(hotspots_df)} total")
with col2:
    st.metric("Diseases Tracked", hotspots_df["disease"].nunique())
with col3:
    st.metric("Articles Analyzed", len(articles_df) if not articles_df.empty else 0)
with col4:
    avg = filtered_hotspots["severity_score"].mean() if not filtered_hotspots.empty else 0
    st.metric("Avg Severity", f"{avg:.1f}/100")

st.markdown("---")

# ── Map ──────────────────────────────────────
st.header("🗺️ Disease Hotspot Map")
COLOR_MAP = {
    "tuberculosis": "#e74c3c", "dengue": "#f39c12", "malaria": "#9b59b6",
    "covid": "#e67e22", "influenza": "#3498db", "cholera": "#1abc9c",
}

if not filtered_hotspots.empty:
    fig_map = go.Figure()
    for disease, group in filtered_hotspots.groupby("disease", sort=False):
        fig_map.add_trace(go.Scattergeo(
            lon=group["longitude"], lat=group["latitude"],
            text=(disease.title() + "<br>Location: " + group["location"].astype(str)
                  + "<br>Severity: " + group["severity_score"].astype(str) + "/100"),
            mode="markers", name=disease.title(),
            marker=dict(
                size=group["severity_score"].clip(lower=16) / 2,
                color=COLOR_MAP.get(disease, "#95a5a6"),
                line=dict(width=2, color="white"), opacity=0.8,
            ),
            hovertemplate="<b>%{text}</b><br>Lat: %{lat:.2f}<br>Lon: %{lon:.2f}<extra></extra>",
        ))
    fig_map.update_geos(
        scope="asia", center=dict(lat=20.5937, lon=78.9629), projection_scale=3,
        showcountries=True, countrycolor="lightgray",
        showland=True, landcolor="white", showcoastlines=True, coastlinecolor="gray",
    )
    fig_map.update_layout(
        height=600, showlegend=True,
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, bgcolor="rgba(255,255,255,0.8)"),
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )
    st.plotly_chart(fig_map, use_container_width=True)
    st.caption("💡 Marker size indicates severity score.")
else:
    st.info("No hotspots match the selected filters.")

st.markdown("---")

# ── Distribution charts ───────────────────────
col_left, col_right = st.columns(2)
with col_left:
    st.subheader("📈 Disease Distribution")
    if not filtered_hotspots.empty:
        dc = filtered_hotspots["disease"].value_counts().reset_index()
        dc.columns = ["Disease", "Count"]
        fig = px.bar(dc, x="Disease", y="Count", color="Disease",
                     title="Hotspots by Disease", color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data to display.")

with col_right:
    st.subheader("📍 Location Distribution")
    if not filtered_hotspots.empty:
        lc = filtered_hotspots["location"].value_counts().reset_index()
        lc.columns = ["Location", "Count"]
        fig = px.bar(lc, x="Location", y="Count", color="Location",
                     title="Hotspots by Location", color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data to display.")

st.markdown("---")

# ── Severity Analysis ─────────────────────────
st.header("⚠️ Severity Analysis")
if not clusters_df.empty:
    st.subheader("Top 5 High-Risk Outbreak Clusters")
    display_cols = [c for c in ["cluster_id","disease","location","mention_count","severity_score"] if c in clusters_df.columns]
    top = clusters_df.head(5)[display_cols].copy()
    if "severity_score" in top.columns:
        top["severity_score"] = top["severity_score"].apply(lambda x: f"{x}/100")
    st.dataframe(top, hide_index=True, use_container_width=True)

    if "severity_score" in clusters_df.columns:
        fig = px.histogram(clusters_df, x="severity_score", nbins=10,
                           title="Severity Score Distribution",
                           color_discrete_sequence=["#e74c3c"])
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No cluster data available.")

st.markdown("---")

# ── Recent Articles ───────────────────────────
st.header("📰 Recent Health Articles")
if not articles_df.empty:
    article_cols = [c for c in ["title","source","published_date","url"] if c in articles_df.columns]
    recent = articles_df.head(10)[article_cols].copy()
    if "published_date" in recent.columns:
        recent["published_date"] = pd.to_datetime(recent["published_date"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
    for _, art in recent.iterrows():
        with st.expander(f"📄 {art.get('title','Untitled')}"):
            if "source" in art:       st.write(f"**Source:** {art['source']}")
            if "published_date" in art: st.write(f"**Published:** {art['published_date']}")
            if "url" in art:          st.write(f"**URL:** [{art['url']}]({art['url']})")
else:
    st.info("No articles available.")

st.markdown("---")
st.markdown(
    f"<div style='text-align:center;color:#666;padding:2rem 0;'>"
    f"<p>🏥 Public Health News Tracker | Built with Databricks &amp; Streamlit</p>"
    f"<p>Data updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p></div>",
    unsafe_allow_html=True,
)
