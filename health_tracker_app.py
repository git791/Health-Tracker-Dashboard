import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="🏥 Public Health News Tracker",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
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

# Title
st.markdown('<h1 class="main-header">🏥 Public Health News Tracker</h1>', unsafe_allow_html=True)
st.markdown("<p style='text-align: center; font-size: 1.2rem; color: #666;'>AI-Powered Disease Outbreak Monitoring System</p>", unsafe_allow_html=True)
st.markdown("---")

# Connect to Databricks and fetch data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    """Load data from Delta Lake tables using Spark"""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
        # Set database context to avoid catalog permission issues
        spark.sql("USE default")
        
        # Load geospatial hotspots (use simple table names)
        hotspots_df = spark.sql("""
            SELECT * FROM disease_hotspots_geo
            ORDER BY severity_score DESC
        """).toPandas()
        
        # Load disease mentions
        mentions_df = spark.sql("""
            SELECT * FROM processed_disease_mentions
            ORDER BY mention_date DESC
        """).toPandas()
        
        # Load raw articles
        articles_df = spark.sql("""
            SELECT * FROM raw_news_articles
            ORDER BY scraped_at DESC
            LIMIT 100
        """).toPandas()
        
        # Load outbreak clusters
        clusters_df = spark.sql("""
            SELECT * FROM outbreak_clusters
            ORDER BY severity_score DESC
        """).toPandas()
        
        return hotspots_df, mentions_df, articles_df, clusters_df
    
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# Load data
with st.spinner("Loading data from Delta Lake..."):
    hotspots_df, mentions_df, articles_df, clusters_df = load_data()

# Check if data is available
if hotspots_df.empty:
    st.warning("⚠️ No data available. Please run the data pipeline notebook first.")
    st.stop()

# Sidebar filters
st.sidebar.header("🎛️ Filters")

# Disease filter
all_diseases = ['All'] + sorted(hotspots_df['disease'].unique().tolist())
selected_disease = st.sidebar.selectbox("Select Disease", all_diseases)

# Severity filter
min_severity = st.sidebar.slider(
    "Minimum Severity Score",
    min_value=0,
    max_value=100,
    value=0,
    step=5
)

# Apply filters
filtered_hotspots = hotspots_df.copy()
if selected_disease != 'All':
    filtered_hotspots = filtered_hotspots[filtered_hotspots['disease'] == selected_disease]
filtered_hotspots = filtered_hotspots[filtered_hotspots['severity_score'] >= min_severity]

# Sidebar info
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

# Key Metrics Row
st.header("📊 Key Metrics")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total Hotspots",
        value=len(filtered_hotspots),
        delta=f"{len(hotspots_df)} total"
    )

with col2:
    st.metric(
        label="Diseases Tracked",
        value=len(hotspots_df['disease'].unique())
    )

with col3:
    st.metric(
        label="Articles Analyzed",
        value=len(articles_df)
    )

with col4:
    avg_severity = filtered_hotspots['severity_score'].mean() if not filtered_hotspots.empty else 0
    st.metric(
        label="Avg Severity",
        value=f"{avg_severity:.1f}/100"
    )

st.markdown("---")

# Map Section
st.header("🗺️ Disease Hotspot Map")

if not filtered_hotspots.empty:
    # Create map using Plotly
    fig = go.Figure()
    
    # Add markers for each hotspot
    for _, hotspot in filtered_hotspots.iterrows():
        # Size based on severity
        marker_size = max(hotspot['severity_score'] / 2, 10)
        
        # Color based on disease
        color_map = {
            'tuberculosis': '#e74c3c',
            'dengue': '#f39c12',
            'malaria': '#9b59b6',
            'covid': '#e67e22',
            'influenza': '#3498db',
            'cholera': '#1abc9c'
        }
        marker_color = color_map.get(hotspot['disease'], '#95a5a6')
        
        fig.add_trace(go.Scattergeo(
            lon=[hotspot['longitude']],
            lat=[hotspot['latitude']],
            text=f"{hotspot['disease'].title()}<br>Location: {hotspot['location']}<br>Severity: {hotspot['severity_score']}/100",
            mode='markers',
            name=hotspot['disease'].title(),
            marker=dict(
                size=marker_size,
                color=marker_color,
                line=dict(width=2, color='white'),
                opacity=0.8
            ),
            hovertemplate="<b>%{text}</b><br>Lat: %{lat:.2f}<br>Lon: %{lon:.2f}<extra></extra>"
        ))
    
    # Update map layout
    fig.update_geos(
        scope='asia',
        center=dict(lat=20.5937, lon=78.9629),  # Center on India
        projection_scale=3,
        showcountries=True,
        countrycolor="lightgray",
        showland=True,
        landcolor="white",
        showcoastlines=True,
        coastlinecolor="gray"
    )
    
    fig.update_layout(
        height=600,
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255, 255, 255, 0.8)"
        ),
        margin={"r":0,"t":0,"l":0,"b":0}
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Legend explanation
    st.caption("💡 Marker size indicates severity score. Click legend items to show/hide specific diseases.")
else:
    st.info("No hotspots match the selected filters. Try adjusting the filters in the sidebar.")

st.markdown("---")

# Two-column layout for charts
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📈 Disease Distribution")
    
    if not filtered_hotspots.empty:
        # Disease count chart
        disease_counts = filtered_hotspots['disease'].value_counts().reset_index()
        disease_counts.columns = ['Disease', 'Count']
        
        fig_disease = px.bar(
            disease_counts,
            x='Disease',
            y='Count',
            color='Disease',
            title="Hotspots by Disease",
            labels={'Count': 'Number of Hotspots'},
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        fig_disease.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig_disease, use_container_width=True)
    else:
        st.info("No data to display")

with col_right:
    st.subheader("📍 Location Distribution")
    
    if not filtered_hotspots.empty:
        # Location count chart
        location_counts = filtered_hotspots['location'].value_counts().reset_index()
        location_counts.columns = ['Location', 'Count']
        
        fig_location = px.bar(
            location_counts,
            x='Location',
            y='Count',
            color='Location',
            title="Hotspots by Location",
            labels={'Count': 'Number of Hotspots'},
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig_location.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig_location, use_container_width=True)
    else:
        st.info("No data to display")

st.markdown("---")

# Severity Analysis
st.header("⚠️ Severity Analysis")

if not clusters_df.empty:
    # Show top clusters by severity
    st.subheader("Top 5 High-Risk Outbreak Clusters")
    
    top_clusters = clusters_df.head(5)[['cluster_id', 'disease', 'location', 'mention_count', 'severity_score']].copy()
    top_clusters['severity_score'] = top_clusters['severity_score'].apply(lambda x: f"{x}/100")
    
    # Style the dataframe
    st.dataframe(
        top_clusters,
        hide_index=True,
        use_container_width=True,
        column_config={
            "cluster_id": "Cluster ID",
            "disease": "Disease",
            "location": "Location",
            "mention_count": st.column_config.NumberColumn("Mentions", format="%d"),
            "severity_score": "Severity"
        }
    )
    
    # Severity distribution chart
    fig_severity = px.histogram(
        clusters_df,
        x='severity_score',
        nbins=10,
        title="Severity Score Distribution",
        labels={'severity_score': 'Severity Score', 'count': 'Number of Clusters'},
        color_discrete_sequence=['#e74c3c']
    )
    fig_severity.update_layout(height=300)
    st.plotly_chart(fig_severity, use_container_width=True)
else:
    st.info("No cluster data available")

st.markdown("---")

# Recent Articles Section
st.header("📰 Recent Health Articles")

if not articles_df.empty:
    # Show recent articles
    st.subheader("Latest 10 Articles")
    
    recent_articles = articles_df.head(10)[['title', 'source', 'published_date', 'url']].copy()
    recent_articles['published_date'] = pd.to_datetime(recent_articles['published_date']).dt.strftime('%Y-%m-%d %H:%M')
    
    for idx, article in recent_articles.iterrows():
        with st.expander(f"📄 {article['title']}"):
            st.write(f"**Source:** {article['source']}")
            st.write(f"**Published:** {article['published_date']}")
            st.write(f"**URL:** [{article['url']}]({article['url']})")
else:
    st.info("No articles available")

st.markdown("---")

# Footer
st.markdown("""
<div style='text-align: center; color: #666; padding: 2rem 0;'>
    <p>🏥 Public Health News Tracker | Built with Databricks & Streamlit</p>
    <p>Data updated: {}</p>
</div>
""".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')), unsafe_allow_html=True)
