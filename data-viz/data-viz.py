import streamlit as st
import pandas as pd
import numpy as np
from sklearn.datasets import load_iris
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt


# Page configuration
st.set_page_config(layout="wide")
st.title('Bangkok Traffy Data Visualization')

# Load and prepare data
@st.cache_data
def load_data(input_filepath):
    df = pd.read_csv(input_filepath)
    return df

MAP_STYLES = {
    'Dark': 'mapbox://styles/mapbox/dark-v10',
    'Light': 'mapbox://styles/mapbox/light-v10',
    'Road': 'mapbox://styles/mapbox/streets-v11',
    'Satellite': 'mapbox://styles/mapbox/satellite-v9'
}

df = load_data('bangkok_traffy.csv')

# drop NaN coords
df = df.dropna(subset=['coords', 'timestamp', 'last_activity', 'state'])

#filter out rows which timestamp >= last_activity
df = df[df['timestamp'] < df['last_activity']]

# drop rows with state is not 'เสร็จสิ้น'
df = df[df['state'] == 'เสร็จสิ้น']

def toDate(serie):
    return pd.to_datetime(serie, format='ISO8601').dt.tz_localize(None)

df['timestamp'] = toDate(df['timestamp'])
df['last_activity'] = toDate(df['last_activity'])

df['duration'] = (df['last_activity'] - df['timestamp']).dt.total_seconds() // 60

# remove those with duration > 20k
df = df[df['duration'] <= 20000]

# use only 10000 rows for testing
df = df.sample(n=10000, random_state=42)

map_style = st.sidebar.selectbox(
    'Select Base Map Style',
    options=['Dark', 'Light', 'Road', 'Satellite'],
    index=0
)

df['latitude'] = df['coords'].apply(lambda x: float(x.split(',')[1]))
df['longitude'] = df['coords'].apply(lambda x: float(x.split(',')[0]))


st.write(df)

#plot duration histogram
st.subheader("Duration Histogram")
fig = px.histogram(df, x='duration', nbins=100, title='Duration Histogram')
fig.update_layout(
    xaxis_title='Duration (minutes)',
    yaxis_title='Count',
    xaxis=dict(
        tickmode='linear',
        dtick=1000
    )
)
st.plotly_chart(fig, use_container_width=True)

try:
    duration = df[['duration']]
    kmeans = KMeans(n_clusters=5, random_state=77, n_init='auto')
    kmeans.fit(duration)
    
    # Add cluster labels to dataframe
    df['cluster'] = kmeans.labels_
    
    # Analyze clusters
    clusters_count = df['cluster'].value_counts()
    clusters_count = clusters_count[clusters_count.index != -1]  # Exclude noise points
    top_clusters = clusters_count.head(10)
    
    # Generate colors for clusters
    unique_clusters = df[df['cluster'].isin(top_clusters.index)]['cluster'].unique()
    colormap = plt.get_cmap('hsv')
    cluster_colors = {cluster: [int(x*255) for x in colormap(i/len(unique_clusters))[:3]] + [160] 
                     for i, cluster in enumerate(unique_clusters)}
    
    # Create visualization dataframe
    viz_data = df[df['cluster'].isin(top_clusters.index)].copy()
    viz_data['color'] = viz_data['cluster'].map(cluster_colors)
    
    view_state = pdk.ViewState(
        latitude=df['latitude'].mean(),
        longitude=df['longitude'].mean(),
        zoom=12,
        pitch=0,
    )

    st.pydeck_chart(
        pdk.Deck(
            map_style=MAP_STYLES[map_style],
            initial_view_state=pdk.ViewState(
                latitude=df['latitude'].mean(),
                longitude=df['longitude'].mean(),
                zoom=12,
                pitch=0,
            ),
            layers=[
                pdk.Layer(
                    'ScatterplotLayer',
                    data=viz_data,
                    get_position='[longitude, latitude]',
                    get_fill_color='color',
                    get_radius=10,
                    radius_scale=10,
                    pickable=True,
                    opacity=1,
                ),
            ],
            tooltip={
                'html': '<b>Cluster:</b> {cluster}<br><b>ticket_id :</b> {ticket_id}',
                'style': {'color': 'white', 'backgroundColor': 'black'}
            }
        )
    )
    
    # Analyze duration statistics per cluster
    cluster_profiles = df.groupby('cluster')['duration'].describe()
    st.write("Cluster Duration Profiles")
    st.dataframe(cluster_profiles)

    total = 0
    st.markdown("### Cluster Legend")
    
    for cluster, count in clusters_count.items():
        if cluster == -1:
            continue
        total += count
        min_duration = df[df['cluster'] == cluster]['duration'].min()
        max_duration = df[df['cluster'] == cluster]['duration'].max()
        cluster_color = f"rgb({','.join(map(str, cluster_colors[cluster][:3]))})"
        st.markdown(f"<span style='color:{cluster_color};'>⬤</span> Cluster {cluster} : {min_duration} - {max_duration} ({count} cases)", unsafe_allow_html=True)

    st.markdown(f"Total: {total} cases")
    
except Exception as e:
    st.error(f"Error in clustering analysis: {e}")