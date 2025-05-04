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
df = df.dropna(subset=['coords'])

# use only 10000 rows for testing
df = df.sample(n=10000, random_state=42)

map_style = st.sidebar.selectbox(
    'Select Base Map Style',
    options=['Dark', 'Light', 'Road', 'Satellite'],
    index=0
)

df['latitude'] = df['coords'].apply(lambda x: float(x.split(',')[1]))
df['longitude'] = df['coords'].apply(lambda x: float(x.split(',')[0]))


try:
    coords = df[['latitude', 'longitude']]
    db = DBSCAN(eps=0.001, min_samples=3).fit(coords)
    
    # Add cluster labels to dataframe
    df['cluster'] = db.labels_
    
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
                    get_radius=3,
                    radius_scale=10,
                    pickable=True,
                    opacity=1,
                ),
            ],
            tooltip={
                'html': '<b>Cluster:</b> {cluster}<br><b>Price:</b> {price}',
                'style': {'color': 'white', 'backgroundColor': 'black'}
            }
        )
    )
    
    st.subheader('Clustering Analysis')

    
except Exception as e:
    st.error(f"Error in clustering analysis: {e}")