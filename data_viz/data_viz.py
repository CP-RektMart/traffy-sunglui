import streamlit as st
import pandas as pd
from sklearn.cluster import KMeans
import plotly.express as px
import pydeck as pdk
import matplotlib.pyplot as plt
from itertools import chain
import colorsys
from client import client

# Page configuration
st.set_page_config(layout="wide")
st.title('Bangkok Traffy Data Visualization')

# Load and prepare data
@st.cache_data
def load_data():
    query = """
        SELECT *
        FROM `dsde-458712.bkk_traffy_fondue.traffy_fondue_data`
        WHERE PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%E6S%Ez\', timestamp) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 100 HOUR)
        ORDER BY PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%E6S%Ez\', timestamp) DESC
    """

    return client.query(query).to_dataframe()

MAP_STYLES = {
    'Dark': 'mapbox://styles/mapbox/dark-v10',
    'Light': 'mapbox://styles/mapbox/light-v10',
    'Road': 'mapbox://styles/mapbox/streets-v11',
    'Satellite': 'mapbox://styles/mapbox/satellite-v9'
}

# df = load_data('bangkok_traffy.csv')

df = load_data()

# drop NaN coords
df = df.dropna(subset=['coords', 'timestamp', 'last_activity', 'state','type'])

# drop rows with type = {}
df = df[df['type'] != '{}']

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
# df = df.sample(n=10000, random_state=42)

map_style = st.sidebar.selectbox(
    'Select Base Map Style',
    options=['Dark', 'Light', 'Road', 'Satellite'],
    index=0
)

df['latitude'] = df['coords'].apply(lambda x: float(x.split(',')[1]))
df['longitude'] = df['coords'].apply(lambda x: float(x.split(',')[0]))


st.write(df)
st.write(df.dtypes)

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
    
    st.header("Distribution by time duration used")
    
    # Step 1: Sort clusters by min duration
    # Compute min duration per cluster
    min_durations = df.groupby('cluster')['duration'].min()

    # Sort clusters by min duration
    sorted_clusters = min_durations.sort_values().index.tolist()

    # Create a mapping from cluster number to rank (starting from 1)
    cluster_ranks = {cluster: rank+1 for rank, cluster in enumerate(sorted_clusters)}


    # Step 2: Add multiselect box to choose clusters to display
    selected_clusters = []
    st.markdown("### Select Clusters to Display")

    for cluster in sorted_clusters:
        if cluster == -1:
            continue  # skip noise
        rank = cluster_ranks[cluster]
        checkbox_label = f"Cluster {rank}"
        if st.checkbox(checkbox_label, value=True, key=f"cluster_{cluster}"):
            selected_clusters.append(cluster)

    # Step 3: Filter data for selected clusters
    filtered_df = df[df['cluster'].isin(selected_clusters)]
    filtered_viz_data = viz_data[viz_data['cluster'].isin(selected_clusters)]
    
    viz_data['cluster_rank'] = viz_data['cluster'].map(cluster_ranks)
    
    filtered_viz_data = viz_data[viz_data['cluster'].isin(selected_clusters)]

    # Step 4: Display map
    st.pydeck_chart(
        pdk.Deck(
            map_style=MAP_STYLES[map_style],
            initial_view_state=view_state,
            layers=[
                pdk.Layer(
                    'ScatterplotLayer',
                    data=filtered_viz_data,
                    get_position='[longitude, latitude]',
                    get_fill_color='color',
                    get_radius=10,
                    radius_scale=10,
                    pickable=True,
                    opacity=1,
                ),
            ],
            tooltip={
                'html': '<b>Cluster:</b> {cluster_rank}<br><b>ticket_id :</b> {ticket_id}',
                'style': {'color': 'white', 'backgroundColor': 'black'}
            }
        )
    )

    # Step 5: Show duration stats
    cluster_profiles = filtered_df.groupby('cluster')['duration'].describe()
    st.write("Cluster Duration Profiles")
    st.dataframe(cluster_profiles)

    # Step 6: Cluster legend
    st.markdown("### Cluster Legend")
    total = 0

    for cluster in sorted_clusters:
        if cluster == -1:
            continue
        if cluster not in clusters_count:
            continue  # in case some clusters were filtered out

        rank = cluster_ranks[cluster]
        count = clusters_count[cluster]
        total += count
        min_duration = df[df['cluster'] == cluster]['duration'].min()
        max_duration = df[df['cluster'] == cluster]['duration'].max()
        cluster_color = f"rgb({','.join(map(str, cluster_colors[cluster][:3]))})"
        st.markdown(
            f"<span style='color:{cluster_color};'>⬤</span> Cluster {rank} : {min_duration} - {max_duration} hr ({count} cases)",
            unsafe_allow_html=True
        )


    st.markdown(f"Total : {total} cases")
    
except Exception as e:
    st.error(f"Error in clustering analysis: {e}")
    
# --------------------------------------------------------------------------    

# plot another map, now classifying by type

# convert type to list
df['parsed_types'] = df['type'].apply(
    lambda x: set(item.strip() for item in x.strip('{}').split(',') if item.strip())
)

# see all unique types
all_types = sorted(set(chain.from_iterable(df['parsed_types'])))
df['primary_type'] = df['parsed_types'].apply(lambda x: sorted(list(x))[0])
def generate_hsv_colors(n):
    return [
        [int(c * 255) for c in colorsys.hsv_to_rgb(i / n, 0.65, 0.95)] + [160]
        for i in range(n)
    ]

type_colors = {
    t: color for t, color in zip(all_types, generate_hsv_colors(len(all_types)))
}

df['color'] = df['primary_type'].map(type_colors)

viz_data = df[['latitude', 'longitude', 'color', 'ticket_id', 'primary_type']]


try:
    
    st.header("Distribution by type of case")
    
    # create a checkbox for each type
    st.markdown("### Select Types to Display")
    cols = st.columns(4)  # Create 4 columns
    selected_types = []

    for i, t in enumerate(all_types):
        col = cols[i % 4]  # Distribute checkboxes evenly across columns
        checkbox_label = f"Type: {t}"
        if col.checkbox(checkbox_label, value=True, key=f"type_{t}"):
            selected_types.append(t)
            
            
    # filter data for selected types
    filtered_viz_data = viz_data[viz_data['primary_type'].isin(selected_types)]
    filtered_viz_data['color'] = filtered_viz_data['color'].apply(
        lambda x: [int(c) for c in x]
    )
    
    st.pydeck_chart(pdk.Deck(
        map_style=MAP_STYLES[map_style],
        initial_view_state=view_state,
        layers=[
            pdk.Layer(
                "ScatterplotLayer",
                data=filtered_viz_data,
                get_position='[longitude, latitude]',
                get_fill_color='color',
                get_radius=100,
                pickable=True,
            )
        ],
        tooltip={
            'html': '<b>Type:</b> {primary_type}<br><b>ID:</b> {ticket_id}',
            'style': {'color': 'white'}
        }
    ))
    
    # Show type counts
    total_count = 0
    st.markdown("### Type Counts")
    
    st.markdown(f"Number of categories : {len(all_types)} categories")

    cols = st.columns(4)  # Create 4 columns
    total_count = 0
    no_of_rows = len(all_types) // 4 + (len(all_types) % 4 > 0)

    for i, t in enumerate(all_types):
        if t not in type_colors:
            continue  # Skip filtered-out types

        count = df[df['primary_type'] == t].shape[0]
        total_count += count
        type_color = f"rgb({','.join(map(str, type_colors[t][:3]))})"
        
        # Display in the appropriate column
        cols[i // no_of_rows].markdown(
            f"<span style='color:{type_color};'>⬤</span> {t}: {count} cases",
            unsafe_allow_html=True
        )

    st.markdown(f"Total : {total_count} cases")
    
except Exception as e:
    st.error(f"Error in type visualization: {e}")





