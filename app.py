import streamlit as st
import pandas as pd
import time

from sqlalchemy import create_engine
st.set_page_config(page_title="Spotify ETL Dashboard")

# DB connection function
def get_data():
    engine = create_engine("postgresql+psycopg2://postgres:password@localhost:5432/spotify_app")

    query = """
    SELECT u.display_name, p.track_name, p.artist_name, p.played_at
    FROM played_tracks p
    JOIN users u ON p.user_id = u.user_id
    ORDER BY p.played_at DESC
    LIMIT 20;
    """

    df = pd.read_sql(query, engine) 
    engine.dispose()
    return df

st.title("🎧 Spotify Listening Dashboard")

data = get_data()

st.subheader("Most Recent Plays")

st.dataframe(data)

# Access the query parameters
params = st.query_params
timestamp = params.get("timestamp", [""])[0]  # get 'timestamp' param
st.write("Timestamp:", timestamp)
st.rerun()


