import spotipy
from spotipy.oauth2 import SpotifyOAuth
import psycopg2
from prefect import flow
from prefect.server.schemas.schedules import CronSchedule

def fetch_recent_tracks():
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id="b5841c20276a494c9740652dfb4ca360",
        client_secret="d014b724b65945e8bb382b587a493592",
        redirect_uri="http://127.0.0.1/callback",
        scope="user-read-recently-played"
    ))

    results = sp.current_user_recently_played(limit=20)
    return results["items"]

def store_recent_tracks(items, users_id):
    conn = psycopg2.connect(
        dbname="spotify_app",
        user="postgres",
        password="password",
        host="localhost"
    )
    cursor = conn.cursor()

    for item in items:
        track = item["track"]
        cursor.execute("""
        INSERT INTO played_tracks 
        (user_id, track_id, track_name, artist_name,artist_id, played_at)
        VALUES (%s, %s, %s, %s, %s, %s);
                       """, (
        users_id,
        track["id"],
        track["name"],
        track["artists"][0]["name"],
        track["artists"][0]["id"],  
        item["played_at"]
))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
#def begin_report():
    users_id = "n6pxrc8vot90kipy4b9b5yd2r"

    items = fetch_recent_tracks()
    store_recent_tracks(items, users_id)

    print("Recent track history stored successfully.")


@flow(name="Spotify ETL Flow")
def spotify_etl_run():
    user_id="n6pxrc8vot90kipy4b9b5yd2r"
    items = fetch_recent_tracks()
    store_recent_tracks(items, user_id = "n6pxrc8vot90kipy4b9b5yd2r")