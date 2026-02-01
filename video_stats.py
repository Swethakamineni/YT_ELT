import os
import requests
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")

API_KEY = os.getenv("YOUTUBE_API_KEY", "AIzaSyBlCtMefYlFCCTTxLGiag-uy3DJj8SDIaE")

def get_playlist_id(channel_handle: str = "MrBeast") -> str:
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        #print("json.dumps(data, indent=4)")
        channel_items = data["items"][0]
        channel_playlist_id = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        print(channel_playlist_id)
        return channel_playlist_id
    except requests.exceptions.RequestException as e:
        raise e
    
if __name__ == "__main__":
    get_playlist_id()
