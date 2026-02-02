###############playlistID----> VideoIDs------> VideoData################

import json
import os
import requests

from datetime import date
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")

API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    raise RuntimeError("YOUTUBE_API_KEY not found. Check your .env file.")

maxresults = 50

############ Get Playlist ID from Channel Handle #############

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
    

############# Get Video IDs from Playlist #############

def get_video_ids(playlistId):
    video_ids = []
    pageToken = None

    try:
        while True:
            url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxresults}&playlistId={playlistId}&key={API_KEY}"
            if pageToken:
                url += f"&pageToken={pageToken}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                video_ids.append(item["contentDetails"]["videoId"])

            pageToken = data.get("nextPageToken")
            if not pageToken:
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        raise e
    
    
def batch_list(video_id_lst, batch_size=50):
    for video_id in range(0, len(video_id_lst), batch_size):
        yield video_id_lst[video_id:video_id + batch_size]
     

def extract_video_data(video_id_lst):
    extracted_data = []
    def batch_list(video_id_lst, batch_size=50):
        for video_id in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[video_id:video_id + batch_size]
            
    try:
        for batch in batch_list(video_id_lst, maxresults):
            video_ids_str = ",".join(batch)
            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            
            for item in data.get("items", []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']
                video_data = {
                    "video_id": video_id,
                    "title": snippet.get("title"),
                    "publishedAt": snippet.get("publishedAt"),
                    "duration": contentDetails.get("duration"),
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None)
                }
                extracted_data.append(video_data)
        return extracted_data
                
    except requests.exceptions.RequestException as e:
            raise e
        
        
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json" 
    with open(file_path,"w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)
        
    
if __name__ == "__main__":
    playlistId = get_playlist_id()
    print(get_video_ids(playlistId))
    video_ids = get_video_ids(playlistId)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)
    
    
