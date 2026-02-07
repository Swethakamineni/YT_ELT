############### playlistID -> VideoIDs -> VideoData ################

import json
import os
import requests
from datetime import date

from airflow.decorators import task
from airflow.models import Variable

MAX_RESULTS = 50

def chunked(lst, size=50):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


@task
def get_playlist_id(channel_handle: str | None = None) -> str:
    api_key = Variable.get("YOUTUBE_API_KEY", default_var=None)
    if not api_key:
        raise ValueError("Missing Airflow Variable: YOUTUBE_API_KEY")

    if channel_handle is None:
        channel_handle = Variable.get("CHANNEL_HANDLE", default_var="MrBeast")

    url = (
        "https://youtube.googleapis.com/youtube/v3/channels"
        f"?part=contentDetails&forHandle={channel_handle}&key={api_key}"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    return data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


@task
def get_video_ids(playlist_id: str):
    api_key = Variable.get("YOUTUBE_API_KEY")

    video_ids = []
    page_token = None

    while True:
        url = (
            "https://youtube.googleapis.com/youtube/v3/playlistItems"
            f"?part=contentDetails&maxResults={MAX_RESULTS}&playlistId={playlist_id}&key={api_key}"
        )
        if page_token:
            url += f"&pageToken={page_token}"

        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        for item in data.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return video_ids


@task
def extract_video_data(video_id_list):
    api_key = Variable.get("YOUTUBE_API_KEY")

    extracted = []
    for batch in chunked(video_id_list, MAX_RESULTS):
        video_ids_str = ",".join(batch)
        url = (
            "https://youtube.googleapis.com/youtube/v3/videos"
            f"?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={api_key}"
        )

        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            content_details = item.get("contentDetails", {})
            statistics = item.get("statistics", {})

            extracted.append(
                {
                    "video_id": item.get("id"),
                    "title": snippet.get("title"),
                    "publishedAt": snippet.get("publishedAt"),
                    "duration": content_details.get("duration"),
                    "viewCount": statistics.get("viewCount"),
                    "likeCount": statistics.get("likeCount"),
                    "commentCount": statistics.get("commentCount"),
                }
            )

    return extracted


@task
def save_to_json(extracted_data):
    # Prefer a stable path inside the container
    out_dir = "/opt/airflow/data"
    os.makedirs(out_dir, exist_ok=True)

    file_path = f"{out_dir}/YT_data_{date.today()}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(extracted_data, f, indent=4, ensure_ascii=False)

    return file_path
