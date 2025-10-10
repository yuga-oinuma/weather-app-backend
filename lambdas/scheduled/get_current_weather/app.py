import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
import requests


def save_to_s3(bucket_name, key, data):
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data),
    )


def lambda_handler(event, context):
    # APIキー取得
    api_key = os.environ["API_KEY"]

    CITIES = [
        {"name": "Sapporo", "lat": 43.0621, "lon": 141.3544},
        {"name": "Sendai", "lat": 38.2682, "lon": 140.8694},
        {"name": "Tokyo", "lat": 35.6812, "lon": 139.7671},
        {"name": "Nagoya", "lat": 35.1815, "lon": 136.9066},
        {"name": "Kanazawa", "lat": 36.5613, "lon": 136.6562},
        {"name": "Kochi", "lat": 33.5597, "lon": 133.5311},
        {"name": "Fukuoka", "lat": 33.5904, "lon": 130.4017},
        {"name": "Naha", "lat": 26.2124, "lon": 127.6809},
    ]
    bucket_name = os.environ["BUCKET_NAME"]
    today = datetime.now(ZoneInfo("Asia/Tokyo")).date().isoformat()

    for city in CITIES:
        try:
            # 気象データ取得
            response = requests.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={
                    "lat": city["lat"],
                    "lon": city["lon"],
                    "appid": api_key,
                    "units": "metric",
                },
            )
            response.raise_for_status()

            response_dict = response.json()
            unix = response_dict["dt"]
            key = f"weather-data-oinuma/{city['name']}/{today}/{unix}.json"
            save_to_s3(bucket_name, key, response_dict)
            print(f"Saved data for {city['name']} to {key}")

        except Exception as e:
            print(f"Failed to fetch or store data for {city['name']}: {e}")
