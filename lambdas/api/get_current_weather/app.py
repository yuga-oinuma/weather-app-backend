import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import boto3
from weather_common.response_utils import api_response
from weather_common.s3_utils import fetch_s3_json

s3 = boto3.client("s3")


def get_latest_weather_file(city, date, bucket_name):
    prefix = f"weather-data-oinuma/{city}/{date.strftime('%Y-%m-%d')}/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" not in response or len(response["Contents"]) == 0:
        return None

    latest_obj = max(
        response["Contents"],
        key=lambda obj: obj["Key"].split("/")[-1],
    )
    return latest_obj["Key"]


def lambda_handler(event, context):
    bucket_name = os.environ["BUCKET_NAME"]

    # クエリパラメータからcityを取得
    params = event.get("queryStringParameters") or {}
    city = params.get("city")
    if not city:
        return api_response(400, {"error": "Missing 'city' query parameter"})

    today = datetime.now(ZoneInfo("Asia/Tokyo")).date()

    key = get_latest_weather_file(city, today, bucket_name)

    if not key:
        yesterday = today - timedelta(days=1)
        key = get_latest_weather_file(city, yesterday, bucket_name)

    if not key:
        return api_response(
            404,
            {"error": f"No weather data found for city: {city}"},
        )

    body = fetch_s3_json(bucket_name, key, default="[]")

    return api_response(200, body, already_json=True)
