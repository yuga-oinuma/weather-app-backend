import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import boto3
from summary import get_summary
from timeseries import get_timeseries


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    bucket_name = os.environ["BUCKET_NAME"]
    CITIES = [
        "Sapporo",
        "Sendai",
        "Tokyo",
        "Nagoya",
        "Kanazawa",
        "Kochi",
        "Fukuoka",
        "Naha"
    ]
    
    yesterday = (
        datetime.now(ZoneInfo("Asia/Tokyo")).date() - timedelta(days=1)
    ).isoformat()

    # 分析データ作成
    summary = get_summary(s3, bucket_name, CITIES, yesterday)
    summary_key = f"analysis-results/summary/{yesterday}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=summary_key,
        Body=json.dumps(summary, ensure_ascii=False),
        ContentType="application/json",
    )
    print(f"Saved summary data: {summary_key}")

    # 時系列データ作成
    timeseries = get_timeseries(s3, bucket_name, CITIES, yesterday)
    timeseries_key = f"analysis-results/timeseries/{yesterday}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=timeseries_key,
        Body=json.dumps(timeseries, ensure_ascii=False),
        ContentType="application/json",
    )
    print(f"Saved timeseries data: {timeseries_key}")
