import json
from datetime import datetime, timedelta, timezone


def get_timeseries(s3, bucket_name, cities, date):
    jst = timezone(timedelta(hours=9))
    hourly_data = {}

    for city in cities:
        try:
            prefix = f"weather-data-oinuma/{city}/{date}/"
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
            )

            if "Contents" not in response:
                print(f"{city}: No files found")
                continue

            for obj in response["Contents"]:
                # JSONファイル以外は対象外
                key = obj["Key"]
                if not (".json" in key):
                    continue

                # JSONファイルを取得してデータ抽出
                file_obj = s3.get_object(Bucket=bucket_name, Key=obj["Key"])
                file_data = json.loads(file_obj["Body"].read())
                timestamp = file_data["dt"]
                temp = file_data["main"]["temp"]

                # JSTに変換して "HH:00" 形式の時刻に
                time_str = datetime.fromtimestamp(timestamp, jst).strftime("%H:00")

                if time_str not in hourly_data:
                    hourly_data[time_str] = {}
                hourly_data[time_str][city] = round(temp, 1)

        except Exception as e:
            print(f"Failed to get houry data of {city}: {e}")
            continue

    # 整形して配列に変換
    result = []
    for time in sorted(hourly_data.keys()):
        entry = {"time": time}
        entry.update(hourly_data[time])
        result.append(entry)

    return result
