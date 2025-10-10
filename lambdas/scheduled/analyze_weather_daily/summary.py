import collections
import json
import statistics


def get_summary(s3, bucket_name, cities, date):
    summaries = []

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

            all_data = []
            for obj in response["Contents"]:
                # JSONファイル以外は対象外
                key = obj["Key"]
                if not (".json" in key):
                    continue

                # JSONファイルを取得してデータ抽出
                file_obj = s3.get_object(Bucket=bucket_name, Key=obj["Key"])
                file_data = json.loads(file_obj["Body"].read())
                all_data.append(file_data)

            if not all_data:
                print(f"{city}: No valid weather data found.")
                continue

            # 分析処理
            temps = [d["main"]["temp"] for d in all_data]
            humidities = [d["main"]["humidity"] for d in all_data]
            weather_ids = [d["weather"][0]["id"] for d in all_data]
            temp_avg = round(statistics.mean(temps), 1)
            temp_diff = round(max(temps) - min(temps), 1)
            temp_max = round(max(temps), 1)
            temp_min = round(min(temps), 1)
            humidity_avg = round(statistics.mean(humidities), 1)
            weather_id = collections.Counter(weather_ids).most_common()[0][0]
            summary = {
                "city": city,
                "temp_avg": temp_avg,
                "temp_max": temp_max,
                "temp_min": temp_min,
                "temp_diff": temp_diff,
                "humidity_avg": humidity_avg,
                "weather_id": weather_id,
            }
            summaries.append(summary)

        except Exception as e:
            print(f"Failed to get summary of {city}: {e}")
            continue

    return summaries
