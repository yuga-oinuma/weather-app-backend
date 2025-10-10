import json
import statistics
from datetime import timedelta


def get_summary(s3, bucket_name, start_date):
    city_data = {}

    for i in range(7):
        try:
            date = (start_date + timedelta(days=i)).isoformat()
            key = f"analysis-results/summary/{date}.json"
            response = s3.get_object(
                Bucket=bucket_name,
                Key=key,
            )
            body = response["Body"].read()
            records = json.loads(body)

            for record in records:
                city = record["city"]
                if city not in city_data:
                    city_data[city] = {
                        "temp_avg": [],
                        "humidity_avg": [],
                    }
                city_data[city]["temp_avg"].append(record["temp_avg"])
                city_data[city]["humidity_avg"].append(record["humidity_avg"])

        except s3.exceptions.NoSuchKey:
            print(f"スキップ: {key} が存在しません")
        except Exception as e:
            print(f"エラー: {key} の読み込みに失敗しました: {str(e)}")

    summaries = []
    for city, values in city_data.items():
        count = len(values["temp_avg"])
        if count == 0:
            continue

        temp_avg = round(statistics.mean(values["temp_avg"]), 1)
        temp_diff = round(max(values["temp_avg"]) - min(values["temp_avg"]), 1)
        temp_max = round(max(values["temp_avg"]), 1)
        temp_min = round(min(values["temp_avg"]), 1)
        humidity_avg = round(statistics.mean(values["humidity_avg"]), 1)

        summaries.append(
            {
                "city": city,
                "temp_avg": temp_avg,
                "temp_max": temp_max,
                "temp_min": temp_min,
                "temp_diff": temp_diff,
                "humidity_avg": humidity_avg,
            }
        )

    return summaries
