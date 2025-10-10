import json
from datetime import timedelta


def get_timeseries(s3, bucket_name, start_date):
    daily_data = {}

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
                if date not in daily_data:
                    daily_data[date] = {}
                daily_data[date][city] = round(record["temp_avg"], 1)

        except s3.exceptions.NoSuchKey:
            print(f"スキップ: {key} が存在しません")
        except Exception as e:
            print(f"エラー: {key} の読み込みに失敗しました: {str(e)}")

    result = []
    for date in sorted(daily_data.keys()):
        entry = {"date": date}
        entry.update(daily_data[date])
        result.append(entry)

    return result
