import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")


def fetch_s3_json(bucket_name: str, key: str, default="[]") -> str:
    """
    指定したS3キーのJSONデータを文字列で取得。
    存在しない場合は default を返す。
    """
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        body = obj["Body"].read().decode("utf-8")
        return body
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return default
        raise
