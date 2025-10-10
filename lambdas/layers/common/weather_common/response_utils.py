import json


def api_response(status: int, body, already_json: bool = False):
    """
    API Gateway (Lambda proxy) 向けレスポンス作成。
    bodyは辞書やリストならJSON文字列化する。
    """
    if not already_json:
        body = json.dumps(body, ensure_ascii=False)

    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": body,
    }
