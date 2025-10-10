import json
import sys
import os

# プロジェクトルートをPythonパスに追加
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lambdas', 'layers', 'common'))

from weather_common.response_utils import api_response


def test_api_response_with_dict():
    """辞書データでのレスポンス作成テスト"""
    body = {"message": "hello world", "status": "success"}
    result = api_response(200, body)
    
    assert result["statusCode"] == 200
    assert result["headers"]["Content-Type"] == "application/json"
    assert result["headers"]["Access-Control-Allow-Origin"] == "*"
    
    parsed_body = json.loads(result["body"])
    assert parsed_body["message"] == "hello world"
    assert parsed_body["status"] == "success"


def test_api_response_with_list():
    """リストデータでのレスポンス作成テスト"""
    body = [{"id": 1, "name": "test"}]
    result = api_response(200, body)
    
    assert result["statusCode"] == 200
    parsed_body = json.loads(result["body"])
    assert len(parsed_body) == 1
    assert parsed_body[0]["id"] == 1


def test_api_response_already_json():
    """既にJSON文字列の場合のテスト"""
    body = '{"message": "already json"}'
    result = api_response(200, body, already_json=True)
    
    assert result["statusCode"] == 200
    assert result["body"] == body


def test_api_response_error_status():
    """エラーステータスのテスト"""
    body = {"error": "Not found"}
    result = api_response(404, body)
    
    assert result["statusCode"] == 404
    parsed_body = json.loads(result["body"])
    assert parsed_body["error"] == "Not found"