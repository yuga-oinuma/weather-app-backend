import json
import sys
import os
from unittest.mock import patch, MagicMock
from datetime import datetime, date
import pytest

# プロジェクトルートをPythonパスに追加
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lambdas', 'layers', 'common'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lambdas', 'api', 'get_current_weather'))

from lambdas.api.get_current_weather.app import lambda_handler, get_latest_weather_file


@pytest.fixture
def api_event():
    """API Gateway イベントのフィクスチャ"""
    return {
        "queryStringParameters": {"city": "Tokyo"},
        "httpMethod": "GET",
        "path": "/current-weather",
        "headers": {},
        "body": None
    }


@pytest.fixture
def api_event_no_city():
    """cityパラメータがないAPI Gateway イベント"""
    return {
        "queryStringParameters": None,
        "httpMethod": "GET",
        "path": "/current-weather",
        "headers": {},
        "body": None
    }


@patch.dict(os.environ, {'BUCKET_NAME': 'test-bucket'})
@patch('lambdas.api.get_current_weather.app.s3')
@patch('lambdas.api.get_current_weather.app.fetch_s3_json')
def test_lambda_handler_success(mock_fetch_s3_json, mock_s3, api_event):
    """正常なレスポンスのテスト"""
    # モックの設定
    mock_s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "weather-data-oinuma/Tokyo/2024-01-01/1704067200.json"}
        ]
    }
    mock_fetch_s3_json.return_value = '{"temperature": 20}'
    
    result = lambda_handler(api_event, None)
    
    assert result["statusCode"] == 200
    assert result["body"] == '{"temperature": 20}'


@patch.dict(os.environ, {'BUCKET_NAME': 'test-bucket'})
def test_lambda_handler_missing_city(api_event_no_city):
    """cityパラメータが不足している場合のテスト"""
    result = lambda_handler(api_event_no_city, None)
    
    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "error" in body
    assert "Missing 'city' query parameter" in body["error"]


@patch.dict(os.environ, {'BUCKET_NAME': 'test-bucket'})
@patch('lambdas.api.get_current_weather.app.s3')
def test_lambda_handler_no_data_found(mock_s3, api_event):
    """データが見つからない場合のテスト"""
    # 今日のデータなし
    mock_s3.list_objects_v2.side_effect = [
        {"Contents": []},  # 今日
        {"Contents": []}   # 昨日
    ]
    
    result = lambda_handler(api_event, None)
    
    assert result["statusCode"] == 404
    body = json.loads(result["body"])
    assert "error" in body
    assert "No weather data found for city: Tokyo" in body["error"]


def test_get_latest_weather_file():
    """最新の天気ファイル取得のテスト"""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "weather-data-oinuma/Tokyo/2024-01-01/1704067200.json"},
            {"Key": "weather-data-oinuma/Tokyo/2024-01-01/1704070800.json"},
            {"Key": "weather-data-oinuma/Tokyo/2024-01-01/1704074400.json"}
        ]
    }
    
    with patch('lambdas.api.get_current_weather.app.s3', mock_s3):
        result = get_latest_weather_file("Tokyo", date(2024, 1, 1), "test-bucket")
    
    # 最新のファイル（最大のタイムスタンプ）が返される
    assert result == "weather-data-oinuma/Tokyo/2024-01-01/1704074400.json"


def test_get_latest_weather_file_no_contents():
    """コンテンツが存在しない場合のテスト"""
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {}
    
    with patch('lambdas.api.get_current_weather.app.s3', mock_s3):
        result = get_latest_weather_file("Tokyo", date(2024, 1, 1), "test-bucket")
    
    assert result is None