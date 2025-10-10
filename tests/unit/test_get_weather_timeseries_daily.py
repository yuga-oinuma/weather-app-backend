import json
import sys
import os
from unittest.mock import patch
import pytest

# プロジェクトルートをPythonパスに追加
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lambdas', 'layers', 'common'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lambdas', 'api', 'get_weather_timeseries_daily'))

from lambdas.api.get_weather_timeseries_daily.app import lambda_handler


@pytest.fixture
def api_event():
    """API Gateway イベントのフィクスチャ"""
    return {
        "queryStringParameters": {"date": "2024-01-01"},
        "httpMethod": "GET",
        "path": "/weather-timeseries-daily",
        "headers": {},
        "body": None
    }


@pytest.fixture
def api_event_no_date():
    """dateパラメータがないAPI Gateway イベント"""
    return {
        "queryStringParameters": None,
        "httpMethod": "GET",
        "path": "/weather-timeseries-daily",
        "headers": {},
        "body": None
    }


@patch.dict(os.environ, {'BUCKET_NAME': 'test-bucket'})
@patch('lambdas.api.get_weather_timeseries_daily.app.fetch_s3_json')
def test_lambda_handler_success(mock_fetch_s3_json, api_event):
    """正常なレスポンスのテスト"""
    mock_fetch_s3_json.return_value = '{"timeseries": "daily weather data"}'
    
    result = lambda_handler(api_event, None)
    
    assert result["statusCode"] == 200
    assert result["body"] == '{"timeseries": "daily weather data"}'
    mock_fetch_s3_json.assert_called_once_with(
        "test-bucket", 
        "analysis-results/timeseries/2024-01-01.json", 
        default="[]"
    )


@patch.dict(os.environ, {'BUCKET_NAME': 'test-bucket'})
def test_lambda_handler_missing_date(api_event_no_date):
    """dateパラメータが不足している場合のテスト"""
    result = lambda_handler(api_event_no_date, None)
    
    assert result["statusCode"] == 400
    body = json.loads(result["body"])
    assert "error" in body
    assert "Missing 'date' query parameter" in body["error"]