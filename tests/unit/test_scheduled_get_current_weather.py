import json
import sys
import os
from unittest.mock import patch, MagicMock
import pytest

# プロジェクトルートをPythonパスに追加
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lambdas', 'scheduled', 'get_current_weather'))

from lambdas.scheduled.get_current_weather.app import lambda_handler, save_to_s3


@pytest.fixture
def scheduled_event():
    """スケジュールされたイベントのフィクスチャ"""
    return {
        "source": "aws.events",
        "detail-type": "Scheduled Event",
        "detail": {}
    }


@patch('lambdas.scheduled.get_current_weather.app.boto3.client')
def test_save_to_s3(mock_boto3_client):
    """S3への保存テスト"""
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    test_data = {"temperature": 20, "humidity": 60}
    save_to_s3("test-bucket", "test-key", test_data)
    
    mock_s3.put_object.assert_called_once_with(
        Bucket="test-bucket",
        Key="test-key",
        Body=json.dumps(test_data)
    )


@patch.dict(os.environ, {'API_KEY': 'test-api-key', 'BUCKET_NAME': 'test-bucket'})
@patch('lambdas.scheduled.get_current_weather.app.requests.get')
@patch('lambdas.scheduled.get_current_weather.app.save_to_s3')
def test_lambda_handler_success(mock_save_to_s3, mock_requests_get, scheduled_event):
    """正常な実行のテスト"""
    # モックレスポンスの設定
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "dt": 1704067200,
        "main": {"temp": 20.5, "humidity": 65},
        "weather": [{"main": "Clear", "description": "clear sky"}]
    }
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response
    
    # 実行
    lambda_handler(scheduled_event, None)
    
    # 8都市分のAPIコールとS3保存が実行されることを確認
    assert mock_requests_get.call_count == 8
    assert mock_save_to_s3.call_count == 8
    
    # 最初の都市（札幌）のAPIコールを確認
    first_call = mock_requests_get.call_args_list[0]
    assert first_call[0][0] == "https://api.openweathermap.org/data/2.5/weather"
    assert first_call[1]["params"]["lat"] == 43.0621
    assert first_call[1]["params"]["lon"] == 141.3544
    assert first_call[1]["params"]["appid"] == "test-api-key"


@patch.dict(os.environ, {'API_KEY': 'test-api-key', 'BUCKET_NAME': 'test-bucket'})
@patch('lambdas.scheduled.get_current_weather.app.requests.get')
@patch('lambdas.scheduled.get_current_weather.app.save_to_s3')
@patch('builtins.print')
def test_lambda_handler_api_error(mock_print, mock_save_to_s3, mock_requests_get, scheduled_event):
    """API エラーの場合のテスト"""
    # 最初の都市でエラー、2番目は成功
    mock_response_success = MagicMock()
    mock_response_success.json.return_value = {"dt": 1704067200, "main": {"temp": 20}}
    mock_response_success.raise_for_status.return_value = None
    
    mock_requests_get.side_effect = [
        Exception("API Error"),  # 札幌でエラー
        mock_response_success,   # 仙台で成功
        mock_response_success,   # 東京で成功
        mock_response_success,   # 名古屋で成功
        mock_response_success,   # 金沢で成功
        mock_response_success,   # 高知で成功
        mock_response_success,   # 福岡で成功
        mock_response_success,   # 那覇で成功
    ]
    
    # 実行（例外で停止しないことを確認）
    lambda_handler(scheduled_event, None)
    
    # エラーメッセージが出力されることを確認
    error_calls = [call for call in mock_print.call_args_list if "Failed to fetch" in str(call)]
    assert len(error_calls) == 1
    assert "Sapporo" in str(error_calls[0])
    
    # 成功した7都市分の保存が実行されることを確認
    assert mock_save_to_s3.call_count == 7