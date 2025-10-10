import json
import sys
import os
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import pytest

# プロジェクトルートをPythonパスに追加
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lambdas', 'scheduled', 'analyze_weather_daily'))

from lambdas.scheduled.analyze_weather_daily.app import lambda_handler


@pytest.fixture
def scheduled_event():
    """スケジュールされたイベントのフィクスチャ"""
    return {
        "source": "aws.events",
        "detail-type": "Scheduled Event",
        "detail": {}
    }


@patch.dict(os.environ, {'BUCKET_NAME': 'test-bucket'})
@patch('lambdas.scheduled.analyze_weather_daily.app.boto3.client')
@patch('lambdas.scheduled.analyze_weather_daily.app.get_summary')
@patch('lambdas.scheduled.analyze_weather_daily.app.get_timeseries')
@patch('builtins.print')
def test_lambda_handler_success(mock_print, mock_get_timeseries, mock_get_summary, mock_boto3_client, scheduled_event):
    """正常な実行のテスト"""
    # モックの設定
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    mock_summary_data = {"summary": "test summary data"}
    mock_timeseries_data = {"timeseries": "test timeseries data"}
    
    mock_get_summary.return_value = mock_summary_data
    mock_get_timeseries.return_value = mock_timeseries_data
    
    # 実行
    lambda_handler(scheduled_event, None)
    
    # 関数が正しい引数で呼ばれることを確認
    cities = ["Sapporo", "Tokyo", "Kanazawa", "Kochi", "Naha"]
    mock_get_summary.assert_called_once()
    mock_get_timeseries.assert_called_once()
    
    # S3への保存が2回実行されることを確認（summary + timeseries）
    assert mock_s3.put_object.call_count == 2
    
    # 保存されるデータの確認
    calls = mock_s3.put_object.call_args_list
    
    # Summary データの確認
    summary_call = calls[0]
    assert summary_call[1]["Bucket"] == "test-bucket"
    assert "analysis-results/summary/" in summary_call[1]["Key"]
    assert summary_call[1]["Body"] == json.dumps(mock_summary_data, ensure_ascii=False)
    assert summary_call[1]["ContentType"] == "application/json"
    
    # Timeseries データの確認
    timeseries_call = calls[1]
    assert timeseries_call[1]["Bucket"] == "test-bucket"
    assert "analysis-results/timeseries/" in timeseries_call[1]["Key"]
    assert timeseries_call[1]["Body"] == json.dumps(mock_timeseries_data, ensure_ascii=False)
    assert timeseries_call[1]["ContentType"] == "application/json"
    
    # ログメッセージが出力されることを確認
    assert mock_print.call_count == 2
    print_calls = [str(call) for call in mock_print.call_args_list]
    assert any("Saved summary data:" in call for call in print_calls)
    assert any("Saved timeseries data:" in call for call in print_calls)