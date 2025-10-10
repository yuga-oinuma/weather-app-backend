import sys
import os
from unittest.mock import patch, MagicMock
import pytest
from botocore.exceptions import ClientError

# プロジェクトルートをPythonパスに追加
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lambdas', 'layers', 'common'))

from weather_common.s3_utils import fetch_s3_json


@patch('weather_common.s3_utils.s3')
def test_fetch_s3_json_success(mock_s3):
    """S3からJSONデータを正常に取得するテスト"""
    # モックの設定
    mock_body = MagicMock()
    mock_body.read.return_value = b'{"test": "data"}'
    mock_s3.get_object.return_value = {"Body": mock_body}
    
    result = fetch_s3_json("test-bucket", "test-key")
    
    assert result == '{"test": "data"}'
    mock_s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="test-key")


@patch('weather_common.s3_utils.s3')
def test_fetch_s3_json_not_found(mock_s3):
    """S3でキーが見つからない場合のテスト"""
    # NoSuchKeyエラーをシミュレート
    error_response = {'Error': {'Code': 'NoSuchKey'}}
    mock_s3.get_object.side_effect = ClientError(error_response, 'GetObject')
    
    result = fetch_s3_json("test-bucket", "test-key")
    
    assert result == "[]"  # デフォルト値


@patch('weather_common.s3_utils.s3')
def test_fetch_s3_json_custom_default(mock_s3):
    """カスタムデフォルト値のテスト"""
    error_response = {'Error': {'Code': 'NoSuchKey'}}
    mock_s3.get_object.side_effect = ClientError(error_response, 'GetObject')
    
    result = fetch_s3_json("test-bucket", "test-key", default='{"empty": true}')
    
    assert result == '{"empty": true}'


@patch('weather_common.s3_utils.s3')
def test_fetch_s3_json_other_error(mock_s3):
    """NoSuchKey以外のエラーの場合は例外を再発生させるテスト"""
    error_response = {'Error': {'Code': 'AccessDenied'}}
    mock_s3.get_object.side_effect = ClientError(error_response, 'GetObject')
    
    with pytest.raises(ClientError):
        fetch_s3_json("test-bucket", "test-key")