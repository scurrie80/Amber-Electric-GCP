import pytest
import os
from unittest import mock
from unittest.mock import MagicMock, patch # Added for GCS tests
import asyncio
import responses
import aiohttp
import time
import json # Added for GCS tests

from shared_utils import ( # Updated imports
    load_config,
    AsyncAmberClient,
    save_to_gcs_async,
    check_gcs_file_exists_async,
    check_gcs_file_exists_sync
)
import shared_utils # Added for patching constants

# Attempt to import Google Cloud exceptions
try:
    from google.cloud import exceptions as gcs_exceptions
except ImportError:
    # Create a dummy exception class if google.cloud.exceptions is not available
    # This allows tests to run without gcs libraries, though errors won't be specific
    class GCSMockError(Exception):
        pass
    gcs_exceptions = MagicMock()
    gcs_exceptions.GoogleCloudError = GCSMockError


# Test successful config loading
@mock.patch.dict(os.environ, {
    "AMBER_SITE_ID": "test_site_id",
    "AMBER_API_KEY": "test_api_key",
    "GCS_BUCKET_NAME": "test_bucket_name"
})
def test_load_config_success():
    """Tests that load_config successfully loads configuration from environment variables."""
    config = load_config()
    assert config["site_id"] == "test_site_id"
    assert config["api_key"] == "test_api_key"
    assert config["gcs_bucket_name"] == "test_bucket_name"

# Test missing configuration - AMBER_API_KEY missing
@mock.patch.dict(os.environ, {
    "AMBER_SITE_ID": "test_site_id",
    "GCS_BUCKET_NAME": "test_bucket_name"
}, clear=True)
def test_load_config_missing_api_key():
    """Tests that load_config raises ValueError if AMBER_API_KEY is missing."""
    with pytest.raises(ValueError) as excinfo:
        load_config()
    assert "AMBER_API_KEY" in str(excinfo.value)
    assert "Missing required configuration(s): AMBER_API_KEY" in str(excinfo.value)

# Test missing configuration - AMBER_SITE_ID missing
@mock.patch.dict(os.environ, {
    "AMBER_API_KEY": "test_api_key",
    "GCS_BUCKET_NAME": "test_bucket_name"
}, clear=True)
def test_load_config_missing_site_id():
    """Tests that load_config raises ValueError if AMBER_SITE_ID is missing."""
    with pytest.raises(ValueError) as excinfo:
        load_config()
    assert "AMBER_SITE_ID" in str(excinfo.value)
    assert "Missing required configuration(s): AMBER_SITE_ID" in str(excinfo.value)

# Test missing configuration - GCS_BUCKET_NAME missing
@mock.patch.dict(os.environ, {
    "AMBER_SITE_ID": "test_site_id",
    "AMBER_API_KEY": "test_api_key"
}, clear=True)
def test_load_config_missing_gcs_bucket():
    """Tests that load_config raises ValueError if GCS_BUCKET_NAME is missing."""
    with pytest.raises(ValueError) as excinfo:
        load_config()
    assert "GCS_BUCKET_NAME" in str(excinfo.value)
    assert "Missing required configuration(s): GCS_BUCKET_NAME" in str(excinfo.value)

# Test missing configuration - Multiple missing
@mock.patch.dict(os.environ, {
    "AMBER_SITE_ID": "test_site_id"
}, clear=True)
def test_load_config_multiple_missing():
    """Tests that load_config raises ValueError with all missing configurations listed."""
    with pytest.raises(ValueError) as excinfo:
        load_config()
    assert "AMBER_API_KEY" in str(excinfo.value)
    assert "GCS_BUCKET_NAME" in str(excinfo.value)
    assert "Missing required configuration(s): AMBER_API_KEY, GCS_BUCKET_NAME" in str(excinfo.value)

# Test missing configuration - All missing
@mock.patch.dict(os.environ, {}, clear=True)
def test_load_config_all_missing():
    """Tests that load_config raises ValueError with all missing configurations listed if all are missing."""
    with pytest.raises(ValueError) as excinfo:
        load_config()
    assert "AMBER_SITE_ID" in str(excinfo.value)
    assert "AMBER_API_KEY" in str(excinfo.value)
    assert "GCS_BUCKET_NAME" in str(excinfo.value)
    assert "Missing required configuration(s): AMBER_SITE_ID, AMBER_API_KEY, GCS_BUCKET_NAME" in str(excinfo.value)


# Tests for AsyncAmberClient
@responses.activate
async def test_async_amber_client_get_successful(mocker):
    """Tests a successful GET request with AsyncAmberClient."""
    base_url = "https://api.amberengine.com/v1/sites/test_site_id"
    endpoint = "test_endpoint"
    expected_url = f"{base_url}/{endpoint}"
    mock_response_payload = {"data": "success"}

    responses.add(
        responses.GET,
        expected_url,
        json=mock_response_payload,
        status=200
    )

    client = AsyncAmberClient(api_key="test_key", site_id="test_site_id")
    response = await client.get(endpoint)

    assert response == mock_response_payload
    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == expected_url
    assert responses.calls[0].request.headers["Authorization"] == "Bearer test_key"
    assert responses.calls[0].request.headers["User-Agent"].startswith("AmberEnginePythonSDK")

@responses.activate
async def test_async_amber_client_get_with_resolution(mocker):
    """Tests a successful GET request with resolution parameter."""
    base_url = "https://api.amberengine.com/v1/sites/test_site_id"
    endpoint = "test_endpoint"
    resolution = 30
    expected_url = f"{base_url}/{endpoint}?resolution={resolution}"
    mock_response_payload = {"status": "ok"}

    responses.add(
        responses.GET,
        expected_url,
        json=mock_response_payload,
        status=200
    )

    client = AsyncAmberClient(api_key="test_key", site_id="test_site_id")
    response = await client.get(endpoint, resolution=resolution)

    assert response == mock_response_payload
    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == expected_url
    assert responses.calls[0].request.headers["Authorization"] == "Bearer test_key"

@responses.activate
async def test_async_amber_client_get_client_error_no_retry(mocker):
    """Tests that a 4xx client error is not retried."""
    base_url = "https://api.amberengine.com/v1/sites/test_site_id"
    endpoint = "test_endpoint"
    expected_url = f"{base_url}/{endpoint}"

    responses.add(
        responses.GET,
        expected_url,
        status=401  # Unauthorized
    )

    client = AsyncAmberClient(api_key="test_key", site_id="test_site_id")

    with pytest.raises(aiohttp.ClientResponseError) as excinfo:
        await client.get(endpoint)

    assert excinfo.value.status == 401
    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == expected_url

@responses.activate
async def test_async_amber_client_get_server_error_retry_success(mocker):
    """Tests that a 5xx server error is retried and can succeed."""
    mocker.patch.object(shared_utils, 'INITIAL_RETRY_DELAY_SECONDS', 0.001)
    mocker.patch.object(shared_utils, 'MAX_RETRIES', 3)
    mock_asyncio_sleep = mocker.patch('asyncio.sleep', return_value=None)

    base_url = "https://api.amberengine.com/v1/sites/test_site_id"
    endpoint = "test_endpoint"
    expected_url = f"{base_url}/{endpoint}"
    success_payload = {"data": "finally_success"}

    responses.add(responses.GET, expected_url, status=500)
    responses.add(responses.GET, expected_url, json=success_payload, status=200)

    client = AsyncAmberClient(api_key="test_key", site_id="test_site_id")
    response = await client.get(endpoint)

    assert response == success_payload
    assert len(responses.calls) == 2
    mock_asyncio_sleep.assert_called_once_with(0.001)

@responses.activate
async def test_async_amber_client_get_max_retries_exceeded_5xx(mocker):
    """Tests that max retries are respected for 5xx errors."""
    mocker.patch.object(shared_utils, 'INITIAL_RETRY_DELAY_SECONDS', 0.001)
    mocker.patch.object(shared_utils, 'MAX_RETRIES', 2)
    mock_asyncio_sleep = mocker.patch('asyncio.sleep', return_value=None)

    base_url = "https://api.amberengine.com/v1/sites/test_site_id"
    endpoint = "test_endpoint"
    expected_url = f"{base_url}/{endpoint}"

    responses.add(responses.GET, expected_url, status=500)
    responses.add(responses.GET, expected_url, status=500)

    client = AsyncAmberClient(api_key="test_key", site_id="test_site_id")

    with pytest.raises(Exception, match=f"Max retries \(2\) reached for GET {expected_url}"):
        await client.get(endpoint)

    assert len(responses.calls) == 2
    assert mock_asyncio_sleep.call_count == 1

@responses.activate
async def test_async_amber_client_get_rate_limit_retry_with_header(mocker):
    mocker.patch.object(shared_utils, 'MAX_RETRIES', 3)
    mock_asyncio_sleep = mocker.patch('asyncio.sleep', return_value=None)
    mock_time = mocker.patch('time.time') # Mock time.time()

    base_url = "https://api.amberengine.com/v1/sites/test_site_id"
    endpoint = "test_endpoint_rate_limit"
    expected_url = f"{base_url}/{endpoint}"
    success_payload = {"data": "rate_limit_success"}

    current_timestamp = 1678886400 # Example: 2023-03-15 12:00:00 PM UTC
    mock_time.return_value = current_timestamp

    # RateLimit-Reset is 2 seconds in the future from current_timestamp
    rate_limit_reset_value = str(current_timestamp + 2)

    responses.add(
        responses.GET, expected_url, status=429, headers={"RateLimit-Reset": rate_limit_reset_value}
    )
    responses.add(responses.GET, expected_url, json=success_payload, status=200)

    client = AsyncAmberClient(api_key="test_key", site_id="test_site_id")
    response = await client.get(endpoint)

    assert response == success_payload
    assert len(responses.calls) == 2
    # Sleep duration should be RateLimit-Reset - current_time
    # allow for a small delta due to processing time if not mocking time.time()
    # but with time.time() mocked, it should be precise.
    expected_sleep_duration = float(rate_limit_reset_value) - current_timestamp
    mock_asyncio_sleep.assert_called_once_with(pytest.approx(expected_sleep_duration, abs=0.01))


@responses.activate
async def test_async_amber_client_get_rate_limit_no_header_uses_exponential_backoff(mocker):
    initial_delay = 0.001
    mocker.patch.object(shared_utils, 'INITIAL_RETRY_DELAY_SECONDS', initial_delay)
    mocker.patch.object(shared_utils, 'MAX_RETRIES', 3)
    mock_asyncio_sleep = mocker.patch('asyncio.sleep', return_value=None)

    base_url = "https://api.amberengine.com/v1/sites/test_site_id"
    endpoint = "test_endpoint_no_header_rl"
    expected_url = f"{base_url}/{endpoint}"
    success_payload = {"data": "no_header_success"}

    responses.add(responses.GET, expected_url, status=429)
    responses.add(responses.GET, expected_url, json=success_payload, status=200)

    client = AsyncAmberClient(api_key="test_key", site_id="test_site_id")
    response = await client.get(endpoint)

    assert response == success_payload
    assert len(responses.calls) == 2
    mock_asyncio_sleep.assert_called_once_with(initial_delay)

@responses.activate
async def test_async_amber_client_get_retry_uses_retry_after_header_if_present(mocker):
    mocker.patch.object(shared_utils, 'MAX_RETRIES', 3)
    mock_asyncio_sleep = mocker.patch('asyncio.sleep', return_value=None)

    base_url = "https://api.amberengine.com/v1/sites/test_site_id"
    endpoint = "test_endpoint_retry_after"
    expected_url = f"{base_url}/{endpoint}"
    success_payload = {"data": "retry_after_success"}
    retry_after_delay_seconds = 0.05

    responses.add(
        responses.GET, expected_url, status=503, headers={"Retry-After": str(retry_after_delay_seconds)}
    )
    responses.add(responses.GET, expected_url, json=success_payload, status=200)

    client = AsyncAmberClient(api_key="test_key", site_id="test_site_id")
    response = await client.get(endpoint)

    assert response == success_payload
    assert len(responses.calls) == 2
    mock_asyncio_sleep.assert_called_once_with(retry_after_delay_seconds)

@responses.activate
async def test_async_amber_client_get_rate_limit_reset_past_immediate_retry(mocker):
    initial_delay = 0.001
    mocker.patch.object(shared_utils, 'INITIAL_RETRY_DELAY_SECONDS', initial_delay)
    mocker.patch.object(shared_utils, 'MAX_RETRIES', 3)
    mock_asyncio_sleep = mocker.patch('asyncio.sleep', return_value=None)
    mock_time = mocker.patch('time.time') # Mock time.time()
    current_timestamp = 1678886400
    mock_time.return_value = current_timestamp


    base_url = "https://api.amberengine.com/v1/sites/test_site_id"
    endpoint = "test_endpoint_rl_past"
    expected_url = f"{base_url}/{endpoint}"
    success_payload = {"data": "rl_past_success"}

    past_reset_time = str(current_timestamp - 3600) # 1 hour in the past

    responses.add(
        responses.GET, expected_url, status=429, headers={"RateLimit-Reset": past_reset_time}
    )
    responses.add(responses.GET, expected_url, json=success_payload, status=200)

    client = AsyncAmberClient(api_key="test_key", site_id="test_site_id")
    response = await client.get(endpoint)

    assert response == success_payload
    assert len(responses.calls) == 2
    mock_asyncio_sleep.assert_called_once_with(initial_delay)


# Tests for GCS Utilities
@pytest.mark.asyncio
async def test_save_to_gcs_async_success():
    mock_gcs_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_gcs_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    # Mock the executor call for blob.upload_from_string
    mock_loop = MagicMock()
    mock_loop.run_in_executor = MagicMock(return_value=asyncio.Future())
    mock_loop.run_in_executor.return_value.set_result(None) # Simulate successful upload

    data_to_save = [{"key": "value1", "id": 1}, {"key": "value2", "id": 2}]
    expected_jsonl = json.dumps(data_to_save[0]) + "\n" + json.dumps(data_to_save[1])

    bucket_name = "test-bucket"
    object_name = "test-object.jsonl"

    with patch('asyncio.get_event_loop', return_value=mock_loop):
        gcs_path = await save_to_gcs_async(data_to_save, bucket_name, object_name, mock_gcs_client)

    mock_gcs_client.bucket.assert_called_once_with(bucket_name)
    mock_bucket.blob.assert_called_once_with(object_name)

    # Assert that run_in_executor was called correctly
    # The actual call to blob.upload_from_string happens inside the executor
    # So we check the arguments passed to run_in_executor
    args, kwargs = mock_loop.run_in_executor.call_args
    assert args[1] == mock_blob.upload_from_string # Check the correct method is being called
    assert args[2] == expected_jsonl # Check data
    assert args[3] == "application/jsonl" # Check content type

    assert gcs_path == f"gs://{bucket_name}/{object_name}"

@pytest.mark.asyncio
async def test_save_to_gcs_async_gcs_error():
    mock_gcs_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_gcs_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    # Simulate GCS error during upload
    gcs_error = gcs_exceptions.GoogleCloudError("GCS Upload Error")
    mock_loop = MagicMock()
    mock_loop.run_in_executor = MagicMock(return_value=asyncio.Future())
    mock_loop.run_in_executor.return_value.set_exception(gcs_error)


    data_to_save = [{"key": "value"}]
    bucket_name = "test-bucket"
    object_name = "test-object.jsonl"

    with patch('asyncio.get_event_loop', return_value=mock_loop):
        with pytest.raises(gcs_exceptions.GoogleCloudError, match="GCS Upload Error"):
            await save_to_gcs_async(data_to_save, bucket_name, object_name, mock_gcs_client)

    mock_gcs_client.bucket.assert_called_once_with(bucket_name)
    mock_bucket.blob.assert_called_once_with(object_name)
    mock_loop.run_in_executor.assert_called_once()


@pytest.mark.asyncio
async def test_check_gcs_file_exists_async_true():
    mock_gcs_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_gcs_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    mock_loop = MagicMock()
    mock_loop.run_in_executor = MagicMock(return_value=asyncio.Future())
    # Configure the mock_blob.exists to be called inside run_in_executor
    # The return value of run_in_executor itself should be the result of blob.exists()
    mock_loop.run_in_executor.return_value.set_result(True)


    bucket_name = "test-bucket"
    object_name = "test-object"

    with patch('asyncio.get_event_loop', return_value=mock_loop):
        result = await check_gcs_file_exists_async(bucket_name, object_name, mock_gcs_client)

    assert result is True
    mock_gcs_client.bucket.assert_called_once_with(bucket_name)
    mock_bucket.blob.assert_called_once_with(object_name)
    # Check that run_in_executor was called with blob.exists
    args, _ = mock_loop.run_in_executor.call_args
    assert args[1] == mock_blob.exists

@pytest.mark.asyncio
async def test_check_gcs_file_exists_async_false():
    mock_gcs_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_gcs_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    mock_loop = MagicMock()
    mock_loop.run_in_executor = MagicMock(return_value=asyncio.Future())
    mock_loop.run_in_executor.return_value.set_result(False) # Simulate blob not existing

    bucket_name = "test-bucket"
    object_name = "test-object"

    with patch('asyncio.get_event_loop', return_value=mock_loop):
        result = await check_gcs_file_exists_async(bucket_name, object_name, mock_gcs_client)

    assert result is False
    mock_gcs_client.bucket.assert_called_once_with(bucket_name)
    mock_bucket.blob.assert_called_once_with(object_name)
    args, _ = mock_loop.run_in_executor.call_args
    assert args[1] == mock_blob.exists


@pytest.mark.asyncio
async def test_check_gcs_file_exists_async_gcs_error():
    mock_gcs_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_gcs_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    gcs_error = gcs_exceptions.GoogleCloudError("GCS Exists Error")
    mock_loop = MagicMock()
    mock_loop.run_in_executor = MagicMock(return_value=asyncio.Future())
    mock_loop.run_in_executor.return_value.set_exception(gcs_error) # Simulate GCS error

    bucket_name = "test-bucket"
    object_name = "test-object"

    with patch('asyncio.get_event_loop', return_value=mock_loop):
        with pytest.raises(gcs_exceptions.GoogleCloudError, match="GCS Exists Error"):
            await check_gcs_file_exists_async(bucket_name, object_name, mock_gcs_client)

    mock_gcs_client.bucket.assert_called_once_with(bucket_name)
    mock_bucket.blob.assert_called_once_with(object_name)
    args, _ = mock_loop.run_in_executor.call_args
    assert args[1] == mock_blob.exists


# Synchronous versions
def test_check_gcs_file_exists_sync_true():
    mock_gcs_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_gcs_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    mock_blob.exists.return_value = True

    result = check_gcs_file_exists_sync("test-bucket", "test-object", mock_gcs_client)

    assert result is True
    mock_gcs_client.bucket.assert_called_once_with("test-bucket")
    mock_bucket.blob.assert_called_once_with("test-object")
    mock_blob.exists.assert_called_once()

def test_check_gcs_file_exists_sync_false():
    mock_gcs_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_gcs_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    mock_blob.exists.return_value = False

    result = check_gcs_file_exists_sync("test-bucket", "test-object", mock_gcs_client)

    assert result is False
    mock_gcs_client.bucket.assert_called_once_with("test-bucket")
    mock_bucket.blob.assert_called_once_with("test-object")
    mock_blob.exists.assert_called_once()

def test_check_gcs_file_exists_sync_gcs_error():
    mock_gcs_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_gcs_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    gcs_error = gcs_exceptions.GoogleCloudError("GCS Sync Exists Error")
    mock_blob.exists.side_effect = gcs_error

    with pytest.raises(gcs_exceptions.GoogleCloudError, match="GCS Sync Exists Error"):
        check_gcs_file_exists_sync("test-bucket", "test-object", mock_gcs_client)

    mock_gcs_client.bucket.assert_called_once_with("test-bucket")
    mock_bucket.blob.assert_called_once_with("test-object")
    mock_blob.exists.assert_called_once()
