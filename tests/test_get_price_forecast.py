import pytest
import asyncio
import datetime
from unittest.mock import patch, MagicMock, AsyncMock

# Import the functions to test
from get_price_forecast.get_price_forecast import _async_process_forecast_data, process_amber_data

# Attempt to import Google Cloud exceptions for type hinting and raising
try:
    from google.cloud import exceptions as gcs_exceptions
except ImportError:
    class GCSMockError(Exception):
        pass
    gcs_exceptions = MagicMock()
    gcs_exceptions.GoogleCloudError = GCSMockError

# Attempt to import aiohttp exceptions for type hinting and raising
try:
    from aiohttp import ClientResponseError
except ImportError:
    class MockClientResponseError(Exception):
        def __init__(self, status, message, headers=None, history=None, request_info=None):
            super().__init__(message)
            self.status = status
            self.message = message
            self.headers = headers
            self.history = history
            self.request_info = request_info
    ClientResponseError = MockClientResponseError


# Common mock configuration
MOCK_CONFIG = {
    "AMBER_SITE_ID": "test_site",
    "AMBER_API_KEY": "test_key",
    "GCS_BUCKET_NAME": "test_bucket"
}

# Fixed date and time for testing
TEST_DATETIME_UTC = datetime.datetime(2023, 10, 26, 10, 30, 0, tzinfo=datetime.timezone.utc)
TEST_DATETIME_UTC_STR = TEST_DATETIME_UTC.strftime("%Y%m%d%H%M%S")

# Path prefix for all patches
PATCH_PREFIX = "get_price_forecast.get_price_forecast"

@pytest.fixture
def mock_gcs_client_forecast():
    """Provides a fresh MagicMock for GCS client for forecast tests."""
    return MagicMock()

@pytest.fixture(autouse=True)
def common_patches_forecast(mocker, mock_gcs_client_forecast):
    """Apply common patches for all tests in this module."""
    mocker.patch(f"{PATCH_PREFIX}.load_config", return_value=MOCK_CONFIG)
    mocker.patch(f"{PATCH_PREFIX}.storage.Client", return_value=mock_gcs_client_forecast)
    mock_dt = MagicMock()
    mock_dt.datetime.utcnow.return_value = TEST_DATETIME_UTC
    mocker.patch(f"{PATCH_PREFIX}.datetime", mock_dt)


@pytest.mark.asyncio
async def test_async_process_forecast_data_success(mock_gcs_client_forecast):
    mock_amber_client_instance = AsyncMock()
    mock_forecast_data = [{'type': 'forecast', 'value': 100, 'duration': 30, 'startTime': '2023-10-26T10:30:00Z'}]
    mock_amber_client_instance.get.return_value = mock_forecast_data

    mock_gcs_path = f"gs://{MOCK_CONFIG['GCS_BUCKET_NAME']}/{MOCK_CONFIG['AMBER_SITE_ID']}/amber_prices_forecast_{TEST_DATETIME_UTC_STR}.jsonl"
    mock_save_gcs_instance = AsyncMock(return_value=mock_gcs_path)

    with patch(f"{PATCH_PREFIX}.AsyncAmberClient", return_value=mock_amber_client_instance) as mock_amber_constructor, \
         patch(f"{PATCH_PREFIX}.save_to_gcs_async", mock_save_gcs_instance):

        message, status_code = await _async_process_forecast_data(
            MOCK_CONFIG, mock_gcs_client_forecast, next_intervals=10, previous_intervals=1, resolution_minutes=15
        )

    mock_amber_constructor.assert_called_once_with(
        api_key=MOCK_CONFIG["AMBER_API_KEY"], site_id=MOCK_CONFIG["AMBER_SITE_ID"]
    )
    mock_amber_client_instance.get.assert_called_once_with(
        "prices/current", params={"next": 10, "previous": 1}, resolution=15
    )

    expected_object_name = f"{MOCK_CONFIG['AMBER_SITE_ID']}/amber_prices_forecast_{TEST_DATETIME_UTC_STR}.jsonl"
    mock_save_gcs_instance.assert_called_once_with(
        mock_forecast_data,
        MOCK_CONFIG["GCS_BUCKET_NAME"],
        expected_object_name,
        mock_gcs_client_forecast
    )

    assert status_code == 200
    assert f"Successfully processed and saved forecast data to {mock_gcs_path}" in message

@pytest.mark.asyncio
async def test_async_process_forecast_data_api_returns_empty_list(mock_gcs_client_forecast):
    mock_amber_client_instance = AsyncMock()
    mock_amber_client_instance.get.return_value = []

    mock_save_gcs_instance = AsyncMock()

    with patch(f"{PATCH_PREFIX}.AsyncAmberClient", return_value=mock_amber_client_instance), \
         patch(f"{PATCH_PREFIX}.save_to_gcs_async", mock_save_gcs_instance):

        message, status_code = await _async_process_forecast_data(
            MOCK_CONFIG, mock_gcs_client_forecast, next_intervals=5
        )

    mock_amber_client_instance.get.assert_called_once_with(
        "prices/current", params={"next": 5}, resolution=30
    )
    mock_save_gcs_instance.assert_not_called()

    assert status_code == 200
    assert "0 records processed from Amber API (no data to save)." in message

@pytest.mark.asyncio
async def test_async_process_forecast_data_api_error(mock_gcs_client_forecast):
    mock_amber_client_instance = AsyncMock()
    mock_amber_client_instance.get.side_effect = ClientResponseError(
        status=500, message='API Error', headers=None, history=None, request_info=MagicMock()
    )

    mock_save_gcs_instance = AsyncMock()

    with patch(f"{PATCH_PREFIX}.AsyncAmberClient", return_value=mock_amber_client_instance), \
         patch(f"{PATCH_PREFIX}.save_to_gcs_async", mock_save_gcs_instance):

        message, status_code = await _async_process_forecast_data(
            MOCK_CONFIG, mock_gcs_client_forecast
        )

    mock_amber_client_instance.get.assert_called_once()
    mock_save_gcs_instance.assert_not_called()

    assert status_code == 500
    assert "Failed to process Amber forecast data. Error fetching data from Amber API: API Error" in message

@pytest.mark.asyncio
async def test_async_process_forecast_data_gcs_save_error(mock_gcs_client_forecast):
    mock_amber_client_instance = AsyncMock()
    mock_forecast_data = [{'type': 'forecast', 'value': 100}]
    mock_amber_client_instance.get.return_value = mock_forecast_data

    gcs_error = gcs_exceptions.GoogleCloudError("GCS Save Error")
    mock_save_gcs_instance = AsyncMock(side_effect=gcs_error)

    with patch(f"{PATCH_PREFIX}.AsyncAmberClient", return_value=mock_amber_client_instance), \
         patch(f"{PATCH_PREFIX}.save_to_gcs_async", mock_save_gcs_instance):

        message, status_code = await _async_process_forecast_data(
            MOCK_CONFIG, mock_gcs_client_forecast
        )

    mock_amber_client_instance.get.assert_called_once()
    mock_save_gcs_instance.assert_called_once()

    assert status_code == 500
    assert "Failed to process Amber forecast data. Error saving data to GCS: GCS Save Error" in message

# Tests for Pub/Sub or non-HTTP entry point `process_amber_data`
@patch(f"{PATCH_PREFIX}._async_process_forecast_data", new_callable=AsyncMock)
def test_process_amber_data_pubsub_wrapper_success(mock_async_runner, mock_gcs_client_forecast, caplog):
    mock_async_runner.return_value = ("Processed successfully", 200)

    process_amber_data(event=None, context=None)

    mock_async_runner.assert_called_once()
    call_args, call_kwargs = mock_async_runner.call_args
    assert call_args[0] == MOCK_CONFIG
    assert call_args[1] == mock_gcs_client_forecast
    assert call_kwargs.get('next_intervals') == 48
    assert call_kwargs.get('previous_intervals') == 0
    assert call_kwargs.get('resolution_minutes') == 30

    assert "Amber price forecast processing complete." in caplog.text
    assert "Processed successfully" in caplog.text
    assert "Status code: 200" in caplog.text

@patch(f"{PATCH_PREFIX}._async_process_forecast_data", new_callable=AsyncMock)
def test_process_amber_data_pubsub_wrapper_failure(mock_async_runner, mock_gcs_client_forecast, caplog):
    mock_async_runner.return_value = ("Processing failed", 500)

    process_amber_data(event=None, context=None)

    mock_async_runner.assert_called_once()
    assert "Amber price forecast processing complete." in caplog.text
    assert "Processing failed" in caplog.text
    assert "Status code: 500" in caplog.text

# --- Tests for HTTP Entry Point `process_amber_data` ---

@patch(f"{PATCH_PREFIX}.asyncio.run")
@patch(f"{PATCH_PREFIX}._async_process_forecast_data", new_callable=AsyncMock)
def test_http_process_amber_data_success_defaults(
    mock_async_orchestrator, mock_asyncio_run, mock_gcs_client_forecast):

    request = MagicMock()
    request.content_type = 'application/json'
    request.get_json.return_value = {}

    mock_async_orchestrator.return_value = ("Success message", 200)
    # Make asyncio.run execute the coroutine and return its result
    mock_asyncio_run.side_effect = lambda coro: asyncio.get_event_loop().run_until_complete(coro)

    response, status_code = process_amber_data(request)

    mock_async_orchestrator.assert_called_once()
    args, kwargs = mock_async_orchestrator.call_args
    assert args[0] == MOCK_CONFIG
    assert args[1] == mock_gcs_client_forecast
    assert kwargs.get('next_intervals') == 96 # HTTP Default
    assert kwargs.get('previous_intervals') == 0 # HTTP Default
    assert kwargs.get('resolution_minutes') == 30 # HTTP Default

    assert status_code == 200
    assert response == "Success message"

@patch(f"{PATCH_PREFIX}.asyncio.run")
@patch(f"{PATCH_PREFIX}._async_process_forecast_data", new_callable=AsyncMock)
def test_http_process_amber_data_success_custom_params(
    mock_async_orchestrator, mock_asyncio_run, mock_gcs_client_forecast):

    request = MagicMock()
    request.content_type = 'application/json'
    custom_params = {"next_intervals": 10, "previous_intervals": 2, "resolution_minutes": 15}
    request.get_json.return_value = custom_params

    mock_async_orchestrator.return_value = ("Success custom", 200)
    mock_asyncio_run.side_effect = lambda coro: asyncio.get_event_loop().run_until_complete(coro)

    response, status_code = process_amber_data(request)

    mock_async_orchestrator.assert_called_once()
    _, kwargs = mock_async_orchestrator.call_args
    assert kwargs.get('next_intervals') == 10
    assert kwargs.get('previous_intervals') == 2
    assert kwargs.get('resolution_minutes') == 15
    assert status_code == 200
    assert response == "Success custom"

@patch(f"{PATCH_PREFIX}._async_process_forecast_data", new_callable=AsyncMock)
def test_http_process_amber_data_invalid_param_type(mock_async_orchestrator):
    request = MagicMock()
    request.content_type = 'application/json'
    request.get_json.return_value = {"next_intervals": "not-an-int"}

    response, status_code = process_amber_data(request)

    assert status_code == 400
    assert "Invalid parameter type for 'next_intervals'. Expected an integer." in response
    mock_async_orchestrator.assert_not_called()

@patch(f"{PATCH_PREFIX}.asyncio.run")
@patch(f"{PATCH_PREFIX}._async_process_forecast_data", new_callable=AsyncMock)
def test_http_process_amber_data_no_json_body_uses_defaults(
    mock_async_orchestrator, mock_asyncio_run, mock_gcs_client_forecast):

    request = MagicMock()
    request.content_type = 'text/plain'
    request.get_json.return_value = None # As per silent=True

    mock_async_orchestrator.return_value = ("Success defaults", 200)
    mock_asyncio_run.side_effect = lambda coro: asyncio.get_event_loop().run_until_complete(coro)

    response, status_code = process_amber_data(request)

    mock_async_orchestrator.assert_called_once()
    _, kwargs = mock_async_orchestrator.call_args
    assert kwargs.get('next_intervals') == 96 # HTTP Default
    assert kwargs.get('previous_intervals') == 0 # HTTP Default
    assert kwargs.get('resolution_minutes') == 30 # HTTP Default
    assert status_code == 200
    assert response == "Success defaults"

@patch(f"{PATCH_PREFIX}._async_process_forecast_data", new_callable=AsyncMock)
def test_http_process_amber_data_config_error(mock_async_orchestrator, mocker):
    # Override common_patches_forecast's load_config for this test
    mocker.patch(f"{PATCH_PREFIX}.load_config", side_effect=ValueError("Missing Forecast Config"))

    request = MagicMock()
    # No need to set request.content_type or get_json if load_config fails early

    response, status_code = process_amber_data(request)

    assert status_code == 500
    assert "Configuration error: Missing Forecast Config" in response
    mock_async_orchestrator.assert_not_called()

@patch(f"{PATCH_PREFIX}.asyncio.run")
@patch(f"{PATCH_PREFIX}._async_process_forecast_data", new_callable=AsyncMock)
def test_http_process_amber_data_core_logic_returns_error_status(
    mock_async_orchestrator, mock_asyncio_run):

    request = MagicMock()
    request.content_type = 'application/json'
    request.get_json.return_value = {}

    mock_async_orchestrator.return_value = ("Core logic failure", 500)
    mock_asyncio_run.side_effect = lambda coro: asyncio.get_event_loop().run_until_complete(coro)

    response, status_code = process_amber_data(request)

    assert status_code == 500
    assert response == "Core logic failure"

@patch(f"{PATCH_PREFIX}.asyncio.run")
@patch(f"{PATCH_PREFIX}._async_process_forecast_data", new_callable=AsyncMock)
def test_http_process_amber_data_unexpected_exception_in_http_handler(
    mock_async_orchestrator, mock_asyncio_run, mocker):

    # Ensure load_config is valid for this test, common_patches_forecast handles this.
    # mocker.patch(f"{PATCH_PREFIX}.load_config", return_value=MOCK_CONFIG) # Already handled

    request = MagicMock()
    request.content_type = 'application/json'
    request.get_json.return_value = {}

    # Simulate a crash within asyncio.run or the orchestrator if not caught by its own try-except
    mock_asyncio_run.side_effect = Exception("Unexpected HTTP boom")

    response, status_code = process_amber_data(request)

    assert status_code == 500
    assert "A critical unexpected error occurred: Unexpected HTTP boom" in response
    # If asyncio.run itself raises the exception before executing the coroutine:
    mock_async_orchestrator.assert_not_called()
