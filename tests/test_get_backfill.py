import pytest
import asyncio
import datetime
from unittest.mock import patch, MagicMock, AsyncMock, call

from get_backfill import _process_single_day_task, run_backfill_async, process_historical_data
from get_backfill import DATA_TASKS_TO_PROCESS_CONFIGS, DEFAULT_START_DATE, DEFAULT_END_DATE

try:
    from google.cloud import exceptions as gcs_exceptions
except ImportError:
    class GCSMockError(Exception):
        pass
    gcs_exceptions = MagicMock()
    gcs_exceptions.GoogleCloudError = GCSMockError

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

MOCK_CONFIG = {
    "AMBER_SITE_ID": "test_site", "AMBER_API_KEY": "test_key", "GCS_BUCKET_NAME": "test_bucket"
}
TEST_DATE_VAL = datetime.date(2023, 1, 15)
TEST_DATE_STR_FILENAME = TEST_DATE_VAL.strftime("%Y%m%d")
TEST_DATE_STR_API = TEST_DATE_VAL.strftime("%Y-%m-%d")
PATCH_PREFIX = "get_backfill"

@pytest.fixture
def mock_gcs_client_backfill():
    return MagicMock()

@pytest.fixture(autouse=True)
def common_patches_backfill(mocker, mock_gcs_client_backfill):
    mocker.patch(f"{PATCH_PREFIX}.load_config", return_value=MOCK_CONFIG)
    mocker.patch(f"{PATCH_PREFIX}.storage.Client", return_value=mock_gcs_client_backfill)
    mocker.patch(f"{PATCH_PREFIX}.setup_logger", return_value=MagicMock())

TASK_CONFIG_PRICES = DATA_TASKS_TO_PROCESS_CONFIGS[0]
TASK_CONFIG_USAGE = DATA_TASKS_TO_PROCESS_CONFIGS[1]

@pytest.mark.asyncio
async def test_process_single_day_success_file_does_not_exist(mock_gcs_client_backfill):
    mock_amber_client = AsyncMock()
    mock_amber_client.get.return_value = [{'data': 'price1'}]
    semaphore = asyncio.Semaphore(1)
    mock_check_gcs = AsyncMock(return_value=False)
    mock_save_gcs = AsyncMock(return_value=f"gs://{MOCK_CONFIG['GCS_BUCKET_NAME']}/{MOCK_CONFIG['AMBER_SITE_ID']}/{TASK_CONFIG_PRICES['filename_prefix']}_{TEST_DATE_STR_FILENAME}.jsonl")
    expected_object_name = f"{MOCK_CONFIG['AMBER_SITE_ID']}/{TASK_CONFIG_PRICES['filename_prefix']}_{TEST_DATE_STR_FILENAME}.jsonl"
    with patch(f"{PATCH_PREFIX}.check_gcs_file_exists_async", mock_check_gcs), \
         patch(f"{PATCH_PREFIX}.save_to_gcs_async", mock_save_gcs):
        result = await _process_single_day_task(mock_amber_client, semaphore, TEST_DATE_VAL, TASK_CONFIG_PRICES, MOCK_CONFIG, mock_gcs_client_backfill)
    mock_check_gcs.assert_called_once_with(MOCK_CONFIG["GCS_BUCKET_NAME"], expected_object_name, mock_gcs_client_backfill)
    mock_amber_client.get.assert_called_once_with(TASK_CONFIG_PRICES["endpoint_path"], params={"startDate": TEST_DATE_STR_API, "endDate": TEST_DATE_STR_API}, resolution=TASK_CONFIG_PRICES["resolution"])
    mock_save_gcs.assert_called_once_with([{'data': 'price1'}], MOCK_CONFIG["GCS_BUCKET_NAME"], expected_object_name, mock_gcs_client_backfill)
    assert result['status'] == "success"
    assert result['gcs_path'] == f"gs://{MOCK_CONFIG['GCS_BUCKET_NAME']}/{MOCK_CONFIG['AMBER_SITE_ID']}/{TASK_CONFIG_PRICES['filename_prefix']}_{TEST_DATE_STR_FILENAME}.jsonl"

@pytest.mark.asyncio
async def test_process_single_day_file_exists_skipped(mock_gcs_client_backfill):
    mock_amber_client = AsyncMock(); semaphore = asyncio.Semaphore(1)
    mock_check_gcs = AsyncMock(return_value=True); mock_save_gcs = AsyncMock()
    expected_object_name = f"{MOCK_CONFIG['AMBER_SITE_ID']}/{TASK_CONFIG_PRICES['filename_prefix']}_{TEST_DATE_STR_FILENAME}.jsonl"
    with patch(f"{PATCH_PREFIX}.check_gcs_file_exists_async", mock_check_gcs), patch(f"{PATCH_PREFIX}.save_to_gcs_async", mock_save_gcs):
        result = await _process_single_day_task(mock_amber_client, semaphore, TEST_DATE_VAL, TASK_CONFIG_PRICES, MOCK_CONFIG, mock_gcs_client_backfill)
    mock_check_gcs.assert_called_once_with(MOCK_CONFIG["GCS_BUCKET_NAME"], expected_object_name, mock_gcs_client_backfill)
    mock_amber_client.get.assert_not_called(); mock_save_gcs.assert_not_called()
    assert result['status'] == "skipped"

@pytest.mark.asyncio
async def test_process_single_day_api_returns_empty_list(mock_gcs_client_backfill):
    mock_amber_client = AsyncMock(); mock_amber_client.get.return_value = []
    semaphore = asyncio.Semaphore(1); mock_check_gcs = AsyncMock(return_value=False); mock_save_gcs = AsyncMock()
    with patch(f"{PATCH_PREFIX}.check_gcs_file_exists_async", mock_check_gcs), patch(f"{PATCH_PREFIX}.save_to_gcs_async", mock_save_gcs):
        result = await _process_single_day_task(mock_amber_client, semaphore, TEST_DATE_VAL, TASK_CONFIG_PRICES, MOCK_CONFIG, mock_gcs_client_backfill)
    mock_amber_client.get.assert_called_once(); mock_save_gcs.assert_not_called()
    assert result['status'] == "success_no_data"

@pytest.mark.asyncio
async def test_process_single_day_api_error(mock_gcs_client_backfill):
    mock_amber_client = AsyncMock()
    mock_amber_client.get.side_effect = ClientResponseError(status=500, message='API Error', headers=None, history=None, request_info=MagicMock())
    semaphore = asyncio.Semaphore(1); mock_check_gcs = AsyncMock(return_value=False); mock_save_gcs = AsyncMock()
    with patch(f"{PATCH_PREFIX}.check_gcs_file_exists_async", mock_check_gcs), patch(f"{PATCH_PREFIX}.save_to_gcs_async", mock_save_gcs):
        result = await _process_single_day_task(mock_amber_client, semaphore, TEST_DATE_VAL, TASK_CONFIG_PRICES, MOCK_CONFIG, mock_gcs_client_backfill)
    mock_amber_client.get.assert_called_once(); mock_save_gcs.assert_not_called()
    assert result['status'] == "failed"; assert "API Error" in result['message']

@pytest.mark.asyncio
async def test_process_single_day_gcs_save_error(mock_gcs_client_backfill):
    mock_amber_client = AsyncMock(); mock_amber_client.get.return_value = [{'data': 'price1'}]
    semaphore = asyncio.Semaphore(1); mock_check_gcs = AsyncMock(return_value=False)
    mock_save_gcs = AsyncMock(side_effect=gcs_exceptions.GoogleCloudError("GCS Save Error"))
    with patch(f"{PATCH_PREFIX}.check_gcs_file_exists_async", mock_check_gcs), patch(f"{PATCH_PREFIX}.save_to_gcs_async", mock_save_gcs):
        result = await _process_single_day_task(mock_amber_client, semaphore, TEST_DATE_VAL, TASK_CONFIG_PRICES, MOCK_CONFIG, mock_gcs_client_backfill)
    mock_amber_client.get.assert_called_once(); mock_save_gcs.assert_called_once()
    assert result['status'] == "failed"; assert "GCS Save Error" in result['message']

@pytest.mark.asyncio
async def test_process_single_day_task_config_resolution_override(mock_gcs_client_backfill):
    mock_amber_client = AsyncMock(); mock_amber_client.get.return_value = [{'data': 'price1'}]
    semaphore = asyncio.Semaphore(1)
    task_config_custom_res = {"type_name": "actual prices", "endpoint_path": "prices/historical", "filename_prefix": "amber_prices_actual", "resolution": 60}
    mock_check_gcs = AsyncMock(return_value=False); mock_save_gcs = AsyncMock(return_value="gs://bucket/file_60min.jsonl")
    with patch(f"{PATCH_PREFIX}.check_gcs_file_exists_async", mock_check_gcs), patch(f"{PATCH_PREFIX}.save_to_gcs_async", mock_save_gcs):
        result = await _process_single_day_task(mock_amber_client, semaphore, TEST_DATE_VAL, task_config_custom_res, MOCK_CONFIG, mock_gcs_client_backfill)
    mock_amber_client.get.assert_called_once_with(task_config_custom_res["endpoint_path"], params={"startDate": TEST_DATE_STR_API, "endDate": TEST_DATE_STR_API}, resolution=60)
    assert result['status'] == "success"

@pytest.mark.asyncio
@patch(f"{PATCH_PREFIX}._process_single_day_task", new_callable=AsyncMock)
@patch(f"{PATCH_PREFIX}.AsyncAmberClient")
async def test_run_backfill_async_success_one_day_two_tasks(MockAsyncAmberClient, mock_process_task, mock_gcs_client_backfill):
    mock_amber_instance = MockAsyncAmberClient.return_value
    mock_process_task.return_value = {"status": "success", "message": "Processed item X", "type": "item type", "date": TEST_DATE_VAL, "details": "Details for X"}
    start_date_str, end_date_str = "2023-01-15", "2023-01-15"
    summary, status_code = await run_backfill_async(MOCK_CONFIG, start_date_str, end_date_str, mock_gcs_client_backfill)
    MockAsyncAmberClient.assert_called_once_with(api_key=MOCK_CONFIG["AMBER_API_KEY"], site_id=MOCK_CONFIG["AMBER_SITE_ID"])
    assert mock_process_task.call_count == 2
    expected_date = datetime.date(2023, 1, 15)
    calls = [call(mock_amber_instance, unittest.mock.ANY, expected_date, DATA_TASKS_TO_PROCESS_CONFIGS[0], MOCK_CONFIG, mock_gcs_client_backfill),
             call(mock_amber_instance, unittest.mock.ANY, expected_date, DATA_TASKS_TO_PROCESS_CONFIGS[1], MOCK_CONFIG, mock_gcs_client_backfill)]
    mock_process_task.assert_has_calls(calls, any_order=True)
    assert status_code == 200; assert "Overall processing summary" in summary; assert "Successful/Skipped operations (2):" in summary

@pytest.mark.asyncio
@patch(f"{PATCH_PREFIX}._process_single_day_task", new_callable=AsyncMock)
@patch(f"{PATCH_PREFIX}.AsyncAmberClient")
async def test_run_backfill_async_date_range_calls(MockAsyncAmberClient, mock_process_task, mock_gcs_client_backfill):
    mock_process_task.return_value = {"status": "success", "message": "OK", "type": "t", "date": "d"}
    start_date_str, end_date_str = "2023-01-15", "2023-01-17"
    await run_backfill_async(MOCK_CONFIG, start_date_str, end_date_str, mock_gcs_client_backfill)
    assert mock_process_task.call_count == 6
    dates_called = sorted([args[2] for args, kwargs in mock_process_task.call_args_list if kwargs['task_config_item'] == DATA_TASKS_TO_PROCESS_CONFIGS[0]])
    assert dates_called == [datetime.date(2023,1,15), datetime.date(2023,1,16), datetime.date(2023,1,17)]

@pytest.mark.asyncio
@patch(f"{PATCH_PREFIX}._process_single_day_task", new_callable=AsyncMock)
@patch(f"{PATCH_PREFIX}.AsyncAmberClient")
async def test_run_backfill_async_one_task_fails(MockAsyncAmberClient, mock_process_task, mock_gcs_client_backfill):
    mock_process_task.side_effect = [{"status": "success", "message": "Prices good", "type": "prices", "date": TEST_DATE_VAL},
                                     {"status": "failed", "message": "Usage bad", "type": "usage", "date": TEST_DATE_VAL}]
    start_date_str, end_date_str = "2023-01-15", "2023-01-15"
    summary, status_code = await run_backfill_async(MOCK_CONFIG, start_date_str, end_date_str, mock_gcs_client_backfill)
    assert status_code == 500; assert "Successful/Skipped operations (1):" in summary; assert "Failed operations (1):" in summary

@pytest.mark.asyncio
@patch(f"{PATCH_PREFIX}._process_single_day_task", new_callable=AsyncMock)
@patch(f"{PATCH_PREFIX}.AsyncAmberClient")
async def test_run_backfill_async_process_task_raises_exception(MockAsyncAmberClient, mock_process_task, mock_gcs_client_backfill):
    mock_process_task.side_effect = Exception("Unexpected task error")
    start_date_str, end_date_str = "2023-01-15", "2023-01-15"
    summary, status_code = await run_backfill_async(MOCK_CONFIG, start_date_str, end_date_str, mock_gcs_client_backfill)
    assert status_code == 500; assert mock_process_task.call_count == 2; assert "Failed operations (2):" in summary
    assert "Task unhandled exception: Exception('Unexpected task error')" in summary

@pytest.mark.asyncio
@patch(f"{PATCH_PREFIX}._process_single_day_task", new_callable=AsyncMock)
async def test_run_backfill_async_invalid_date_range_start_after_end(mock_process_task, mock_gcs_client_backfill):
    start_date_str, end_date_str = "2023-01-15", "2023-01-14"
    with pytest.raises(ValueError, match="Start date 2023-01-15 cannot be after end date 2023-01-14."):
        await run_backfill_async(MOCK_CONFIG, start_date_str, end_date_str, mock_gcs_client_backfill)
    mock_process_task.assert_not_called()

@pytest.mark.asyncio
@patch(f"{PATCH_PREFIX}._process_single_day_task", new_callable=AsyncMock)
@patch(f"{PATCH_PREFIX}.AsyncAmberClient")
async def test_run_backfill_async_invalid_date_format_start(MockAsyncAmberClient, mock_process_task, mock_gcs_client_backfill, caplog, mocker):
    start_date_str, end_date_str = "2023/01/15", "2023-01-15"
    mocker.patch(f"{PATCH_PREFIX}.DEFAULT_START_DATE", datetime.date(2023, 1, 15))
    mock_process_task.return_value = {"status": "success", "message": "OK", "type": "t", "date": datetime.date(2023,1,15)}
    await run_backfill_async(MOCK_CONFIG, start_date_str, end_date_str, mock_gcs_client_backfill)
    assert "Invalid start_date format: 2023/01/15. Using default start date" in caplog.text
    assert mock_process_task.call_count == 2
    assert mock_process_task.call_args_list[0][0][2] == datetime.date(2023, 1, 15)

@pytest.mark.asyncio
@patch(f"{PATCH_PREFIX}._process_single_day_task", new_callable=AsyncMock)
@patch(f"{PATCH_PREFIX}.AsyncAmberClient")
async def test_run_backfill_async_no_dates_uses_defaults(MockAsyncAmberClient, mock_process_task, mock_gcs_client_backfill, mocker):
    mocked_default_start, mocked_default_end = datetime.date(2023, 1, 1), datetime.date(2023, 1, 2)
    mocker.patch(f"{PATCH_PREFIX}.DEFAULT_START_DATE", mocked_default_start)
    mocker.patch(f"{PATCH_PREFIX}.DEFAULT_END_DATE", mocked_default_end)
    mock_process_task.return_value = {"status": "success", "message": "OK", "type": "t", "date": "d"}
    await run_backfill_async(MOCK_CONFIG, None, None, mock_gcs_client_backfill)
    assert mock_process_task.call_count == 4
    first_day_calls = sorted([c for c in mock_process_task.call_args_list if c[0][2] == mocked_default_start], key=lambda c: c[0][3]['type_name'])
    second_day_calls = sorted([c for c in mock_process_task.call_args_list if c[0][2] == mocked_default_end], key=lambda c: c[0][3]['type_name'])
    assert len(first_day_calls) == 2 and first_day_calls[0][0][2] == mocked_default_start
    assert len(second_day_calls) == 2 and second_day_calls[0][0][2] == mocked_default_end

# --- Tests for HTTP Entry Point process_historical_data ---

@patch(f"{PATCH_PREFIX}.asyncio.run")
@patch(f"{PATCH_PREFIX}.run_backfill_async", new_callable=AsyncMock)
def test_http_process_historical_data_success_no_dates(
    mock_run_backfill, mock_asyncio_run, mock_gcs_client_backfill, mocker): # Added mocker for load_config access if needed
    # common_patches_backfill fixture already mocks load_config and storage.Client
    # It also gives us mock_gcs_client_backfill if run_backfill_async needs it directly (it does)

    request = MagicMock()
    request.content_type = 'application/json'
    request.get_json.return_value = {}

    mock_run_backfill.return_value = ("Backfill summary success", 200)
    # Make asyncio.run execute the coroutine and return its result
    mock_asyncio_run.side_effect = lambda coro: asyncio.get_event_loop().run_until_complete(coro)

    # Ensure load_config returns the MOCK_CONFIG for this specific path if not already handled by autouse
    # This is mainly for clarity, common_patches_backfill should handle it.
    mocker.patch(f"{PATCH_PREFIX}.load_config", return_value=MOCK_CONFIG)


    response, status_code = process_historical_data(request)

    # run_backfill_async is called with (config, start_date_str, end_date_str, gcs_client)
    # config is from load_config(), gcs_client from storage.Client()
    # We need to ensure these mocks are seen by process_historical_data
    mock_run_backfill.assert_called_once()
    args, _ = mock_run_backfill.call_args
    assert args[0] == MOCK_CONFIG
    assert args[1] is None # start_date_str
    assert args[2] is None # end_date_str
    assert args[3] == mock_gcs_client_backfill # gcs_client instance from common_patches_backfill

    assert status_code == 200
    assert response == "Backfill summary success"


@patch(f"{PATCH_PREFIX}.asyncio.run")
@patch(f"{PATCH_PREFIX}.run_backfill_async", new_callable=AsyncMock)
def test_http_process_historical_data_success_with_dates(
    mock_run_backfill, mock_asyncio_run, mock_gcs_client_backfill): # mock_gcs_client_backfill from fixture

    request = MagicMock()
    request.content_type = 'application/json'
    request.get_json.return_value = {"start_date": "2023-01-01", "end_date": "2023-01-10"}

    mock_run_backfill.return_value = ("Backfill date range success", 200)
    mock_asyncio_run.side_effect = lambda coro: asyncio.get_event_loop().run_until_complete(coro)

    response, status_code = process_historical_data(request)

    mock_run_backfill.assert_called_once()
    args, _ = mock_run_backfill.call_args
    assert args[0] == MOCK_CONFIG
    assert args[1] == "2023-01-01"
    assert args[2] == "2023-01-10"
    assert args[3] == mock_gcs_client_backfill

    assert status_code == 200
    assert response == "Backfill date range success"

@patch(f"{PATCH_PREFIX}.run_backfill_async", new_callable=AsyncMock) # To ensure it's not called
def test_http_process_historical_data_invalid_date_format_in_json(mock_run_backfill):
    request = MagicMock()
    request.content_type = 'application/json'
    request.get_json.return_value = {"start_date": "2023/01/01"} # Invalid format

    response, status_code = process_historical_data(request)

    assert status_code == 400
    assert "Invalid date format for start_date. Please use YYYY-MM-DD." in response
    mock_run_backfill.assert_not_called()

@patch(f"{PATCH_PREFIX}.asyncio.run")
@patch(f"{PATCH_PREFIX}.run_backfill_async", new_callable=AsyncMock)
def test_http_process_historical_data_no_json_body_uses_defaults(
    mock_run_backfill, mock_asyncio_run, mock_gcs_client_backfill):

    request = MagicMock()
    request.content_type = 'text/plain'
    request.get_json.return_value = None # As per silent=True

    mock_run_backfill.return_value = ("Backfill no JSON success", 200)
    mock_asyncio_run.side_effect = lambda coro: asyncio.get_event_loop().run_until_complete(coro)

    response, status_code = process_historical_data(request)

    mock_run_backfill.assert_called_once()
    args, _ = mock_run_backfill.call_args
    assert args[1] is None # start_date_str
    assert args[2] is None # end_date_str
    assert status_code == 200
    assert response == "Backfill no JSON success"

@patch(f"{PATCH_PREFIX}.run_backfill_async", new_callable=AsyncMock) # To ensure it's not called
def test_http_process_historical_data_config_error(mock_run_backfill, mocker):
    # Override common_patches_backfill's load_config for this test
    mocker.patch(f"{PATCH_PREFIX}.load_config", side_effect=ValueError("Missing Backfill Config"))

    request = MagicMock()
    response, status_code = process_historical_data(request)

    assert status_code == 500 # As per current code for load_config errors
    assert "Configuration error: Missing Backfill Config" in response
    mock_run_backfill.assert_not_called()

@patch(f"{PATCH_PREFIX}.asyncio.run")
@patch(f"{PATCH_PREFIX}.run_backfill_async", new_callable=AsyncMock)
def test_http_process_historical_data_run_backfill_raises_valueerror_for_date_range(
    mock_run_backfill, mock_asyncio_run, mock_gcs_client_backfill):

    request = MagicMock()
    request.content_type = 'application/json'
    request.get_json.return_value = {"start_date": "2023-01-15", "end_date": "2023-01-14"}

    # Configure run_backfill_async mock to raise ValueError as it would for this case
    mock_run_backfill.side_effect = ValueError("Start date cannot be after end date.")
    mock_asyncio_run.side_effect = lambda coro: asyncio.get_event_loop().run_until_complete(coro)

    response, status_code = process_historical_data(request)

    assert status_code == 400 # As per current code for this type of ValueError
    assert "Invalid input error: Start date cannot be after end date." in response
    mock_run_backfill.assert_called_once() # It is called, and then it raises the error

@patch(f"{PATCH_PREFIX}.asyncio.run")
@patch(f"{PATCH_PREFIX}.run_backfill_async", new_callable=AsyncMock)
def test_http_process_historical_data_run_backfill_returns_error_status(
    mock_run_backfill, mock_asyncio_run):

    request = MagicMock()
    mock_run_backfill.return_value = ("Backfill failed summary", 500)
    mock_asyncio_run.side_effect = lambda coro: asyncio.get_event_loop().run_until_complete(coro)

    response, status_code = process_historical_data(request)

    assert status_code == 500
    assert response == "Backfill failed summary"

@patch(f"{PATCH_PREFIX}.asyncio.run")
@patch(f"{PATCH_PREFIX}.run_backfill_async", new_callable=AsyncMock) # Mocked but asyncio.run is the one failing
def test_http_process_historical_data_unexpected_exception(
    mock_run_backfill, mock_asyncio_run, mocker):

    mocker.patch(f"{PATCH_PREFIX}.load_config", return_value=MOCK_CONFIG) # Ensure load_config is fine

    request = MagicMock()
    mock_asyncio_run.side_effect = Exception("Super unexpected")

    response, status_code = process_historical_data(request)

    assert status_code == 500
    assert "Internal Server Error: A critical unexpected error occurred: Super unexpected" in response
    # If asyncio.run itself raises the exception before executing the coroutine:
    mock_run_backfill.assert_not_called()
