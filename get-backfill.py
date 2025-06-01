# Copyright (c) 2025, Sam Currie
# All rights reserved.
# 
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# TODO: Optimise Amber API call to get 7 days of data

import functions_framework
import json
import datetime
import os
import sys
import time
import asyncio
import aiohttp
import logging
from google.cloud import storage

# --- Global Configuration & Clients ---
# These are loaded from environment variables.
# Default values are placeholders and will cause the function to error if not overridden.
AMBER_SITE_ID = os.environ.get('AMBER_SITE_ID', 'your-amber-site-id')
AMBER_API_KEY = os.environ.get('AMBER_API_KEY', 'your-amber-api-key')
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'your-gcs-bucket-name')

storage_client = storage.Client() # Initialize GCS client globally for reuse

# --- Logging Setup ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Configure logging to output to stdout for Cloud Functions
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Retry Logic Constants (shared by both data fetching operations)
_MAX_RETRIES = 3
_INITIAL_RETRY_DELAY_SECONDS = 5
_RETRY_BACKOFF_FACTOR = 2
_SAFETY_BUFFER_SECONDS = 2
_DEFAULT_RETRY_SECONDS_FALLBACK = 60
_MAX_CONCURRENT_TASKS = 25 # Limit concurrent API calls/GCS operations

# --- Helper Functions ---

async def _fetch_data_from_amber_api(session, api_url, headers, data_description):
    """
    Fetches data from the Amber API with retry logic.
    Returns the JSON data list if successful, an empty list if API returns no data,
    or None if all retries fail or a critical error occurs.
    """
    amber_data = None
    for attempt in range(_MAX_RETRIES):
        logger.info(f"Calling Amber API for {data_description} (Attempt {attempt + 1}/{_MAX_RETRIES}): {api_url}")
        try:
            async with session.get(api_url, headers=headers) as response:
                if response.status == 429: # Rate limit
                    if attempt < _MAX_RETRIES - 1:
                        wait_time = _DEFAULT_RETRY_SECONDS_FALLBACK
                        rate_limit_reset_header = response.headers.get('RateLimit-Reset')
                        try:
                            if rate_limit_reset_header:
                                wait_time = int(rate_limit_reset_header) + _SAFETY_BUFFER_SECONDS
                                logger.warning(f"Rate limit (429) for {data_description}. RateLimit-Reset: {rate_limit_reset_header}. Retrying in {wait_time}s.")
                            else:
                                logger.warning(f"Rate limit (429) for {data_description}. No RateLimit-Reset. Using exponential backoff for {wait_time}s.")
                                wait_time = (_INITIAL_RETRY_DELAY_SECONDS * (_RETRY_BACKOFF_FACTOR ** attempt)) + _SAFETY_BUFFER_SECONDS
                        except ValueError:
                            logger.error(f"Rate limit (429) for {data_description}. Invalid RateLimit-Reset ('{rate_limit_reset_header}'). Using exponential backoff for {wait_time}s.")
                            wait_time = (_INITIAL_RETRY_DELAY_SECONDS * (_RETRY_BACKOFF_FACTOR ** attempt)) + _SAFETY_BUFFER_SECONDS
                        
                        await asyncio.sleep(wait_time)
                        continue # Go to the next attempt
                    else:
                        logger.error(f"Rate limit (429) for {data_description} on final attempt.")
                        return None # Indicate fetch failure

                response.raise_for_status()  # Raises ClientResponseError for other 4xx/5xx responses
                
                # Check for empty response before attempting JSON decode
                content = await response.read()
                if not content:
                    logger.info(f"Successfully fetched 0 records (empty response body) for {data_description} from Amber API.")
                    return [] # Successful fetch, but no data

                amber_data = json.loads(content) # Manually decode after reading
                logger.info(f"Successfully fetched {len(amber_data)} records for {data_description} from Amber API.")
                return amber_data  # Return data list (could be empty if API returns [])

        except aiohttp.ClientError as e: # Includes ClientResponseError
            logger.error(f"API call for {data_description} (attempt {attempt + 1}) failed: {e}")
            if attempt < _MAX_RETRIES - 1:
                wait_time = (_INITIAL_RETRY_DELAY_SECONDS * (_RETRY_BACKOFF_FACTOR ** attempt)) + _SAFETY_BUFFER_SECONDS
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"All API retries failed for {data_description}.")
                return None # Indicate fetch failure after all retries
        except json.JSONDecodeError as e:
            response_text_preview = content.decode(errors='ignore')[:500] if content else "N/A"
            logger.error(f"JSON decoding failed for {data_description} after successful request: {e}. Response text preview: {response_text_preview}...")
            # Not typically retried, as it indicates a malformed response from the server.
            return None # Indicate fetch failure (specifically decode error)

    return None # Should be unreachable if loop completes, but as a fallback


async def _save_data_to_gcs(data, site_id, bucket_name, gcs_client_instance, filename_prefix, file_date_str, data_description):
    """
    Converts data list to JSONL and uploads it to GCS.
    Returns the GCS path if successful.
    Raises an exception if the upload fails or data is not a list.
    """
    loop = asyncio.get_running_loop()
    if not isinstance(data, list):
        # This case should ideally be caught earlier if amber_data is None from fetch
        err_msg = f"Invalid data type for GCS upload for {data_description}. Expected list, got {type(data)}."
        logger.error(err_msg)
        raise ValueError(err_msg)

    # If data is an empty list, no file is created.
    if not data:
        logger.info(f"No data records provided for {data_description}, so no file will be saved to GCS.")
        return None # Indicate no file was saved

    try:
        jsonl_lines = [json.dumps(record) for record in data]
        jsonl_data = "\n".join(jsonl_lines)
        if jsonl_data: # Ensure a trailing newline for non-empty files
            jsonl_data += "\n"

        base_file_name = f"{filename_prefix}_{file_date_str}.jsonl"
        gcs_object_name = f"{site_id}/{base_file_name}" # Assumes site_id is a valid GCS directory prefix

        bucket = gcs_client_instance.bucket(bucket_name)
        blob = bucket.blob(gcs_object_name)

        logger.info(f"Uploading {data_description} file '{gcs_object_name}' to bucket '{bucket_name}'...")
        await loop.run_in_executor(None, blob.upload_from_string, jsonl_data, 'application/jsonl')
        logger.info(f"{data_description} file '{gcs_object_name}' uploaded successfully to GCS.")
        return f"gs://{bucket_name}/{gcs_object_name}"
    except Exception as e:
        logger.error(f"Failed to prepare or save {data_description} to GCS: {e}", exc_info=True)
        raise # Re-raise to be caught by the main handler for the task

async def _process_single_day_task(session, semaphore, date_val, task_config):
    """
    Processes a single task (e.g., prices for a specific day) asynchronously.
    Includes GCS check, API fetch, and GCS save.
    """
    async with semaphore: # Acquire semaphore to limit concurrency
        date_str_api_param = date_val.strftime('%Y-%m-%d')
        file_date_str_filename = date_val.strftime('%Y%m%d')
        task_description = f"{task_config['type_name']} for Site ID {AMBER_SITE_ID} on {date_str_api_param}"
        logger.info(f"--- Starting task: Processing {task_description} ---")

        # --- Check if file already exists in GCS (run blocking GCS call in executor) ---
        expected_base_filename = f"{task_config['filename_prefix']}_{file_date_str_filename}.jsonl"
        expected_gcs_object_name = f"{AMBER_SITE_ID}/{expected_base_filename}"
        
        bucket = storage_client.bucket(GCS_BUCKET_NAME) # GCS client is thread-safe for this
        blob = bucket.blob(expected_gcs_object_name)
        loop = asyncio.get_running_loop()
        
        try:
            exists = await loop.run_in_executor(None, blob.exists)
            if exists:
                skip_message = f"File 'gs://{GCS_BUCKET_NAME}/{expected_gcs_object_name}' already exists. Skipping fetch and save for {task_description}."
                logger.info(skip_message)
                logger.info(f"--- Finished task (skipped): Processing {task_description} ---")
                return {"status": "skipped", "message": skip_message}
        except Exception as e:
            err_msg = f"Error checking GCS for {expected_gcs_object_name}: {e}"
            logger.error(err_msg, exc_info=True)
            return {"status": "failed", "message": err_msg}

        # If file does not exist, proceed with fetching and saving
        headers = {'accept': 'application/json', 'Authorization': f'Bearer {AMBER_API_KEY}'}
        api_url = f'https://api.amber.com.au/v1/sites/{AMBER_SITE_ID}/{task_config["endpoint_path"]}?startDate={date_str_api_param}&endDate={date_str_api_param}&resolution=30'
        
        try:
            amber_data_list = await _fetch_data_from_amber_api(session, api_url, headers, task_description)

            if amber_data_list is not None:
                if not amber_data_list:
                    message = f"Successfully fetched 0 {task_config['type_name']} records for {AMBER_SITE_ID} on {date_str_api_param} (no data to save)."
                    logger.info(message)
                    return {"status": "success_no_data", "message": message}
                
                gcs_path = await _save_data_to_gcs(amber_data_list, AMBER_SITE_ID, GCS_BUCKET_NAME, storage_client, task_config["filename_prefix"], file_date_str_filename, task_description)
                message = f"Successfully processed {len(amber_data_list)} {task_config['type_name']} records for {date_str_api_param}. Saved to: {gcs_path}"
                logger.info(message)
                return {"status": "success", "message": message}
            else:
                failure_message = f"Failed to retrieve {task_description} from Amber API after multiple retries."
                logger.error(failure_message)
                return {"status": "failed", "message": failure_message}
        except Exception as e:
            error_log_message = f"An unexpected error occurred while processing {task_description}: {e}"
            logger.error(error_log_message, exc_info=True)
            return {"status": "failed", "message": f"Failed to process {task_description} due to: {e}"}
        finally:
            logger.info(f"--- Finished task: Processing {task_description} ---")

async def run_backfill_async():
    """
    Asynchronously processes historical data for the defined date range.
    """
    overall_successful_operations = []
    overall_failed_operations = []

    # --- Date Range Definition ---
    start_date_val = datetime.date(2020, 11, 26)
    end_date_val = datetime.date(2025, 5, 28)

    logger.info(f"Starting historical data backfill from {start_date_val.strftime('%Y-%m-%d')} to {end_date_val.strftime('%Y-%m-%d')}.")
    logger.warning("INFO: This process may take a significant time. Concurrency is limited to %s tasks.", _MAX_CONCURRENT_TASKS)

    # Define the data fetching tasks (could be moved outside if static)
    data_tasks_to_process_configs = [
        {"type_name": "actual prices", "endpoint_path": "prices", "filename_prefix": "amber_prices_actual"},
        {"type_name": "usage data", "endpoint_path": "usage", "filename_prefix": "amber_usage_data"}
    ]

    tasks_to_await = []
    semaphore = asyncio.Semaphore(_MAX_CONCURRENT_TASKS)

    async with aiohttp.ClientSession() as session:
        current_date = start_date_val
        while current_date <= end_date_val:
            logger.info(f"\n>>> Preparing tasks for date: {current_date.strftime('%Y-%m-%d')} <<<")
            for task_cfg in data_tasks_to_process_configs:
                # Create a coroutine for each task
                task_coro = _process_single_day_task(session, semaphore, current_date, task_cfg)
                tasks_to_await.append(task_coro)
            current_date += datetime.timedelta(days=1)

        # Run all tasks concurrently
        results = await asyncio.gather(*tasks_to_await, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            logger.error(f"A task failed with an unhandled exception: {result}", exc_info=result)
            overall_failed_operations.append(f"Task unhandled exception: {result}")
        elif isinstance(result, dict): # Expected result structure
            if result.get("status") in ["success", "success_no_data", "skipped"]:
                overall_successful_operations.append(result.get("message", "Unknown success"))
            else: # "failed"
                overall_failed_operations.append(result.get("message", "Unknown failure"))
        else: # Unexpected result type
            logger.error(f"Received unexpected result type from a task: {type(result)} - {str(result)[:200]}")
            overall_failed_operations.append(f"Unexpected task result: {str(result)[:200]}")

    # --- Construct Final Response ---
    final_status_message = f"Overall processing summary for Site ID {AMBER_SITE_ID} from {start_date_val.strftime('%Y-%m-%d')} to {end_date_val.strftime('%Y-%m-%d')}:\n"
    if overall_successful_operations:
        final_status_message += f"Successful/Skipped operations ({len(overall_successful_operations)}):\n" + "\n".join(overall_successful_operations) + "\n"
    if overall_failed_operations:
        final_status_message += f"Failed operations ({len(overall_failed_operations)}):\n" + "\n".join(overall_failed_operations) + "\n"

    if not overall_successful_operations and not overall_failed_operations:
        return f"No operations were recorded as successful or failed for Site ID {AMBER_SITE_ID} in the date range {start_date_val.strftime('%Y-%m-%d')} to {end_date_val.strftime('%Y-%m-%d')}. Please check logs for details.", 200
    elif overall_failed_operations:
        logger.error(f"Completed with errors: {final_status_message}")
        return final_status_message, 500
    else:
        logger.info(f"All tasks for all dates completed successfully or were skipped: {final_status_message}")
        return final_status_message, 200

@functions_framework.http
def process_historical_data(request):
    """
    HTTP Cloud Function entry point.
    Validates configuration and runs the asynchronous backfill process.
    """
    # --- Configuration Validation ---
    config_errors = []
    if not AMBER_SITE_ID or AMBER_SITE_ID == 'your-amber-site-id':
        config_errors.append("AMBER_SITE_ID not set or is default.")
    if not AMBER_API_KEY or AMBER_API_KEY == 'your-amber-api-key':
        config_errors.append("AMBER_API_KEY not set or is default.")
    if not GCS_BUCKET_NAME or GCS_BUCKET_NAME == 'your-gcs-bucket-name':
        config_errors.append("GCS_BUCKET_NAME not set or is default.")

    if config_errors:
        error_msg = "Configuration errors: " + " ".join(config_errors) + " Please set the required environment variables."
        logger.error(error_msg)
        return error_msg, 500

    try:
        return asyncio.run(run_backfill_async())
    except Exception as e: # Catch-all for unexpected errors during async setup or execution
        critical_error_message = f"A critical unexpected error occurred during the backfill process: {e}"
        logger.error(critical_error_message, exc_info=True)
        return critical_error_message, 500
