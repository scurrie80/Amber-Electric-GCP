# Copyright (c) 2025, Sam Currie
# All rights reserved.
# 
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# TODO: Optimise Amber API call to get 7 days of data

import functions_framework
import json # Still needed for json.dumps if not fully handled by save_to_gcs_async for all cases
import datetime
import os
import sys
import asyncio
# import aiohttp # No longer directly used, AsyncAmberClient handles it
# import logging # No longer directly used for setup
from google.cloud import storage

# If shared_utils.py is in the same directory (e.g. root) as get-backfill.py
sys.path.append(os.path.dirname(__file__))
# If shared_utils.py is in a parent directory (e.g. ./src/shared_utils.py and this is ./src/functions/get-backfill.py)
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from shared_utils import (
    load_config,
    setup_logger,
    AsyncAmberClient,
    save_to_gcs_async,
    check_gcs_file_exists_async
)

# --- Global Logger & Constants ---
logger = setup_logger() # Use shared logger setup
_MAX_CONCURRENT_TASKS = 25 # Limit concurrent API calls/GCS operations specific to backfill

# --- Core Logic Functions ---

async def _process_single_day_task(async_amber_client, semaphore, date_val, task_config_item, app_config, gcs_client):
    """
    Processes a single task (e.g., prices for a specific day) asynchronously.
    Includes GCS check, API fetch using AsyncAmberClient, and GCS save using save_to_gcs_async.
    """
    async with semaphore: # Acquire semaphore to limit concurrency
        date_str_api_param = date_val.strftime('%Y-%m-%d')
        file_date_str_filename = date_val.strftime('%Y%m%d')
        task_description = f"{task_config_item['type_name']} for Site ID {app_config['AMBER_SITE_ID']} on {date_str_api_param}"
        logger.info(f"--- Starting task: Processing {task_description} ---")

        expected_base_filename = f"{task_config_item['filename_prefix']}_{file_date_str_filename}.jsonl"
        expected_gcs_object_name = f"{app_config['AMBER_SITE_ID']}/{expected_base_filename}"
        
        try:
            # --- Check if file already exists in GCS ---
            file_exists = await check_gcs_file_exists_async(
                bucket_name=app_config['GCS_BUCKET_NAME'],
                object_name=expected_gcs_object_name,
                gcs_client=gcs_client
            )
            if file_exists:
                skip_message = f"File 'gs://{app_config['GCS_BUCKET_NAME']}/{expected_gcs_object_name}' already exists. Skipping fetch and save for {task_description}."
                logger.info(skip_message)
                return {"status": "skipped", "message": skip_message, "details": task_description}

            # --- If file does not exist, proceed with fetching ---
            # Default resolution, can be overridden by task_config_item or future global config
            resolution = task_config_item.get("resolution", 30)
            api_params = {
                "startDate": date_str_api_param,
                "endDate": date_str_api_param
                # Resolution is now passed directly to async_amber_client.get
            }
            logger.info(f"Fetching {task_description} from Amber API endpoint {task_config_item['endpoint_path']} with params {api_params} and resolution {resolution}")
            amber_data_list = await async_amber_client.get(
                task_config_item["endpoint_path"],
                params=api_params,
                resolution=resolution # Pass resolution to the client
            )

            if amber_data_list is None: # Should ideally be an exception from AsyncAmberClient
                # This path might not be hit if AsyncAmberClient always raises an exception on full failure.
                failure_message = f"Failed to retrieve {task_description} (API client returned None)."
                logger.error(failure_message)
                return {"status": "failed", "message": failure_message, "details": task_description}

            if not amber_data_list: # Successfully fetched, but API returned an empty list
                message = f"Successfully fetched 0 {task_config_item['type_name']} records for {app_config['AMBER_SITE_ID']} on {date_str_api_param} (no data to save)."
                logger.info(message)
                return {"status": "success_no_data", "message": message, "details": task_description}

            # --- Save Data to GCS ---
            gcs_path = await save_to_gcs_async(
                data=amber_data_list,
                bucket_name=app_config["GCS_BUCKET_NAME"],
                object_name=expected_gcs_object_name,
                gcs_client=gcs_client
            )
            message = f"Successfully processed {len(amber_data_list)} {task_config_item['type_name']} records for {date_str_api_param}. Saved to: {gcs_path}"
            logger.info(message)
            return {"status": "success", "message": message, "details": task_description}

        except Exception as e: # Catch errors from API client, GCS utils, or other unexpected issues
            error_log_message = f"An error occurred while processing {task_description}: {e}"
            logger.error(error_log_message, exc_info=True)
            return {"status": "failed", "message": error_log_message, "details": task_description}
        finally:
            logger.info(f"--- Finished task attempt: Processing {task_description} ---")


async def run_backfill_async(app_config, start_date_str: str = None, end_date_str: str = None):
    """
    Asynchronously processes historical data for a specified or default date range using shared utilities.
    Date strings should be in 'YYYY-MM-DD' format.
    """
    overall_successful_operations = []
    overall_failed_operations = []

    # Default date range if not provided
    default_start_date = datetime.date(2020, 11, 26) # Existing hardcoded start
    default_end_date = datetime.date(2025, 5, 28)   # Existing hardcoded end
    # Alternative default: default_end_date = datetime.date.today() - datetime.timedelta(days=1)


    start_date_val = default_start_date
    if start_date_str:
        try:
            start_date_val = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
        except ValueError:
            # Log warning and use default. Could also raise ValueError to cause a 400 error.
            logger.warning(f"Invalid start_date format: '{start_date_str}'. Using default: {default_start_date.strftime('%Y-%m-%d')}")
            # For now, let's allow fallback to default. To be stricter, uncomment next line.
            # raise ValueError(f"Invalid start_date format: '{start_date_str}'. Expected YYYY-MM-DD.")

    end_date_val = default_end_date
    if end_date_str:
        try:
            end_date_val = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except ValueError:
            logger.warning(f"Invalid end_date format: '{end_date_str}'. Using default: {default_end_date.strftime('%Y-%m-%d')}")
            # For now, let's allow fallback to default. To be stricter, uncomment next line.
            # raise ValueError(f"Invalid end_date format: '{end_date_str}'. Expected YYYY-MM-DD.")

    if start_date_val > end_date_val:
        # This is a critical error for backfill logic, should probably stop and inform.
        err_msg = f"Start date {start_date_val.strftime('%Y-%m-%d')} cannot be after end date {end_date_val.strftime('%Y-%m-%d')}."
        logger.error(err_msg)
        # To make this a client error, it needs to be caught by the HTTP entry point and returned as 400.
        # Raising ValueError here will be caught by the top-level handler in process_historical_data.
        raise ValueError(err_msg)

    logger.info(f"Starting historical data backfill for Site ID {app_config['AMBER_SITE_ID']} from {start_date_val.strftime('%Y-%m-%d')} to {end_date_val.strftime('%Y-%m-%d')}.")
    logger.warning(f"INFO: This process may take a significant time. Concurrency is limited to {_MAX_CONCURRENT_TASKS} tasks.")

    # Initialize clients
    async_amber_client = AsyncAmberClient(api_key=app_config['AMBER_API_KEY'], site_id=app_config['AMBER_SITE_ID'])
    gcs_client = storage.Client() # GCS client can be reused

    data_tasks_to_process_configs = [
        {"type_name": "actual prices", "endpoint_path": "prices", "filename_prefix": "amber_prices_actual"},
        {"type_name": "usage data", "endpoint_path": "usage", "filename_prefix": "amber_usage_data"}
    ]

    tasks_to_await = []
    semaphore = asyncio.Semaphore(_MAX_CONCURRENT_TASKS)

    current_date = start_date_val
    while current_date <= end_date_val:
        logger.info(f"\n>>> Preparing tasks for date: {current_date.strftime('%Y-%m-%d')} <<<")
        for task_cfg in data_tasks_to_process_configs:
            task_coro = _process_single_day_task(
                async_amber_client,
                semaphore,
                current_date,
                task_cfg,
                app_config,
                gcs_client
            )
            tasks_to_await.append(task_coro)
        current_date += datetime.timedelta(days=1)

    # Run all tasks concurrently
    results = await asyncio.gather(*tasks_to_await, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            logger.error(f"A task failed with an unhandled exception: {result}", exc_info=result)
            overall_failed_operations.append(f"Task unhandled exception: {result} (Details: N/A, check logs)")
        elif isinstance(result, dict):
            details = result.get("details", "N/A")
            if result.get("status") in ["success", "success_no_data", "skipped"]:
                overall_successful_operations.append(f"{result.get('message', 'Unknown success')} (Task: {details})")
            else: # "failed"
                overall_failed_operations.append(f"{result.get('message', 'Unknown failure')} (Task: {details})")
        else:
            logger.error(f"Received unexpected result type from a task: {type(result)} - {str(result)[:200]}")
            overall_failed_operations.append(f"Unexpected task result: {str(result)[:200]}")

    # --- Construct Final Response ---
    final_status_message = f"Overall processing summary for Site ID {app_config['AMBER_SITE_ID']} from {start_date_val.strftime('%Y-%m-%d')} to {end_date_val.strftime('%Y-%m-%d')}:\n"
    if overall_successful_operations:
        final_status_message += f"Successful/Skipped operations ({len(overall_successful_operations)}):\n" + "\n".join(overall_successful_operations) + "\n"
    if overall_failed_operations:
        final_status_message += f"Failed operations ({len(overall_failed_operations)}):\n" + "\n".join(overall_failed_operations) + "\n"

    status_code = 200
    if not overall_successful_operations and not overall_failed_operations:
        final_status_message += f"No operations were recorded as successful or failed. Please check logs for details."
    elif overall_failed_operations:
        logger.error(f"Backfill completed with errors for Site ID {app_config['AMBER_SITE_ID']}:\n{final_status_message}")
        status_code = 500 # Indicate partial or full failure
    else:
        logger.info(f"All backfill tasks for Site ID {app_config['AMBER_SITE_ID']} completed successfully or were skipped:\n{final_status_message}")

    return final_status_message, status_code

@functions_framework.http
def process_historical_data(request):
    """
    HTTP Cloud Function entry point.
    Validates configuration and runs the asynchronous backfill process using shared utilities.
    """
    try:
        app_config = load_config() # Loads and validates essential configs from shared_utils

        start_date_req_str = None
        end_date_req_str = None

        # Attempt to get dates from request JSON body
        if request and request.content_type == 'application/json':
            try:
                # Use silent=True to avoid raising an exception on empty or malformed JSON
                req_json = request.get_json(silent=True)
                if req_json:
                    start_date_req_str = req_json.get('start_date')
                    end_date_req_str = req_json.get('end_date')

                    # Basic validation of date formats if provided, can be more robust
                    if start_date_req_str:
                        datetime.datetime.strptime(start_date_req_str, '%Y-%m-%d')
                    if end_date_req_str:
                        datetime.datetime.strptime(end_date_req_str, '%Y-%m-%d')
                else:
                    # Log if JSON is empty or malformed but content_type was application/json
                    logger.info("Request content-type is application/json but JSON body is empty or malformed.")

            except ValueError as ve_date: # Catches strptime errors
                 logger.warning(f"Invalid date format in request JSON: {ve_date}", exc_info=True)
                 return f"Invalid date format in request JSON. Please use YYYY-MM-DD. Error: {ve_date}", 400
        else:
            logger.info("No JSON body found in request or content type not application/json. Using default date range for backfill if any.")

        return asyncio.run(run_backfill_async(app_config, start_date_str=start_date_req_str, end_date_str=end_date_req_str))

    except ValueError as ve:
        # Catches errors from load_config OR date validation errors re-raised from run_backfill_async
        logger.critical(f"Input or Configuration error: {ve}", exc_info=True)
        # Distinguish between server error (config) and client error (bad date format/range)
        if "Missing essential configuration" in str(ve):
            return f"Configuration error: {ve}", 500
        else: # Assume other ValueErrors are client-related (e.g. bad date format/range)
            return f"Invalid input error: {ve}", 400

    except Exception as e: # Catch-all for other unexpected errors
        critical_error_message = f"A critical unexpected error occurred: {e}"
        logger.critical(critical_error_message, exc_info=True)
        return f"Internal Server Error: {critical_error_message}", 500
