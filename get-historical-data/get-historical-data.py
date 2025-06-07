# Copyright (c) 2025, Sam Currie
# All rights reserved.
# 
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import functions_framework
import datetime
import os
import sys
import json # Kept for general JSON ops if any, but shared_utils handles API JSON
import asyncio

from google.cloud import storage

# Adjust path to import from parent directory
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from shared_utils import (
    load_config,
    setup_logger,
    AsyncAmberClient,
    save_to_gcs_async,
    check_gcs_file_exists_async
)

# --- Global Clients and Logger ---
# GCS client can be initialized globally or per function call. Global is fine for Cloud Functions.
storage_client = storage.Client()
logger = setup_logger() # Use shared logger setup

# --- Async Core Logic ---
async def _async_process_historical_data(config, storage_client_instance, resolution_minutes: int = 30):
    """
    Core asynchronous logic to fetch historical data from Amber API and save to GCS.
    Includes a check to see if the data already exists in GCS.
    Resolution can be specified.
    """
    successful_tasks_details = []
    failed_tasks_details = []

    amber_client = AsyncAmberClient(api_key=config["AMBER_API_KEY"], site_id=config["AMBER_SITE_ID"])

    # Date for the API call (yesterday)
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    date_str_api_param = yesterday.strftime('%Y-%m-%d') # For API query parameter
    file_date_str_filename = yesterday.strftime('%Y%m%d')   # For filename component

    data_tasks_to_process = [
        {
            "type_name": "actual prices",
            "endpoint_path": "prices", # Relative path for AsyncAmberClient
            "filename_prefix": "amber_prices_actual"
        },
        {
            "type_name": "usage data",
            "endpoint_path": "usage",
            "filename_prefix": "amber_usage_data"
        }
    ]

    for task_config in data_tasks_to_process:
        task_description = f"{task_config['type_name']} for Site ID {config['AMBER_SITE_ID']} on {date_str_api_param}"
        logger.info(f"--- Starting task: Processing {task_description} ---")

        base_file_name = f"{task_config['filename_prefix']}_{file_date_str_filename}.jsonl"
        gcs_object_name = f"{config['AMBER_SITE_ID']}/{base_file_name}"

        try:
            # Check if file already exists in GCS
            file_exists = await check_gcs_file_exists_async(
                bucket_name=config["GCS_BUCKET_NAME"],
                object_name=gcs_object_name,
                gcs_client=storage_client_instance
            )

            if file_exists:
                message = f"Data for {task_description} already exists at gs://{config['GCS_BUCKET_NAME']}/{gcs_object_name}. Skipping fetch and save."
                logger.info(message)
                successful_tasks_details.append(message)
                logger.info(f"--- Finished task: Processing {task_description} (skipped) ---")
                continue

            # Construct API parameters for the Amber client
            # Resolution is now passed as a direct argument to the client's get method
            api_params = {
                "startDate": date_str_api_param,
                "endDate": date_str_api_param
            }

            logger.info(f"Fetching {task_description} from Amber API endpoint {task_config['endpoint_path']} with params {api_params} and resolution {resolution_minutes}min")
            amber_data_list = await amber_client.get(
                task_config["endpoint_path"],
                params=api_params,
                resolution=resolution_minutes # Pass resolution to client
            )

            if amber_data_list is not None: # API fetch attempt concluded
                if not amber_data_list: # Successfully fetched, but API returned an empty list
                     message = f"Successfully fetched 0 {task_config['type_name']} records for {config['AMBER_SITE_ID']} on {date_str_api_param} (no data to save)."
                     logger.info(message)
                     successful_tasks_details.append(message)
                else: # Successfully fetched data
                    gcs_path = await save_to_gcs_async(
                        data=amber_data_list,
                        bucket_name=config["GCS_BUCKET_NAME"],
                        object_name=gcs_object_name,
                        gcs_client=storage_client_instance
                    )
                    message = f"Successfully processed {len(amber_data_list)} {task_config['type_name']} records. Saved to: {gcs_path}"
                    logger.info(message)
                    successful_tasks_details.append(message)
            else: # amber_client.get returned None or raised an exception that was caught by its retry logic and resulted in None (should ideally raise)
                  # Assuming AsyncAmberClient raises an exception on failure after retries.
                  # This 'else' block might not be hit if AsyncAmberClient always raises.
                failure_message = f"Failed to retrieve {task_description} from Amber API after multiple retries (client returned None)."
                logger.error(failure_message)
                failed_tasks_details.append(failure_message)

        except Exception as e: # Catch errors from API client, GCS utils, or other unexpected issues for this task
            error_log_message = f"An unexpected error occurred while processing {task_description}: {e}"
            logger.error(error_log_message, exc_info=True)
            failed_tasks_details.append(f"Failed to process {task_description} due to: {str(e)}")
        logger.info(f"--- Finished task: Processing {task_description} ---")

    return successful_tasks_details, failed_tasks_details

@functions_framework.http
def process_historical_data(request):
    """
    HTTP Cloud Function that calls the Amber Electric API for historical
    price actuals and usage data for the previous day, converts responses
    to JSONL, and saves them to GCS. Uses shared utilities for config, logging, API client, and GCS operations.
    """
    try:
        config = load_config() # Loads and validates essential configs

        # Initialize GCS client (can be global or here)
        # If global `storage_client` is used, ensure it's initialized.
        # For clarity in this function, creating/passing an instance.
        gcs_client_instance = storage.Client()

        resolution_req = 30 # Default resolution
        if request and request.content_type == 'application/json':
            try:
                req_json = request.get_json(silent=True)
                if req_json:
                    resolution_req = int(req_json.get('resolution_minutes', resolution_req))
                else:
                    logger.info("Request content-type is application/json but JSON body is empty or malformed.")
            except (ValueError, TypeError) as param_ex:
                logger.warning(f"Invalid parameter type for 'resolution_minutes' in request JSON: {param_ex}", exc_info=True)
                return f"Invalid parameter type for 'resolution_minutes'. Must be an integer. Error: {param_ex}", 400
            except Exception as json_ex:
                 logger.warning(f"Error processing request JSON for 'resolution_minutes': {json_ex}", exc_info=True)
                 return "Error processing request JSON for 'resolution_minutes'.", 400
        else:
            logger.info("No JSON body found or content type not application/json. Using default resolution for historical data.")


        successful_tasks, failed_tasks = asyncio.run(
            _async_process_historical_data(config, gcs_client_instance, resolution_minutes=resolution_req)
        )

        # --- Construct Final Response ---
        # Using yesterday's date for consistent reporting as in the async function
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        date_str_api_param = yesterday.strftime('%Y-%m-%d')

        final_status_message = f"Processing summary for Site ID {config['AMBER_SITE_ID']}, date {date_str_api_param}:\n"
        if successful_tasks:
            final_status_message += "Successful operations:\n" + "\n".join(successful_tasks) + "\n"
        if failed_tasks:
            final_status_message += "Failed operations:\n" + "\n".join(failed_tasks) + "\n"

        if not successful_tasks and not failed_tasks:
            # This can happen if all tasks were skipped due to existing files and no new data processed.
            final_status_message += "No new data was processed (tasks may have been skipped if data already exists or no tasks defined)."
            logger.info(final_status_message)
            return final_status_message, 200
        elif failed_tasks: # If any task failed
            logger.error(f"Completed with errors: {final_status_message}")
            return final_status_message, 500 # Indicate an issue
        else: # All tasks succeeded or were skipped
            logger.info(f"All tasks completed successfully or were appropriately skipped: {final_status_message}")
            return final_status_message, 200

    except ValueError as ve: # Catch config loading errors specifically
        logger.critical(f"Configuration error: {ve}", exc_info=True)
        return f"Configuration error: {ve}", 500
    except Exception as e: # Catch-all for broader issues (e.g., asyncio.run issues, GCS client init)
        critical_error_message = f"A critical unexpected error occurred in the main HTTP handler: {e}"
        logger.critical(critical_error_message, exc_info=True) # Use critical for top-level crashes
        return f"Internal Server Error: {critical_error_message}", 500
