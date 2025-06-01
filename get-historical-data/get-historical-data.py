# Copyright (c) 2025, Sam Currie
# All rights reserved.
# 
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import functions_framework
import requests
import json
import datetime
import os
import sys
import logging # Added for structured logging
import time
from google.cloud import storage

# --- Global Configuration & Clients ---
# These are loaded from environment variables.
# Default values are placeholders and will cause the function to error if not overridden.
AMBER_SITE_ID = os.environ.get('AMBER_SITE_ID', 'your-amber-site-id')
AMBER_API_KEY = os.environ.get('AMBER_API_KEY', 'your-amber-api-key')
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'your-gcs-bucket-name')

storage_client = storage.Client() # Initialize GCS client globally for reuse

# Retry Logic Constants (shared by both data fetching operations)
_MAX_RETRIES = 3
_INITIAL_RETRY_DELAY_SECONDS = 5
_RETRY_BACKOFF_FACTOR = 2
_SAFETY_BUFFER_SECONDS = 2
_DEFAULT_RETRY_SECONDS_FALLBACK = 60

# --- Logging Setup ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Avoid adding handlers multiple times in warm start scenarios for Cloud Functions
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout) # Log to stdout for Cloud Functions
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --- Helper Functions ---

def _fetch_data_from_amber_api(api_url, headers, data_description):
    """
    Fetches data from the Amber API with retry logic.
    Returns the JSON data list if successful, an empty list if API returns no data,
    or None if all retries fail or a critical error occurs.
    """
    amber_data = None
    for attempt in range(_MAX_RETRIES):
        logger.info(f"Calling Amber API for {data_description} (Attempt {attempt + 1}/{_MAX_RETRIES}): {api_url}")
        try:
            response = requests.get(api_url, headers=headers)

            if response.status_code == 429: # Rate limit
                if attempt < _MAX_RETRIES - 1:
                    actual_wait_time = 0 # Will be set by specific logic below
                    rate_limit_reset_header = response.headers.get('RateLimit-Reset')
                    
                    if rate_limit_reset_header:
                        try:
                            actual_wait_time = int(rate_limit_reset_header) + _SAFETY_BUFFER_SECONDS
                            logger.warning(
                                f"Rate limit (429) for {data_description}. "
                                f"RateLimit-Reset: {rate_limit_reset_header}. Retrying in {actual_wait_time}s."
                            )
                        except ValueError:
                            logger.error(
                                f"Rate limit (429) for {data_description}. "
                                f"Invalid RateLimit-Reset ('{rate_limit_reset_header}'). Falling back to exponential backoff."
                            )
                            # Fall through to calculate exponential backoff
                            actual_wait_time = (_INITIAL_RETRY_DELAY_SECONDS * (_RETRY_BACKOFF_FACTOR ** attempt)) + _SAFETY_BUFFER_SECONDS
                            logger.warning(f"Exponential backoff: retrying in {actual_wait_time}s for {data_description}.")
                    else: # No RateLimit-Reset header
                        actual_wait_time = (_INITIAL_RETRY_DELAY_SECONDS * (_RETRY_BACKOFF_FACTOR ** attempt)) + _SAFETY_BUFFER_SECONDS
                        logger.warning(
                            f"Rate limit (429) for {data_description}. No RateLimit-Reset header. "
                            f"Using exponential backoff. Retrying in {actual_wait_time}s."
                        )
                    
                    time.sleep(actual_wait_time)
                    continue # Go to the next attempt
                else:
                    logger.error(f"Rate limit (429) for {data_description} on final attempt.")
                    return None # Indicate fetch failure

            response.raise_for_status()  # Raises HTTPError for other 4xx/5xx responses
            
            # Check for empty response before attempting JSON decode
            if not response.content:
                logger.info(f"Successfully fetched 0 records (empty response body) for {data_description} from Amber API.")
                return [] # Successful fetch, but no data

            amber_data = response.json()
            logger.info(f"Successfully fetched {len(amber_data)} records for {data_description} from Amber API.")
            return amber_data  # Return data list (could be empty if API returns [])

        except requests.exceptions.RequestException as e: # Includes HTTPError
            logger.error(f"API call for {data_description} (attempt {attempt + 1}) failed: {e}", exc_info=True)
            if attempt < _MAX_RETRIES - 1:
                wait_time = (_INITIAL_RETRY_DELAY_SECONDS * (_RETRY_BACKOFF_FACTOR ** attempt)) + _SAFETY_BUFFER_SECONDS
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"All API retries failed for {data_description}.")
                return None # Indicate fetch failure after all retries
        except json.JSONDecodeError as e:
            response_text_preview = response.text[:500] if response and response.text else "N/A"
            logger.error(f"JSON decoding failed for {data_description} after successful request: {e}. Response text preview: {response_text_preview}...", exc_info=True)
            # Not typically retried, as it indicates a malformed response from the server.
            return None # Indicate fetch failure (specifically decode error)

    return None # Should be unreachable if loop completes, but as a fallback


def _save_data_to_gcs(data, site_id, bucket_name, gcs_client_instance, filename_prefix, file_date_str, data_description):
    """
    Converts data list to JSONL and uploads it to GCS.
    Returns the GCS path if successful.
    Raises an exception if the upload fails or data is not a list.
    """
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
        blob.upload_from_string(jsonl_data, content_type='application/jsonl')
        logger.info(f"{data_description} file '{gcs_object_name}' uploaded successfully to GCS.")
        return f"gs://{bucket_name}/{gcs_object_name}"
    except Exception as e:
        logger.error(f"Failed to prepare or save {data_description} to GCS: {e}", exc_info=True)
        raise # Re-raise to be caught by the main handler for the task


@functions_framework.http
def process_historical_data(request):
    """
    HTTP Cloud Function that calls the Amber Electric API for historical
    price actuals and usage data for the previous day, converts responses
    to JSONL, and saves them to GCS.
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

    successful_tasks_details = []
    failed_tasks_details = []

    try:
        # Date for the API call (yesterday)
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        date_str_api_param = yesterday.strftime('%Y-%m-%d') # For API query parameter
        file_date_str_filename = yesterday.strftime('%Y%m%d')   # For filename component

        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {AMBER_API_KEY}'
        }

        # Define the data fetching tasks
        data_tasks_to_process = [
            {
                "type_name": "actual prices", # User-friendly name for logs/messages
                "endpoint_path": "prices",    # Path component for the API URL
                "filename_prefix": "amber_prices_actual" # Prefix for the GCS filename
            },
            {
                "type_name": "usage data",
                "endpoint_path": "usage",
                "filename_prefix": "amber_usage_data"
            }
        ]

        for task_config in data_tasks_to_process:
            task_description = f"{task_config['type_name']} for Site ID {AMBER_SITE_ID} on {date_str_api_param}"
            logger.info(f"--- Starting task: Processing {task_description} ---")
            
            api_url = f'https://api.amber.com.au/v1/sites/{AMBER_SITE_ID}/{task_config["endpoint_path"]}?startDate={date_str_api_param}&endDate={date_str_api_param}&resolution=30'
            
            amber_data_list = None # Initialize before try block
            try:
                amber_data_list = _fetch_data_from_amber_api(api_url, headers, task_description)

                if amber_data_list is not None: # API fetch attempt concluded (successfully or with empty data)
                    if not amber_data_list: # Successfully fetched, but API returned an empty list
                         message = f"Successfully fetched 0 {task_config['type_name']} records for {AMBER_SITE_ID} on {date_str_api_param} (no data to save)."
                         logger.info(message)
                         successful_tasks_details.append(message)
                    else: # Successfully fetched data
                        gcs_path = _save_data_to_gcs(
                            data=amber_data_list,
                            site_id=AMBER_SITE_ID,
                            bucket_name=GCS_BUCKET_NAME,
                            gcs_client_instance=storage_client, # Use the global client
                            filename_prefix=task_config["filename_prefix"],
                            file_date_str=file_date_str_filename,
                            data_description=task_description # Pass full description for context in saver
                        )
                        message = f"Successfully processed {len(amber_data_list)} {task_config['type_name']} records. Saved to: {gcs_path}"
                        logger.info(message)
                        successful_tasks_details.append(message)
                else: # _fetch_data_from_amber_api returned None, indicating fetch failure after retries
                    # Detailed errors already logged by _fetch_data_from_amber_api
                    failure_message = f"Failed to retrieve {task_description} from Amber API after multiple retries."
                    logger.error(failure_message)
                    failed_tasks_details.append(failure_message)
            
            except Exception as e: # Catch errors from _save_data_to_gcs or other unexpected issues for this task
                error_log_message = f"An unexpected error occurred while processing {task_description}: {e}"
                logger.error(error_log_message, exc_info=True)
                failed_tasks_details.append(f"Failed to process {task_description} due to: {e}")
            logger.info(f"--- Finished task: Processing {task_description} ---")

        # --- Construct Final Response ---
        final_status_message = f"Processing summary for Site ID {AMBER_SITE_ID}, date {date_str_api_param}:\n"
        if successful_tasks_details:
            final_status_message += "Successful operations:\n" + "\n".join(successful_tasks_details) + "\n"
        if failed_tasks_details:
            final_status_message += "Failed operations:\n" + "\n".join(failed_tasks_details) + "\n"

        if not successful_tasks_details and not failed_tasks_details:
             # Should not happen if tasks are defined, but as a fallback
            return "No tasks were processed or defined.", 200
        elif failed_tasks_details: # If any task failed
            logger.error(f"Completed with errors: {final_status_message}")
            return final_status_message, 500 # Indicate an issue
        else: # All tasks succeeded (even if some had no data to save)
            logger.info(f"All tasks completed successfully: {final_status_message}")
            return final_status_message, 200

    except Exception as e: # Catch-all for broader issues (e.g., config validation, date issues before loop)
        critical_error_message = f"A critical unexpected error occurred early in execution: {e}"
        logger.error(critical_error_message, exc_info=True)
        return critical_error_message, 500
