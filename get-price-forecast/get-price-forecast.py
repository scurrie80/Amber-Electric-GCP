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
AMBER_SITE_ID = os.environ.get('AMBER_SITE_ID', 'your-amber-site-id') # Replace with your site ID
AMBER_API_KEY = os.environ.get('AMBER_API_KEY', 'your-amber-api-key') # Replace with your API key or set as env var
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME', 'your-gcs-bucket-name') # Replace with your bucket name or set as env var

storage_client = storage.Client() # Initialize GCS client globally for reuse

# Retry Logic Constants
_MAX_RETRIES = 3
_INITIAL_RETRY_DELAY_SECONDS = 5
_RETRY_BACKOFF_FACTOR = 2
_SAFETY_BUFFER_SECONDS = 2
# _DEFAULT_RETRY_SECONDS_FALLBACK = 60 # This can be removed if exponential backoff is the standard fallback

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

def _fetch_forecast_from_amber_api(api_url, headers, data_description):
    """
    Fetches forecast data from the Amber API with retry logic.
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
                    actual_wait_time = 0
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
            
            if not response.content: # Should not happen for forecast, but good practice
                logger.info(f"Successfully fetched 0 records (empty response body) for {data_description} from Amber API.")
                return [] 

            amber_data = response.json()
            logger.info(f"Successfully fetched {len(amber_data)} records for {data_description} from Amber API.")
            return amber_data

        except requests.exceptions.RequestException as e:
            logger.error(f"API call for {data_description} (attempt {attempt + 1}) failed: {e}", exc_info=True)
            if attempt < _MAX_RETRIES - 1:
                wait_time = (_INITIAL_RETRY_DELAY_SECONDS * (_RETRY_BACKOFF_FACTOR ** attempt)) + _SAFETY_BUFFER_SECONDS
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"All API retries failed for {data_description}.")
                return None
        except json.JSONDecodeError as e:
            response_text_preview = response.text[:500] if response and response.text else "N/A"
            logger.error(f"JSON decoding failed for {data_description} after successful request: {e}. Response text preview: {response_text_preview}...", exc_info=True)
            return None

    return None # Fallback, should be unreachable if loop logic is correct

def _save_forecast_to_gcs(data, site_id, bucket_name, gcs_client_instance, timestamp_str, data_description):
    """
    Converts forecast data list to JSONL and uploads it to GCS.
    Returns the GCS path if successful.
    Raises an exception if the upload fails or data is not a list.
    Returns None if no data is provided (empty list).
    """
    if not isinstance(data, list):
        err_msg = f"Invalid data type for GCS upload for {data_description}. Expected list, got {type(data)}."
        logger.error(err_msg)
        raise ValueError(err_msg)

    if not data:
        logger.info(f"No data records provided for {data_description}, so no file will be saved to GCS.")
        return None 

    try:
        jsonl_lines = [json.dumps(record) for record in data]
        jsonl_data = "\n".join(jsonl_lines)
        if jsonl_data:
            jsonl_data += "\n"

        base_file_name = f"amber_prices_forecast_{timestamp_str}.jsonl"
        gcs_object_name = f"{site_id}/{base_file_name}"

        bucket = gcs_client_instance.bucket(bucket_name)
        blob = bucket.blob(gcs_object_name)

        logger.info(f"Uploading {data_description} file '{gcs_object_name}' to bucket '{bucket_name}'...")
        blob.upload_from_string(jsonl_data, content_type='application/jsonl')
        logger.info(f"{data_description} file '{gcs_object_name}' uploaded successfully to GCS.")
        return f"gs://{bucket_name}/{gcs_object_name}"
    except Exception as e:
        logger.error(f"Failed to prepare or save {data_description} to GCS: {e}", exc_info=True)
        raise

@functions_framework.http
def process_amber_data(request):
    """
    HTTP Cloud Function that calls the Amber Electric API for
    the latest price forecast data, converts the JSON response to JSONL,
    and saves it to GCS.
    """
    try:
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

        # --- Prepare API Call ---
        data_description = f"price forecast for Site ID {AMBER_SITE_ID}"
        amber_api_url = f'https://api.amber.com.au/v1/sites/{AMBER_SITE_ID}/prices/current?next=96&previous=0&resolution=30'
        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {AMBER_API_KEY}'
        }

        # --- Fetch Data ---
        logger.info(f"--- Starting task: Processing {data_description} ---")
        forecast_data_list = _fetch_forecast_from_amber_api(amber_api_url, headers, data_description)

        if forecast_data_list is None: # API fetch failed after retries
            # Error already logged by _fetch_forecast_from_amber_api
            failure_message = f"Failed to retrieve {data_description} from Amber API after multiple retries."
            logger.error(failure_message) # Log summary failure here as well
            return failure_message, 500
        
        if not forecast_data_list: # API returned empty list (successful fetch, no data)
            message = f"Successfully fetched 0 records for {data_description} (no data to save)."
            logger.info(message)
            logger.info(f"--- Finished task: Processing {data_description} ---")
            return message, 200

        # --- Save Data to GCS ---
        try:
            timestamp_for_filename = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
            gcs_path = _save_forecast_to_gcs(
                data=forecast_data_list,
                site_id=AMBER_SITE_ID,
                bucket_name=GCS_BUCKET_NAME,
                gcs_client_instance=storage_client,
                timestamp_str=timestamp_for_filename,
                data_description=data_description
            )
            
            num_records = len(forecast_data_list)
            success_message = f"Successfully processed {num_records} records for {data_description}. Saved to: {gcs_path}"
            logger.info(success_message)
            logger.info(f"--- Finished task: Processing {data_description} ---")
            return success_message, 200

        except Exception as e: # Catch errors from _save_forecast_to_gcs
            # Error already logged by _save_forecast_to_gcs
            error_log_message = f"Failed to save {data_description} to GCS: {e}"
            logger.error(error_log_message) # Log summary failure here
            logger.info(f"--- Finished task with error: Processing {data_description} ---")
            return error_log_message, 500

    except Exception as e: # Catch-all for broader issues (e.g., config, unexpected errors)
        critical_error_message = f"A critical unexpected error occurred: {e}"
        logger.error(critical_error_message, exc_info=True)
        return critical_error_message, 500

