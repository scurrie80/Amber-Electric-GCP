# Copyright (c) 2025, Sam Currie
# All rights reserved.
# 
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import functions_framework
import datetime # For UTC timestamp
import os
import sys
# import json # Only if explicitly needed for other JSON ops; shared_utils handles API JSON
import asyncio

from google.cloud import storage

# Adjust path to import from parent directory
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from shared_utils import (
    load_config,
    setup_logger,
    AsyncAmberClient,
    save_to_gcs_async
)

# --- Global Logger ---
logger = setup_logger() # Use shared logger setup
# Import json for request parsing
import json

# --- Async Core Logic ---
async def _async_process_forecast_data(config, storage_client_instance,
                                       next_intervals: int = 96,
                                       previous_intervals: int = 0,
                                       resolution_minutes: int = 30):
    """
    Core asynchronous logic to fetch price forecast data from Amber API and save to GCS.
    Parameters for next_intervals, previous_intervals, and resolution_minutes can be passed from the HTTP request.
    """
    amber_client = AsyncAmberClient(api_key=config["AMBER_API_KEY"], site_id=config["AMBER_SITE_ID"])
    data_description = (f"price forecast for Site ID {config['AMBER_SITE_ID']} with "
                        f"next={next_intervals}, prev={previous_intervals}, res={resolution_minutes}min")

    api_endpoint = "prices/current"
    # Parameters for the API call are now dynamic
    api_params = {
        "next": next_intervals,
        "previous": previous_intervals
        # resolution is handled by the client's get method's `resolution` parameter
    }

    logger.info(f"--- Starting task: Processing {data_description} ---")
    logger.info(f"Fetching {data_description} from Amber API endpoint {api_endpoint} with params {api_params} and resolution {resolution_minutes} min")

    try:
        forecast_data_list = await amber_client.get(
            api_endpoint,
            params=api_params,
            resolution=resolution_minutes # Pass resolution to client
        )

        if forecast_data_list is None: # Should be raised as an exception by AsyncAmberClient
            # This case might indicate an issue with AsyncAmberClient not raising an exception as expected
            logger.error(f"API client returned None for {data_description} without raising an exception.")
            return f"Failed to retrieve {data_description}: API client returned None.", 500

        if not forecast_data_list: # Successfully fetched, but API returned an empty list
            message = f"Successfully fetched 0 records for {data_description} (no data to save)."
            logger.info(message)
            logger.info(f"--- Finished task: Processing {data_description} ---")
            return message, 200

        # Generate UTC timestamp for filename
        timestamp_for_filename = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
        base_file_name = f"amber_prices_forecast_{timestamp_for_filename}.jsonl"
        gcs_object_name = f"{config['AMBER_SITE_ID']}/{base_file_name}"

        logger.info(f"Attempting to save {len(forecast_data_list)} records for {data_description} to GCS: gs://{config['GCS_BUCKET_NAME']}/{gcs_object_name}")

        gcs_path = await save_to_gcs_async(
            data=forecast_data_list,
            bucket_name=config["GCS_BUCKET_NAME"],
            object_name=gcs_object_name,
            gcs_client=storage_client_instance
        )

        num_records = len(forecast_data_list)
        success_message = f"Successfully processed {num_records} records for {data_description}. Saved to: {gcs_path}"
        logger.info(success_message)
        logger.info(f"--- Finished task: Processing {data_description} ---")
        return success_message, 200

    except Exception as e: # Catch errors from API client, GCS utils, or other issues
        error_message = f"Failed to process {data_description}: {e}"
        logger.error(error_message, exc_info=True)
        logger.info(f"--- Finished task with error: Processing {data_description} ---")
        return error_message, 500


@functions_framework.http
def process_amber_data(request):
    """
    HTTP Cloud Function that calls the Amber Electric API for
    the latest price forecast data, converts the JSON response to JSONL,
    and saves it to GCS. Uses shared utilities.
    """
    try:
        config = load_config() # Loads and validates essential configs
        gcs_client_instance = storage.Client()

        # Defaults for forecast parameters
        next_intervals_req = 96
        previous_intervals_req = 0
        resolution_minutes_req = 30

        if request and request.content_type == 'application/json':
            try:
                req_json = request.get_json(silent=True)
                if req_json:
                    next_intervals_req = int(req_json.get('next_intervals', next_intervals_req))
                    previous_intervals_req = int(req_json.get('previous_intervals', previous_intervals_req))
                    resolution_minutes_req = int(req_json.get('resolution_minutes', resolution_minutes_req))
                else:
                    logger.info("Request content-type is application/json but JSON body is empty or malformed.")
            except (ValueError, TypeError) as param_ex: # Catches errors from int() conversion or if get returns non-int type
                logger.warning(f"Invalid parameter type in request JSON: {param_ex}", exc_info=True)
                return f"Invalid parameter type in request JSON. Ensure 'next_intervals', 'previous_intervals', and 'resolution_minutes' are integers. Error: {param_ex}", 400
            except Exception as json_ex: # Catch any other JSON processing errors
                 logger.warning(f"Error processing request JSON: {json_ex}", exc_info=True)
                 return "Error processing request JSON.", 400
        else:
            logger.info("No JSON body found or content type not application/json. Using default forecast parameters.")

        message, status_code = asyncio.run(
            _async_process_forecast_data(
                config,
                gcs_client_instance,
                next_intervals=next_intervals_req,
                previous_intervals=previous_intervals_req,
                resolution_minutes=resolution_minutes_req
            )
        )
        return message, status_code

    except ValueError as ve: # Catches errors from load_config
        logger.critical(f"Configuration error: {ve}", exc_info=True)
        return f"Configuration error: {ve}", 500 # Config errors are server-side
    except Exception as e: # Catch-all for other unexpected errors
        critical_error_message = f"A critical unexpected error occurred in the main HTTP handler: {e}"
        logger.critical(critical_error_message, exc_info=True)
        return f"Internal Server Error: {critical_error_message}", 500

