# shared_utils.py
import os
import sys
import logging
import json
import asyncio
import aiohttp
import time # For sync version, if kept
from google.cloud import storage

# --- Retry Logic Constants ---
MAX_RETRIES = 5
INITIAL_RETRY_DELAY_SECONDS = 1
MAX_RETRY_DELAY_SECONDS = 60
RETRY_BACKOFF_FACTOR = 2

# --- Logger ---
def setup_logger():
    """Configures a logger that logs to sys.stdout."""
    logger = logging.getLogger(__name__)
    if not logger.handlers: # Avoid adding multiple handlers on warm starts
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

logger = setup_logger()

# --- Configuration ---
def load_config():
    """Loads configuration from environment variables."""
    config = {
        "AMBER_SITE_ID": os.environ.get("AMBER_SITE_ID"),
        "AMBER_API_KEY": os.environ.get("AMBER_API_KEY"),
        "GCS_BUCKET_NAME": os.environ.get("GCS_BUCKET_NAME"),
    }

    missing_configs = [key for key, value in config.items() if value is None]
    if missing_configs:
        raise ValueError(f"Missing essential configuration(s): {', '.join(missing_configs)}")

    return config

# --- Amber API Client (Async) ---
class AsyncAmberClient:
    def __init__(self, api_key, site_id):
        self.api_key = api_key
        self.site_id = site_id
        self.base_url = f"https://api.amberengine.com/v1/sites/{self.site_id}"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "accept": "application/json"
        }

    async def _request(self, method, endpoint, params=None, data=None, resolution: int = None):
        url = f"{self.base_url}/{endpoint}"

        # Use a copy of params if it exists, or initialize a new dict
        request_params = dict(params) if params is not None else {}

        if resolution is not None:
            request_params['resolution'] = resolution

        retry_count = 0
        retry_delay = INITIAL_RETRY_DELAY_SECONDS

        async with aiohttp.ClientSession() as session:
            while retry_count < MAX_RETRIES:
                try:
                    # Pass params to the request method
                    async with session.request(method, url, headers=self.headers, params=request_params, json=data) as response:
                        response.raise_for_status() # Raises HTTPError for 4xx/5xx responses
                        # Handle potentially empty responses that are valid JSON (e.g. `[]`)
                        # Checking content_type is important to avoid trying to .json() on non-json responses.
                        if response.content_type == 'application/json':
                            return await response.json()
                        else:
                            # For non-JSON responses, you might want to return text or handle differently
                            # For now, let's assume Amber API always returns JSON or an error that's raised by raise_for_status
                            logger.warning(f"Unexpected content type {response.content_type} for {url}")
                            return await response.text() # Or raise an error
                except aiohttp.ClientResponseError as e:
                    # Attempt to get more detailed error message from response if available
                    error_body = await response.text() if response else "No response body"
                    logger.error(f"ClientResponseError for {method} {url} with params {request_params}: {e.status} {e.message}, Headers: {e.headers}, Response Body: {error_body[:500]}") # Log first 500 chars of body

                    if e.status == 429: # Rate limit
                        reset_time_str = response.headers.get("RateLimit-Reset")
                        sleep_duration = retry_delay # Default if no header
                        if reset_time_str:
                            try:
                                # Assuming RateLimit-Reset is a UNIX timestamp for when to retry
                                reset_timestamp = int(reset_time_str)
                                current_timestamp = time.time()
                                # Ensure sleep_duration is not negative if reset_timestamp is in the past
                                sleep_duration = max(0, reset_timestamp - current_timestamp) + INITIAL_RETRY_DELAY_SECONDS # Add small buffer
                                logger.warning(f"Rate limited (429). RateLimit-Reset: {reset_time_str}. Calculated sleep: {sleep_duration}s. Retrying after {sleep_duration}s.")
                            except ValueError:
                                logger.warning(f"Rate limited (429). Invalid RateLimit-Reset header ('{reset_time_str}'). Using default retry delay {retry_delay}s.")
                                sleep_duration = retry_delay
                        else: # No RateLimit-Reset header
                            logger.warning(f"Rate limited (429). No RateLimit-Reset header. Using default retry delay {retry_delay}s.")
                            sleep_duration = retry_delay

                        await asyncio.sleep(sleep_duration)

                    elif e.status >= 500: # Server-side errors
                        logger.warning(f"Server error ({e.status}) for {method} {url}. Retrying after {retry_delay} seconds.")
                        await asyncio.sleep(retry_delay)
                    else: # Other client-side errors (4xx not 429) - usually not retried unless specific
                        # For example, 401/403 might mean auth issues that won't resolve with a retry
                        # 404 means endpoint not found.
                        # These should typically raise immediately.
                        raise # Re-raise the original error to be handled by the caller

                except aiohttp.ClientError as e: # Other client errors (e.g., connection issues, timeouts)
                    logger.error(f"AIOHTTP client error for {method} {url} with params {request_params}: {e}. Retrying after {retry_delay} seconds.")
                    await asyncio.sleep(retry_delay)

                retry_count += 1
                # Exponential backoff for retry_delay
                retry_delay = min(INITIAL_RETRY_DELAY_SECONDS * (RETRY_BACKOFF_FACTOR ** retry_count), MAX_RETRY_DELAY_SECONDS)

            # If loop finishes, all retries have been exhausted
            raise Exception(f"Max retries ({MAX_RETRIES}) reached for {method} request to {url} with params {request_params}")

    async def get(self, endpoint, params=None, resolution: int = None):
        """Makes a GET request to the Amber API, optionally including a resolution parameter."""
        return await self._request("GET", endpoint, params=params, resolution=resolution)

# --- GCS Utilities (Async for save, flexible for check) ---
async def save_to_gcs_async(data: list[dict], bucket_name: str, object_name: str, gcs_client: storage.Client):
    """Saves data as JSONL to GCS asynchronously."""
    try:
        jsonl_data = "\n".join([json.dumps(item) for item in data])

        loop = asyncio.get_event_loop()
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(object_name)

        await loop.run_in_executor(None, blob.upload_from_string, jsonl_data, "application/jsonl")

        gcs_path = f"gs://{bucket_name}/{object_name}"
        logger.info(f"Successfully saved data to {gcs_path}")
        return gcs_path
    except Exception as e:
        logger.error(f"Error saving data to GCS: {e}")
        raise

async def check_gcs_file_exists_async(bucket_name: str, object_name: str, gcs_client: storage.Client) -> bool:
    """Checks if a file exists in GCS asynchronously."""
    try:
        loop = asyncio.get_event_loop()
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(object_name)

        return await loop.run_in_executor(None, blob.exists)
    except Exception as e:
        logger.error(f"Error checking GCS file existence: {e}")
        raise

def check_gcs_file_exists_sync(bucket_name: str, object_name: str, gcs_client: storage.Client) -> bool:
    """Checks if a file exists in GCS synchronously."""
    try:
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        return blob.exists()
    except Exception as e:
        logger.error(f"Error checking GCS file existence: {e}")
        raise
