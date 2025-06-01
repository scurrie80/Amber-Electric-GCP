# Amber Electric Data Cloud Run Functions

This directory contains Google Cloud Run Functions designed to interact with the Amber Electric API, fetch various types of electricity data (historical prices, usage, forecasts), and store this data in Google Cloud Storage (GCS) in JSONL format.

## Features

* **Modular**
    The functions are independant so you can deploy one or both.
* **Completenss**
    I'm often frustrated by by finding fragments of working code, so I wrote these functions to be as complete and as robust as I can. I've also include shell scripts to deploy the functions to Google Cloud Run.
* **Backfill Script**
    The Cloud Functions scripts are focused on collecting new data, so to get historical data there is a backfill (get-backfill.py) script.
  

## Directory Structure

```text
/
├── artifacts-cleanup-policy.json
├── env.yaml
├── get-backfill/
│   └── get-backfill.py
│   └── (requirements.txt - typically here)
│   └── (deploy-get-backfill.sh - typically here)
├── get-historical-data/
│   ├── deploy-get-historical-data.sh
│   ├── get-historical-data.py
│   └── requirements.txt
├── get-price-forecast/
│   ├── deploy-get-price-forecast.sh
│   ├── get-price-forecast.py
│   └── requirements.txt
├── README.md
└── requirements.txt
```

## Common Files

* **`env.yaml`**:
    A template or actual configuration file for environment variables required by the Cloud Functions. This typically includes:
  * `GCS_BUCKET_NAME`: The name of the Google Cloud Storage bucket where data will be stored.
  * `AMBER_API_KEY`: Your personal Amber Electric API key.
  * `AMBER_SITE_ID`: The specific site ID for which to fetch data.
  * **Note**: Ensure this file (or the actual environment variables in your Cloud Function deployment) is correctly populated before deploying.

* **`artifacts-cleanup-policy.json`**:
    Defines a cleanup policy for Google Artifact Registry, specifically for deleting old, untagged container images to manage storage costs and keep the registry tidy. This policy is applied by the deployment scripts.

* **`requirements.txt`** (at the root of `Cloud Run Function/`):
    This file lists common Python dependencies that might be shared across functions or used for local development/testing. Each function subdirectory also has its own `requirements.txt` for specific dependencies.

## Subdirectories (Cloud Functions)

Each subdirectory under `Cloud Run Function/` typically represents a distinct Google Cloud Function.

### 1. `get-backfill/`

* **Purpose**: This function is designed to perform a large-scale backfill of historical electricity data (actual prices and usage) from the Amber Electric API for a specified date range.
* **Key File**:
  * `get-backfill.py`: The Python script containing the asynchronous logic to fetch data for each day in the range, handle API rate limits, and save data to GCS. It processes data concurrently to improve efficiency.
* **Deployment**: While not explicitly listed, a deployment script (e.g., `deploy-get-backfill.sh`) would typically be used to deploy this function. This function is likely manually triggered or run on an ad-hoc basis due to its potentially long execution time.
* **Features**:
  * Asynchronous API calls using `aiohttp`.
  * Concurrency control using `asyncio.Semaphore`.
  * Checks for existing files in GCS to avoid re-processing.
  * Robust retry logic for API calls.
* **Limitations**:
  * The Amber API only allows 90 days of usage history. Though it can be inconsistent allow more. 

### 2. `get-historical-data/`

* **Purpose**: This function fetches historical electricity data (actual prices and usage) from the Amber Electric API for the *previous day*. It is designed to be run regularly (e.g., daily) via Cloud Scheduler.
* **Key Files**:
  * `get-historical-data.py`: The Python script for the Cloud Function. It fetches data for yesterday, formats it as JSONL, and uploads it to GCS.
  * `deploy-get-historical-data.sh`: A shell script to deploy the Cloud Function to Google Cloud, set up necessary permissions, and create/update a Cloud Scheduler job to trigger the function automatically.
  * `requirements.txt`: Python dependencies for this specific function (e.g., `requests`, `google-cloud-storage`).
* **Features**:
  * Scheduled execution via Cloud Scheduler.
  * Retry logic for API calls.
  * Configuration via environment variables.

### 3. `get-price-forecast/`

* **Purpose**: This function fetches the latest electricity price forecast data (e.g., next 48 hours, equivalent to 96 intervals of 30 mins) from the Amber Electric API. It is designed to be run frequently (e.g., every 30 minutes) via Cloud Scheduler to keep forecast data up-to-date.
* **Key Files**:
  * `get-price-forecast.py`: The Python script for the Cloud Function. It fetches current price forecasts, formats them as JSONL, and uploads them to GCS.
  * `deploy-get-price-forecast.sh`: A shell script to deploy the Cloud Function, set up permissions, and create/update a Cloud Scheduler job.
  * `requirements.txt`: Python dependencies for this function.
* **Features**:
  * Scheduled execution via Cloud Scheduler.
  * Retry logic for API calls.
  * Timestamped filenames for forecast data.

## General Notes

* **Authentication**: Functions that are triggered via HTTP by Cloud Scheduler are configured to require authentication, using OIDC tokens from a dedicated service account.
* **Error Handling**: The Python scripts include error handling, logging, and retry mechanisms to deal with API rate limits and transient network issues.
* **Cost Management**: Deployment scripts include steps to clean up old container image revisions to manage costs.
* **Python Runtime**: The functions are developed for Python 3.12 or 3.13 (as seen in deployment scripts).

Make sure to review and update the `env.yaml` file with your specific Amber Electric API key, Site ID, and GCS bucket name before deploying these functions.