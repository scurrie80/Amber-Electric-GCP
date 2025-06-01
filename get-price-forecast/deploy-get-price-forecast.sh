#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error when substituting.
set -u
# Pipefail: the return value of a pipeline is the status of the last command to exit with a non-zero status,
# or zero if no command exited with a non-zero status.
set -o pipefail

# Declarations
readonly my_function_name='amber-grenville-get-price-forecast' # change to your function name
readonly my_region='us-central1' # change to your region
readonly my_project='prod-data-projects' # change to your project
readonly delete_old_revisions='true' # Set to false if you want to keep prior revisions of this service
readonly source_python_file='get-price-forecast.py'
readonly entry_point_name='process_amber_data'
readonly python_runtime='python313'
readonly function_memory='128Mi'
readonly env_vars_file='../env.yaml'

# Cloud Scheduler Configuration
readonly scheduler_job_name="${my_function_name}-trigger" # Name for the Cloud Scheduler job
readonly scheduler_description="Periodically triggers the ${my_function_name} Cloud Function to fetch Amber price forecasts."
readonly scheduler_schedule="5,35 * * * *" # Example: Run daily at 2 AM. Adjust as needed.
readonly scheduler_time_zone="Australia/Sydney" # Example: Set to your desired timezone.
readonly scheduler_sa_name="scheduler-invoker-sa" # Short name for the service account
readonly scheduler_oidc_service_account_email="${scheduler_sa_name}@${my_project}.iam.gserviceaccount.com"

# Function to clean up temporary files
cleanup() {
  echo -e "\nCleaning up temporary file main.py..."
  rm -f main.py
}

# Register the cleanup function to be called on EXIT, ERR, SIGINT, SIGTERM
trap cleanup EXIT ERR SIGINT SIGTERM

# gcloud command expects main.py for script
if [ ! -f "$source_python_file" ]; then
    echo -e "\nError: Source Python file '$source_python_file' not found."
    exit 1
fi
echo -e "\nCreating temporary main.py from $source_python_file..."
cp "$source_python_file" main.py

if [ ! -f "$env_vars_file" ]; then
    echo -e "\nError: Environment variables file '$env_vars_file' not found."
    exit 1
fi

# Deploy script for Cloud Run Function
echo -e "\nBeginning deployment of Cloud Function: $my_function_name in project $my_project region $my_region"
gcloud functions deploy "$my_function_name" \
    --gen2 \
    --runtime "$python_runtime" \
    --region "$my_region" \
    --source . \
    --entry-point "$entry_point_name" \
    --trigger-http \
    --ingress-settings all \
    --no-allow-unauthenticated \
    --env-vars-file "$env_vars_file" \
    --project "$my_project" \
    --memory "$function_memory"

# --- Service Account Setup for Scheduler ---
echo -e "\n--- Setting up Service Account: $scheduler_sa_name ---"
echo -e "\nChecking if service account '$scheduler_oidc_service_account_email' exists..."
if gcloud iam service-accounts describe "$scheduler_oidc_service_account_email" --project="$my_project" >/dev/null 2>&1; then
    echo -e "\nService account '$scheduler_oidc_service_account_email' already exists."
else
    echo -e "\nService account '$scheduler_oidc_service_account_email' not found. Creating..."
    gcloud iam service-accounts create "$scheduler_sa_name" \
        --description="Service account for Cloud Scheduler to invoke Cloud Functions" \
        --display-name="Scheduler Invoker SA" \
        --project="$my_project"
    echo -e "\nService account '$scheduler_oidc_service_account_email' created."
fi

echo -e "\nGranting invoker permission to '$scheduler_oidc_service_account_email' for function '$my_function_name'..."
gcloud functions add-invoker-policy-binding "$my_function_name" \
    --member="serviceAccount:$scheduler_oidc_service_account_email" \
    --region="$my_region" \
    --project="$my_project"
echo -e "\nIAM policy updated for function '$my_function_name'."

# --- Cloud Scheduler Setup ---
echo -e "\n--- Setting up Cloud Scheduler job: $scheduler_job_name ---"

echo -e "\nFetching URI for Cloud Function: $my_function_name"
function_uri=$(gcloud functions describe "$my_function_name" \
    --region "$my_region" \
    --project "$my_project" \
    --gen2 \
    --format="value(serviceConfig.uri)")

if [ -z "$function_uri" ]; then
    echo -e "\nError: Could not retrieve URI for function $my_function_name. Skipping scheduler setup."
    # If this is critical, you might want to exit 1 here
else
    echo -e "\nFunction URI: $function_uri"
    echo -e "\nChecking if Cloud Scheduler job '$scheduler_job_name' exists..."
    if gcloud scheduler jobs describe "$scheduler_job_name" --location="$my_region" --project="$my_project" >/dev/null 2>&1; then
        echo -e "\nJob '$scheduler_job_name' found. Updating existing Cloud Scheduler job..."
        gcloud scheduler jobs update http "$scheduler_job_name" \
            --schedule="$scheduler_schedule" \
            --uri="$function_uri" \
            --http-method=GET \
            --description="$scheduler_description" \
            --time-zone="$scheduler_time_zone" \
            --oidc-service-account-email="$scheduler_oidc_service_account_email" \
            --oidc-token-audience="$function_uri" \
            --attempt-deadline=320s \
            --location="$my_region" \
            --project="$my_project"
        echo -e "\nCloud Scheduler job '$scheduler_job_name' updated."
    else
        echo -e "\nJob '$scheduler_job_name' not found. Creating new Cloud Scheduler job..."
        gcloud scheduler jobs create http "$scheduler_job_name" \
            --schedule="$scheduler_schedule" \
            --uri="$function_uri" \
            --http-method=GET \
            --location="$my_region" \
            --description="$scheduler_description" \
            --time-zone="$scheduler_time_zone" \
            --oidc-service-account-email="$scheduler_oidc_service_account_email" \
            --oidc-token-audience="$function_uri" \
            --attempt-deadline=320s \
            --project="$my_project"
        echo -e "\nCloud Scheduler job '$scheduler_job_name' created."
    fi
fi

# Clean up old inactive revisions
if [ "$delete_old_revisions" = "true" ]; then
    echo -e "\nChecking for inactive Cloud Run revisions for service: $my_function_name"
    revisions_to_delete=$(gcloud run revisions list \
        --service="$my_function_name" \
        --region="$my_region" \
        --project="$my_project" \
        --filter="status.conditions.type:Active AND status.conditions.status:'False'" \
        --format="value(metadata.name)")

    if [ -n "$revisions_to_delete" ]; then
        echo -e "\nFound inactive revisions to delete:"
        echo -e "\n$revisions_to_delete"
        echo -e "\n$revisions_to_delete" | xargs -L1 gcloud run revisions delete --quiet --region="$my_region" --project="$my_project"
        echo -e "\nInactive revisions deleted."
    else
        echo -e "\nNo inactive revisions found to delete."
    fi

    # Setting Artifact Registry policy to removed untagged/unused images
    echo -e "\nSetting Artifact Registry policy to removed untagged/unused images"
    gcloud artifacts repositories set-cleanup-policies gcf-artifacts \
    --location=$my_region \
    --policy=../artifacts-cleanup-policy.json

else
    echo -e "\nSkipping deletion of old revisions as per configuration."
fi

echo -e "\nDone"
