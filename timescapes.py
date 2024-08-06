import azure.functions as func
import os
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timezone
import json
import asyncio

class BlobCache:
    def __init__(self):
        self.cache = {}

    async def get_blob_data(self, blob_client):
        blob_name = blob_client.blob_name
        if blob_name not in self.cache:
            try:
                blob_data = blob_client.download_blob().readall().decode('utf-8')
                self.cache[blob_name] = json.loads(blob_data)
            except Exception as e:
                self.cache[blob_name] = {"error": str(e)}
        return self.cache[blob_name]

async def process_batch(container_client, blob_cache, batch):
    tasks = [blob_cache.get_blob_data(container_client.get_blob_client(blob_info.name)) for blob_info in batch]
    return await asyncio.gather(*tasks)

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Get the connection string and container name from environment variables
    connection_string = os.environ['AzureWebStorage']
    container_name = os.environ['ContainerName']

    try:
        # Get the expected authentication token from environment variables
        auth_token = os.environ.get('Timescapes_PROVIDER_AUTHENTICATION_SECRET')

        # Retrieve the provided authentication token from the request headers
        provided_token = req.headers.get('X-Auth-Token')

        # Check if the provided token matches the expected token for all request types
        if provided_token != auth_token:
            return func.HttpResponse("Unauthorized", status_code=401)

        # Create a BlobServiceClient using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(container_name)

        # Get the start_time, end_time, and device_uid from query parameters
        start_time = req.params.get('start_time')
        end_time = req.params.get('end_time')
        device_uid = req.params.get('device_uid')

        # Validate that start_time and end_time are present
        if not start_time or not end_time:
            return func.HttpResponse(
                json.dumps({"error": "Both start_time and end_time are required parameters"}),
                status_code=400,
                mimetype="application/json"
            )

        # Parse start_time and end_time into datetime objects with timezone info
        start_datetime = datetime.fromisoformat(start_time).replace(tzinfo=timezone.utc)
        end_datetime = datetime.fromisoformat(end_time).replace(tzinfo=timezone.utc)

        # Get a list of blobs in the container
        if not device_uid:
            # List all blobs in the container
            blob_list = container_client.list_blobs()
        
        else:
            # Only list blobs that start with the device ID being requested
            blob_list = container_client.list_blobs(name_starts_with=device_uid)

        # Filter blobs based on the specified time interval and device_uid
        filtered_blobs = [
            blob for blob in blob_list
            if start_datetime <= blob['last_modified'] <= end_datetime and (not device_uid or device_uid in blob['name'])
        ]


        # Create a BlobCache instance for caching
        blob_cache = BlobCache()

        # Process the filtered_blobs in batches
        batch_size = 100  # Adjust the batch size based on your requirements
        data = []

        for i in range(0, len(filtered_blobs), batch_size):
            batch = filtered_blobs[i:i + batch_size]

            # Use asyncio.gather to process the batch asynchronously
            batch_results = asyncio.run(process_batch(container_client, blob_cache, batch))
            data.extend(batch_results)

        # Return the processed data as JSON
        return func.HttpResponse(body=json.dumps(data), status_code=200, mimetype="application/json")

    except Exception as e:
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
