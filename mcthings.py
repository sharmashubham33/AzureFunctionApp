import azure.functions as func
import os
import json  
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Get the connection strings and container names from environment variables
    #Standard Blob Storage
    connection_string1 = os.environ['AzureWebStorage']
    container_name1 = os.environ['ContainerName']
    
    #Testing Premium Blob Storage Connection
    connection_string2 = os.environ['AzureWebStorage2']
    container_name2 = os.environ['ContainerName2']

    # Get the expected authentication token from environment variables
    auth_token = os.environ.get('MICROSOFT_PROVIDER_AUTHENTICATION_SECRET')

    try:
        # Check if the request method is POST
        if req.method == 'POST':
            # Retrieve the provided authentication token from the request headers
            provided_token = req.headers.get('X-Auth-Token')

            # Check if the provided token matches the expected token
            if provided_token != auth_token:
                return func.HttpResponse("Unauthorized", status_code=401)

            # Receive the JSON payload from the request
            payload = req.get_json()

            # Check if the payload is not empty
            if payload:
                # Convert the payload to a JSON string with double quotes
                payload_json = json.dumps(payload)

                # Create BlobServiceClients using the connection strings
                blob_service_client1 = BlobServiceClient.from_connection_string(connection_string1)
                blob_service_client2 = BlobServiceClient.from_connection_string(connection_string2)

                # Get references to the containers
                container_client1 = blob_service_client1.get_container_client(container_name1)
                container_client2 = blob_service_client2.get_container_client(container_name2)

                # Generate unique names for the blobs
                blob_name = f"{payload['DeviceUID']}_{payload['DateTime']}.json"

                # Check if the blobs already exist
                blob_client1 = container_client1.get_blob_client(blob_name)
                blob_client2 = container_client2.get_blob_client(blob_name)
                if blob_client1.exists():
                    return func.HttpResponse(f"Blob {blob_name} already exists in ContainerName1", status_code=200)
                if blob_client2.exists():
                    return func.HttpResponse(f"Blob {blob_name} already exists in ContainerName2", status_code=200)

                # Upload the JSON payload as blobs to both containers
                blob_client1.upload_blob(data=payload_json)
                blob_client2.upload_blob(data=payload_json)

                return func.HttpResponse(f"Payload stored in containers {container_name1} and {container_name2} as {blob_name}", status_code=200)

            return func.HttpResponse("Empty payload", status_code=400)

        return func.HttpResponse("Unsupported HTTP method", status_code=400)

    except Exception as e:
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
