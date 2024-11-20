import os
import json
import argparse
import getpass
from azure.storage.blob import BlobServiceClient

# File to store the configuration
CONFIG_FILE = os.path.expanduser("~/.data_helper_config.json")

def load_config():
    """Load configuration from the config file."""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as file:
            return json.load(file)
    return {}

def save_config(config):
    """Save configuration to the config file."""
    with open(CONFIG_FILE, "w") as file:
        json.dump(config, file)

def get_connection_string():
    """Retrieve the connection string from the config file."""
    config = load_config()
    return config.get("connection_string")

def get_container_name():
    """Retrieve the container name from the config file."""
    config = load_config()
    return config.get("container_name")

def set_connection_string():
    """Interactively set and save the connection string and container name."""
    connection_string = getpass.getpass(prompt="Enter your Azure Storage connection string: ")
    container_name = input("Enter your Azure Blob container name: ")

    # Validate the connection string and container name
    try:
        blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        # Attempt to get container properties to validate access
        container_client.get_container_properties()
    except Exception as e:
        print(f"Error validating connection string and container name: {e}")
        return

    config = load_config()
    config["connection_string"] = connection_string
    config["container_name"] = container_name
    save_config(config)
    print("Connection string and container name set successfully.")

def list_folders_in_container():
    """List all folders in the Azure Blob Storage container."""
    connection_string = get_connection_string()
    container_name = get_container_name()
    if not connection_string or not container_name:
        print("Connection string or container name not set. Use 'data_helper connection-string' first.")
        return
    
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Use a set to store unique folder names
    folder_set = set()

    # Iterate over blobs and extract folder names
    blobs = container_client.list_blobs()
    for blob in blobs:
        blob_name = blob.name
        if "/" in blob_name:
            folder = blob_name.split("/")[0]  # Get the top-level folder
            folder_set.add(folder)

    if folder_set:
        print("Folders in container:")
        for folder in sorted(folder_set):
            print(folder)
    else:
        print("No folders found in the container.")


def upload_folder_to_blob(folder_path):
    """Upload all files in a folder to Azure Blob Storage recursively, preserving folder structure."""
    connection_string = get_connection_string()
    container_name = get_container_name()
    if not connection_string or not container_name:
        print("Connection string or container name not set. Use 'data_helper connection-string' first.")
        return

    # Normalize the folder path
    folder_path = os.path.normpath(folder_path)
    folder_name = os.path.basename(folder_path)  # Get the folder name to use as a prefix

    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    for root, _, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)  # Full path to the file
            # Include the folder_name as part of the blob path
            blob_name = os.path.join(folder_name, os.path.relpath(file_path, folder_path)).replace("\\", "/")
            blob_client = container_client.get_blob_client(blob_name)

            try:
                with open(file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                print(f"Uploaded {file_path} as {blob_name}")
            except Exception as e:
                print(f"Failed to upload {file_path}: {e}")



def list_blobs_in_container():
    """List all blobs in the Azure Blob Storage container."""
    connection_string = get_connection_string()
    container_name = get_container_name()
    if not connection_string or not container_name:
        print("Connection string or container name not set. Use 'data_helper connection-string' first.")
        return
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    blobs = container_client.list_blobs()
    print("Blobs in container:")
    for blob in blobs:
        print(blob.name)

def download_folder_from_blob(folder_name, destination="."):
    """Download a folder and its contents from Azure Blob Storage."""
    connection_string = get_connection_string()
    container_name = get_container_name()
    if not connection_string or not container_name:
        print("Connection string or container name not set. Use 'data_helper connection-string' first.")
        return
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    blobs = container_client.list_blobs(name_starts_with=folder_name)
    for blob in blobs:
        blob_client = container_client.get_blob_client(blob)
        file_path = os.path.join(destination, blob.name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file:
            file.write(blob_client.download_blob().readall())
        print(f"Downloaded {blob.name} to {file_path}")

def delete_folder_from_blob(folder_name):
    """Delete a folder and its contents from Azure Blob Storage."""
    connection_string = get_connection_string()
    container_name = get_container_name()
    if not connection_string or not container_name:
        print("Connection string or container name not set. Use 'data_helper connection-string' first.")
        return
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    blobs = container_client.list_blobs(name_starts_with=folder_name)
    for blob in blobs:
        blob_client = container_client.get_blob_client(blob)
        blob_client.delete_blob()
        print(f"Deleted {blob.name}")

def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Data Helper CLI")
    subparsers = parser.add_subparsers(dest="command")

    # Connection string command
    conn_parser = subparsers.add_parser("connection-string", help="Set up the connection string and container name interactively")

    # Blob commands
    blob_parser = subparsers.add_parser("blob")
    blob_subparsers = blob_parser.add_subparsers(dest="blob_command")

    upload_parser = blob_subparsers.add_parser("upload")
    upload_parser.add_argument("folder", help="Folder path to upload")

    list_parser = blob_subparsers.add_parser("list", help="List all blobs")

    listdir_parser = blob_subparsers.add_parser("listdir", help="List all top-level folders in the container")

    download_parser = blob_subparsers.add_parser("download")
    download_parser.add_argument("folder_name", help="Folder name to download")

    delete_parser = blob_subparsers.add_parser("delete")
    delete_parser.add_argument("folder_name", help="Folder name to delete")

    args = parser.parse_args()

    if args.command == "connection-string":
        set_connection_string()
    elif args.command == "blob":
        if args.blob_command == "upload":
            upload_folder_to_blob(args.folder)
        elif args.blob_command == "list":
            list_blobs_in_container()
        elif args.blob_command == "listdir":
            list_folders_in_container()
        elif args.blob_command == "download":
            download_folder_from_blob(args.folder_name)
        elif args.blob_command == "delete":
            delete_folder_from_blob(args.folder_name)
        else:
            parser.print_help()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
