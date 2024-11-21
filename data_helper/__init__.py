import os
import json
import argparse
import getpass
from datetime import datetime
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


def recreate_dataset(snapshot_name, destination):
    """Recreate a dataset locally from snapshot metadata."""
    connection_string = get_connection_string()
    container_name = get_container_name()
    if not connection_string or not container_name:
        print("Connection string or container name not set. Use 'data_helper connection-string' first.")
        return

    # Download metadata from Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    metadata_blob_name = f"snapshots/{snapshot_name}/metadata.json"
    blob_client = container_client.get_blob_client(metadata_blob_name)

    try:
        metadata_content = blob_client.download_blob().readall()
        snapshot_metadata = json.loads(metadata_content)
    except Exception as e:
        print(f"Failed to download metadata: {e}")
        return

    # Recreate dataset directory structure
    for split, files in snapshot_metadata["data_splits"].items():
        split_dir = os.path.join(destination, split)
        os.makedirs(split_dir, exist_ok=True)
        for file in files:
            file_path = os.path.join(split_dir, os.path.basename(file))
            open(file_path, "a").close()  # Create an empty file to mimic the structure

    for split, annotations in snapshot_metadata["annotations"].items():
        split_dir = os.path.join(destination, split)
        for annotation in annotations:
            annotation_path = os.path.join(split_dir, os.path.basename(annotation))
            open(annotation_path, "a").close()  # Create an empty annotation file

    print(f"Dataset '{snapshot_name}' recreated at {destination}.")


def create_snapshot(dataset_path, snapshot_name):
    """Create a snapshot containing only metadata about the dataset."""
    connection_string = get_connection_string()
    container_name = get_container_name()
    if not connection_string or not container_name:
        print("Connection string or container name not set. Use 'data_helper connection-string' first.")
        return

    # Prepare metadata
    snapshot_metadata = {
        "snapshot_name": snapshot_name,
        "created_at": datetime.now().isoformat(),
        "description": f"Snapshot of dataset {os.path.basename(dataset_path)}",
        "data_splits": {
            "train": [],
            "val": [],
            "test": []
        },
        "annotations": {
            "train": [],
            "val": [],
            "test": []
        }
    }

    # Scan dataset for splits and annotations
    for split in ["train", "val", "test"]:
        split_path = os.path.join(dataset_path, split)
        if os.path.exists(split_path) and os.path.isdir(split_path):
            for file in os.listdir(split_path):
                file_path = os.path.join(split_path, file)
                if file.endswith(".png") and os.path.isfile(file_path):
                    # Add the image to data_splits
                    snapshot_metadata["data_splits"][split].append(os.path.join(split, file))

                    # Check for a corresponding annotation file
                    annotation_file = file.replace(".png", ".txt")
                    annotation_path = os.path.join(split_path, annotation_file)
                    if os.path.exists(annotation_path):
                        snapshot_metadata["annotations"][split].append(os.path.join(split, annotation_file))

    # Save metadata locally
    local_metadata_file = os.path.join(dataset_path, f"{snapshot_name}_metadata.json")
    with open(local_metadata_file, "w") as metadata_file:
        json.dump(snapshot_metadata, metadata_file, indent=4)

    print(f"Metadata saved locally at {local_metadata_file}")

    # Upload metadata to Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    metadata_blob_name = f"snapshots/{snapshot_name}/metadata.json"
    blob_client = container_client.get_blob_client(metadata_blob_name)

    try:
        with open(local_metadata_file, "rb") as metadata_file:
            blob_client.upload_blob(metadata_file, overwrite=True)
        print(f"Snapshot '{snapshot_name}' created and metadata uploaded to Azure Blob Storage.")
    except Exception as e:
        print(f"Failed to upload metadata to Azure Blob Storage: {e}")


def list_snapshots():
    """List all dataset snapshots."""
    connection_string = get_connection_string()
    container_name = get_container_name()
    if not connection_string or not container_name:
        print("Connection string or container name not set. Use 'data_helper connection-string' first.")
        return

    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # List all blobs under the "snapshots/" prefix
    snapshots = set()
    blobs = container_client.list_blobs(name_starts_with="snapshots/")
    for blob in blobs:
        parts = blob.name.split("/")
        if len(parts) > 1:
            snapshots.add(parts[1])

    if snapshots:
        print("Snapshots:")
        for snapshot in sorted(snapshots):
            print(snapshot)
    else:
        print("No snapshots found.")


def delete_snapshot(snapshot_name):
    """Delete a dataset snapshot."""
    connection_string = get_connection_string()
    container_name = get_container_name()
    if not connection_string or not container_name:
        print("Connection string or container name not set. Use 'data_helper connection-string' first.")
        return

    blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    snapshot_prefix = f"snapshots/{snapshot_name}/"
    blobs = container_client.list_blobs(name_starts_with=snapshot_prefix)
    for blob in blobs:
        blob_client = container_client.get_blob_client(blob)
        blob_client.delete_blob()
        print(f"Deleted {blob.name}")

    print(f"Snapshot '{snapshot_name}' deleted successfully.")

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

    # Snapshot commands
    snapshot_parser = subparsers.add_parser("dataset")
    snapshot_subparsers = snapshot_parser.add_subparsers(dest="dataset_command")

    create_snapshot_parser = snapshot_subparsers.add_parser("snapshot-create")
    create_snapshot_parser.add_argument("dataset_path", help="Path to the dataset to snapshot")
    create_snapshot_parser.add_argument("snapshot_name", help="Name of the snapshot")
   
    recreate_snapshot_parser = snapshot_subparsers.add_parser("snapshot-recreate")
    recreate_snapshot_parser.add_argument("snapshot_name", help="Name of the snapshot to recreate")
    recreate_snapshot_parser.add_argument("destination", help="Destination path for the recreated dataset")

    list_snapshot_parser = snapshot_subparsers.add_parser("snapshot-list", help="List all dataset snapshots")

    delete_snapshot_parser = snapshot_subparsers.add_parser("snapshot-delete")
    delete_snapshot_parser.add_argument("snapshot_name", help="Name of the snapshot to delete")

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
    elif args.command == "dataset":
        if args.dataset_command == "snapshot-create":
            create_snapshot(args.dataset_path, args.snapshot_name)
        elif args.dataset_command == "snapshot-list":
            list_snapshots()
        elif args.dataset_command == "snapshot-delete":
            delete_snapshot(args.snapshot_name)
        else:
            parser.print_help()
    elif args.dataset_command == "snapshot-recreate":
        recreate_dataset(args.snapshot_name, args.destination)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
