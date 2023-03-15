import firebase_admin
import typer
import os
import json
import boto3

from fnmatch import fnmatch
from rich import print
from rich.progress import track

from firebase_admin import credentials

root = "./"
pattern = "*.mat"
db_file_name = "db.json"
bucket_name = "craam-files-bucket"

s3 = boto3.client(
    "s3",
    aws_access_key_id="AKIAVE3JHNTULM7JK4F5",
    aws_secret_access_key="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",
)


narrowband_file_size = 26
broadband_file_size = 22

uploaded_files = []
narrowband_queue = []
broadband_queue = []


def saveJsonFile() -> None:
    with open(db_file_name, "w") as outfile:
        json.dump(
            {
                "uploaded_files": uploaded_files,
                "narrowband_queue": narrowband_queue,
                "broadband_queue": broadband_queue,
            },
            outfile,
        )


def getJsonData() -> dict:
    print("Loading storage data...")
    with open(db_file_name) as json_file:
        data = json.load(json_file)
        global uploaded_files
        global narrowband_queue
        global broadband_queue

        if data:
            uploaded_files = data["uploaded_files"]
            narrowband_queue = data["narrowband_queue"]
            broadband_queue = data["broadband_queue"]
            print("Storage data found!")

        else:
            print("No storage data found")


def shouldProcessFile(path: str) -> bool:
    return not (
        path in uploaded_files or path in narrowband_queue or path in broadband_queue
    )


def uploadToS3(path: str, type: str) -> None:
    file_name = os.path.basename(path)

    s3.upload_file(path, bucket_name, file_name)

    if type == "NARROWBAND":
        narrowband_queue.remove(path)
    elif type == "BROADBAND":
        broadband_queue.remove(path)

    uploaded_files.append(path)

    saveJsonFile()


def main() -> None:
    cred = credentials.Certificate("./credentials.json")
    firebase_admin.initialize_app(cred)

    getJsonData()

    for path, subdirs, files in track(
        os.walk(root), description="Finding .MAT files..."
    ):
        for name in files:
            if fnmatch(name, pattern):
                file_path = os.path.join(path, name)

                if len(name) == narrowband_file_size and shouldProcessFile(file_path):
                    narrowband_queue.append(file_path)
                    print(f"Found narrowband file {narrowband_queue[-1]}")

                elif len(name) == broadband_file_size and shouldProcessFile(file_path):
                    broadband_queue.append(file_path)
                    print(f"Found broadband file {broadband_queue[-1]}")

                elif (
                    len(name) == narrowband_file_size
                    or len(name) == broadband_file_size
                ) and not shouldProcessFile(file_path):
                    print(f"File {file_path} is already uploaded or in queue")

                else:
                    print(f"File {file_path} not supported, skipping...")

    print(
        f"Found {len(narrowband_queue)} narrowband files and {len(broadband_queue)} broadband files"
    )

    for item in track(narrowband_queue, description="Working on narrowband files..."):
        uploadToS3(item, "NARROWBAND")

    for item in track(broadband_queue, description="Working on broadband files..."):
        uploadToS3(item, "BROADBAND")

    saveJsonFile()

    print("Program finished.")


if __name__ == "__main__":
    typer.run(main)
