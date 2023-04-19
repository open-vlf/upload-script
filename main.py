import firebase_admin
import typer
import os
import json
import boto3

from fnmatch import fnmatch
from rich import print
from rich.progress import track
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv


from firebase_admin import firestore
from firebase_admin import credentials

root = "./"
pattern = "*.mat"
fits_pattern = "*.fits"
db_file_name = "db.json"
bucket_name = "craam-files-bucket"
tz = ZoneInfo("America/Sao_Paulo")

load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
)

NARROWBAND = "NARROWBAND"
BROADBAND = "BROADBAND"

narrowband_file_size = 26
broadband_file_size = 22
fits_broadband_file_size = 17

uploaded_files = []
narrowband_queue = []
broadband_queue = []


def get_narrowband_data(filename: str) -> dict:
    return {
        "Station_ID": filename[:2],
        "Year": filename[2:4],
        "Month": filename[4:6],
        "Day": filename[6:8],
        "Hour": filename[8:10],
        "Minute": filename[10:12],
        "Second": filename[12:14],
        "Transmitter": filename[14:17],
        "_": filename[17:18],
        "Usually not used": filename[18:19],
        "CC": filename[19:21],
        # CC — 00 for N/S channel:01 for E/W channel
        "Type_ABCDF": filename[21:22],
        # A is low resolution (1 Hz sampling rate) amplitude
        # B is low resolution (1 Hz sampling rate) phase
        # C is high resolution (50 Hz sampling rate) amplitude
        # D is high resolution (50 Hz sampling rate) phase
        # F is high resolution (50 Hz sampling rate) effective group delay
        ".mat": filename[22:26],
    }


def get_broadband_data(filename: str) -> dict:
    is_fits = filename.endswith("fits")

    if is_fits:
        return {
            "Station_ID": filename[:3],
            "-": filename[3:4],
            "Year": filename[4:8][2:],
            "Month": filename[8:10],
            "Day": filename[10:12],
            ".fits": filename[12:],
        }

    return {
        "Station_ID": filename[:2],
        "Year": filename[2:4],
        "Month": filename[4:6],
        "Day": filename[6:8],
        "Hour": filename[8:10],
        "Minute": filename[10:12],
        "Second": filename[12:14],
        "_": filename[14:15],
        "A": filename[15:16],
        # A — Sampling rate. 0 for 100 kHz sampled data (VLF), 1 for 1 MHz sampled data (LF), 2 for 25 kHz sampled data (Siple station experiment).
        "CC": filename[16:18],
        # CC — 00 for N/S channel, 01 for E/W channel
        ".mat": filename[18:22],
    }


def save_json_data() -> None:
    with open(db_file_name, "w+") as outfile:
        json.dump(
            {
                "uploaded_files": uploaded_files,
                "narrowband_queue": narrowband_queue,
                "broadband_queue": broadband_queue,
            },
            outfile,
        )


def get_json_data() -> dict:
    print("Loading storage data...")

    try:
        with open(db_file_name) as json_file:
            global uploaded_files
            global narrowband_queue
            global broadband_queue

            data = json.load(json_file)

            if data:
                uploaded_files = data["uploaded_files"]
                narrowband_queue = data["narrowband_queue"]
                broadband_queue = data["broadband_queue"]

                print("Storage data found!")

    except Exception:
        print("No storage data found")


def should_process_file(path: str) -> bool:
    return not (
        path in uploaded_files or path in narrowband_queue or path in broadband_queue
    )


def store_narrowband(file_name: str, path: str, db) -> None:
    file_data = get_narrowband_data(file_name)

    data = f'20{file_data["Year"]}/{file_data["Month"]}/{file_data["Day"]}'
    s3_path = f'{data}/narrowband/{file_data["Station_ID"]}/{file_name}'
    collection_name = f'craam/date/year/20{file_data["Year"]}/station/{file_data["Station_ID"]}/band/narrowband/month_day/{file_data["Month"]}_{file_data["Day"]}/file'

    s3.upload_file(path, bucket_name, s3_path)

    db.collection(collection_name).document(file_name).set(
        {
            "fileName": file_name,
            "path": s3_path,
            "url": f"https://{bucket_name}.s3.sa-east-1.amazonaws.com/{s3_path}",
            "stationId": file_data["Station_ID"],
            "transmitter": file_data["Transmitter"],
            "dateTime": datetime(
                int(f"20{file_data['Year']}"),
                int(file_data["Month"]),
                int(file_data["Day"]),
                int(file_data["Hour"]),
                int(file_data["Minute"]),
                int(file_data["Second"]),
                tzinfo=tz,
            ),
            "CC": file_data["CC"],
            "typeABCDF": file_data["Type_ABCDF"],
            "timestamp": firestore.SERVER_TIMESTAMP,
            "endpointType": "AWS S3",
        }
    )


def store_broadband(file_name: str, path: str, db) -> None:
    file_data = get_broadband_data(file_name)

    data = f'20{file_data["Year"]}/{file_data["Month"]}/{file_data["Day"]}'
    s3_path = f'{data}/broadband/{file_data["Station_ID"]}/{file_name}'
    collection_name = f'craam/date/year/20{file_data["Year"]}/station/band/broadband/{file_data["Station_ID"]}/month_day/{file_data["Month"]}_{file_data["Day"]}/file'

    s3.upload_file(path, bucket_name, s3_path)

    broadband_datetime = datetime(
        int(file_data["Year"]),
        int(file_data["Month"]),
        int(file_data["Day"]),
        tzinfo=tz,
    )

    if file_name.endswith(".mat"):
        broadband_datetime = datetime(
            int(f"20{file_data['Year']}"),
            int(file_data["Month"]),
            int(file_data["Day"]),
            int(file_data["Hour"]),
            int(file_data["Minute"]),
            int(file_data["Second"]),
            tzinfo=tz,
        )

    db.collection(collection_name).document(file_name).set(
        {
            "fileName": file_name,
            "path": s3_path,
            "url": f"https://{bucket_name}.s3.sa-east-1.amazonaws.com/{s3_path}",
            "stationId": file_data["Station_ID"],
            "dateTime": broadband_datetime,
            "CC": file_data.get("CC"),
            "A": file_data.get("A"),
            "timestamp": firestore.SERVER_TIMESTAMP,
            "endpointType": "AWS S3",
        }
    )


def upload_to_s3(path: str, type: str, db) -> None:
    file_name = os.path.basename(path)

    if type == NARROWBAND:
        store_narrowband(file_name, path, db)
        narrowband_queue.remove(path)

    elif type == BROADBAND:
        store_broadband(file_name, path, db)
        broadband_queue.remove(path)

    uploaded_files.append(path)

    save_json_data()


def main() -> None:
    cred = credentials.Certificate("./credentials.json")
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    get_json_data()

    for path, subdirs, files in track(
        os.walk(root), description="Finding supported files"
    ):
        for name in files:
            if fnmatch(name, pattern):
                file_path = os.path.join(path, name)

                if len(name) == narrowband_file_size and should_process_file(file_path):
                    narrowband_queue.append(file_path)

                elif len(name) == broadband_file_size and should_process_file(
                    file_path
                ):
                    broadband_queue.append(file_path)

                elif (
                    len(name) == narrowband_file_size
                    or len(name) == broadband_file_size
                ) and not should_process_file(file_path):
                    print(f"File {file_path} is already uploaded or in queue")

                else:
                    print(f"File {file_path} not supported, skipping...")

            elif fnmatch(name, fits_pattern):
                file_path = os.path.join(path, name)

                if len(name) == fits_broadband_file_size and should_process_file(
                    file_path
                ):
                    broadband_queue.append(file_path)

                else:
                    print(f"File {file_path} not supported, skipping...")

    save_json_data()

    print(
        f"Found {len(narrowband_queue)} narrowband files and {len(broadband_queue)} broadband files"
    )

    starter_narrowband_queue = [elem for elem in narrowband_queue]
    starter_broadband_queue = [elem for elem in broadband_queue]

    for item in track(
        starter_narrowband_queue,
        description="Working on narrowband files",
    ):
        upload_to_s3(item, NARROWBAND, db)

    for item in track(
        starter_broadband_queue,
        description="Working on broadband files",
    ):
        upload_to_s3(item, BROADBAND, db)

    save_json_data()

    print("Program finished.")


if __name__ == "__main__":
    typer.run(main)
