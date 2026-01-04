from fnmatch import fnmatch
import typer
import os
import json
import boto3
from collections import defaultdict
from typing import Optional

from rich import print
from rich.progress import track
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

import firebase_admin
from google.auth import default as google_auth_default
from google.cloud import firestore

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
narrowband_label = "narrowband"
broadband_label = "broadband"

FILES_BY_DAY_COLLECTION = "files_by_day"
YEARS_STATIONS_COLLECTION = "years_stations"
AVAILABLE_DATES_COLLECTION = "available_dates"
MATRIX_COLLECTION = "matrix"

MAX_BATCH_SIZE = 500

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
        "extension": "mat",
    }


def get_broadband_data(filename: str) -> dict:
    is_fits = filename.endswith("fits")

    if is_fits:
        return {
            "Station_ID": filename[:3],
            "-": filename[3:4],
            "Year": filename[4:8],
            "Month": filename[8:10],
            "Day": filename[10:12],
            ".fits": filename[12:],
            "extension": "fits",
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
        "extension": "mat",
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

def init_firestore():
    if not firebase_admin._apps:
        firebase_admin.initialize_app()
    credentials, project = google_auth_default()
    database_id = os.getenv("FIRESTORE_DATABASE", "(default)")
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or project
    return firestore.Client(
        project=project_id,
        credentials=credentials,
        database=database_id,
    )


def infer_file_kind(file_name: str):
    if file_name.endswith(".fits") and len(file_name) == fits_broadband_file_size:
        return BROADBAND
    if file_name.endswith(".mat") and len(file_name) == narrowband_file_size:
        return NARROWBAND
    if file_name.endswith(".mat") and len(file_name) == broadband_file_size:
        return BROADBAND
    return None


def build_file_entry(file_name: str, file_kind: Optional[str] = None):
    if file_kind is None:
        file_kind = infer_file_kind(file_name)

    if file_kind is None:
        return None

    if file_kind == NARROWBAND:
        file_data = get_narrowband_data(file_name)
        year = int(f"20{file_data['Year']}")
        month = int(file_data["Month"])
        day = int(file_data["Day"])
        date_time = datetime(
            year,
            month,
            day,
            int(file_data["Hour"]),
            int(file_data["Minute"]),
            int(file_data["Second"]),
            tzinfo=tz,
        )
        station_id = file_data["Station_ID"]
        extension = file_data["extension"]
        data_path = f"{year:04d}/{month:02d}/{day:02d}"
        s3_path = f"{data_path}/narrowband/{station_id}/{file_name}"
        file_record = {
            "fileName": file_name,
            "path": s3_path,
            "url": f"https://{bucket_name}.s3.sa-east-1.amazonaws.com/{s3_path}",
            "stationId": station_id,
            "transmitter": file_data["Transmitter"],
            "dateTime": date_time,
            "CC": file_data["CC"],
            "typeABCDF": file_data["Type_ABCDF"],
            "endpointType": "AWS S3",
            "type": narrowband_label,
            "extension": extension,
        }
        type_label = narrowband_label

    else:
        file_data = get_broadband_data(file_name)
        station_id = file_data["Station_ID"]
        extension = file_data["extension"]

        if extension == "fits":
            year = int(file_data["Year"])
            month = int(file_data["Month"])
            day = int(file_data["Day"])
            date_time = datetime(year, month, day, tzinfo=tz)
        else:
            year = int(f"20{file_data['Year']}")
            month = int(file_data["Month"])
            day = int(file_data["Day"])
            date_time = datetime(
                year,
                month,
                day,
                int(file_data["Hour"]),
                int(file_data["Minute"]),
                int(file_data["Second"]),
                tzinfo=tz,
            )

        data_path = f"{year:04d}/{month:02d}/{day:02d}"
        s3_path = f"{data_path}/broadband/{station_id}/{file_name}"
        file_record = {
            "fileName": file_name,
            "path": s3_path,
            "url": f"https://{bucket_name}.s3.sa-east-1.amazonaws.com/{s3_path}",
            "stationId": station_id,
            "dateTime": date_time,
            "CC": file_data.get("CC"),
            "A": file_data.get("A"),
            "endpointType": "AWS S3",
            "type": broadband_label,
            "extension": extension,
        }
        type_label = broadband_label

    date_str = f"{year:04d}-{month:02d}-{day:02d}"
    group_id = f"{date_str}_{station_id}_{type_label}_{extension}"

    return {
        "group_id": group_id,
        "date": date_str,
        "year": year,
        "month": month,
        "day": day,
        "stationId": station_id,
        "type": type_label,
        "extension": extension,
        "file": file_record,
        "s3_path": s3_path,
    }


def upload_to_s3(path: str, file_type: str) -> None:
    file_name = os.path.basename(path)
    file_entry = build_file_entry(file_name, file_type)

    if not file_entry:
        print(f"File {path} not supported, skipping upload...")
        return

    s3.upload_file(path, bucket_name, file_entry["s3_path"])

    if file_type == NARROWBAND:
        narrowband_queue.remove(path)
    elif file_type == BROADBAND:
        broadband_queue.remove(path)

    uploaded_files.append(path)
    save_json_data()


def build_firestore_payload(file_paths):
    groups = {}
    years_stations = defaultdict(set)
    available_dates = defaultdict(
        lambda: {narrowband_label: set(), broadband_label: set()}
    )
    matrix = defaultdict(lambda: defaultdict(lambda: {"count": 0, "stations": set()}))

    for path in file_paths:
        file_name = os.path.basename(path)
        file_entry = build_file_entry(file_name)

        if not file_entry:
            continue

        group_id = file_entry["group_id"]
        group = groups.get(group_id)

        if not group:
            groups[group_id] = {
                "date": file_entry["date"],
                "year": file_entry["year"],
                "month": file_entry["month"],
                "day": file_entry["day"],
                "stationId": file_entry["stationId"],
                "type": file_entry["type"],
                "extension": file_entry["extension"],
                "files": [file_entry["file"]],
            }
        else:
            group["files"].append(file_entry["file"])

        years_stations[(file_entry["extension"], file_entry["year"])].add(
            file_entry["stationId"]
        )

        available_dates[
            (file_entry["extension"], file_entry["stationId"], file_entry["year"])
        ][file_entry["type"]].add((file_entry["month"], file_entry["day"]))

        matrix[(file_entry["extension"], file_entry["year"])][file_entry["date"]][
            "count"
        ] += 1
        matrix[(file_entry["extension"], file_entry["year"])][file_entry["date"]][
            "stations"
        ].add(file_entry["stationId"])

    for group in groups.values():
        group["files"].sort(
            key=lambda item: item.get("dateTime")
            or datetime.min.replace(tzinfo=tz)
        )
        group["fileCount"] = len(group["files"])

    return groups, years_stations, available_dates, matrix


def commit_batches(db, documents):
    if not documents:
        return

    batch = db.batch()
    count = 0

    for doc_ref, data in documents:
        batch.set(doc_ref, data)
        count += 1

        if count >= MAX_BATCH_SIZE:
            batch.commit()
            batch = db.batch()
            count = 0

    if count:
        batch.commit()


def write_firestore_documents(db, groups, years_stations, available_dates, matrix):
    print("Writing files_by_day documents...")
    files_docs = []
    for group_id, group_data in groups.items():
        doc_ref = db.collection(FILES_BY_DAY_COLLECTION).document(group_id)
        files_docs.append((doc_ref, group_data))
    commit_batches(db, files_docs)

    print("Writing years_stations documents...")
    years_docs = []
    for (extension, year), stations in years_stations.items():
        doc_ref = db.collection(YEARS_STATIONS_COLLECTION).document(
            f"{extension}_{year}"
        )
        years_docs.append(
            (
                doc_ref,
                {
                    "year": year,
                    "stations": sorted(stations),
                    "extension": extension,
                },
            )
        )
    commit_batches(db, years_docs)

    print("Writing available_dates documents...")
    dates_docs = []
    for (extension, station_id, year), types in available_dates.items():
        narrowband_dates = sorted(types[narrowband_label])
        broadband_dates = sorted(types[broadband_label])

        narrowband_payload = [
            {"day": f"{day:02d}", "month": f"{month:02d}"}
            for month, day in narrowband_dates
        ]
        broadband_payload = [
            {"day": f"{day:02d}", "month": f"{month:02d}"}
            for month, day in broadband_dates
        ]

        doc_ref = db.collection(AVAILABLE_DATES_COLLECTION).document(
            f"{extension}_{station_id}_{year}"
        )
        dates_docs.append(
            (
                doc_ref,
                {
                    "stationId": station_id,
                    "year": year,
                    "narrowband": narrowband_payload,
                    "broadband": broadband_payload,
                    "extension": extension,
                },
            )
        )
    commit_batches(db, dates_docs)

    print("Writing matrix documents...")
    matrix_docs = []
    for (extension, year), date_data in matrix.items():
        items = []
        for date, values in sorted(date_data.items()):
            items.append(
                {
                    "date": date,
                    "stations": sorted(values["stations"]),
                    "count": values["count"],
                }
            )

        doc_ref = db.collection(MATRIX_COLLECTION).document(f"{extension}_{year}")
        matrix_docs.append(
            (
                doc_ref,
                {
                    "year": year,
                    "items": items,
                    "extension": extension,
                },
            )
        )
    commit_batches(db, matrix_docs)


def main() -> None:
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

    for item in track(
        [elem for elem in narrowband_queue],
        description="Working on narrowband files",
    ):
        upload_to_s3(item, NARROWBAND)

    for item in track(
        [elem for elem in broadband_queue],
        description="Working on broadband files",
    ):
        upload_to_s3(item, BROADBAND)

    save_json_data()

    if not uploaded_files:
        print("No uploaded files found to index in Firestore.")
        return

    print("Building Firestore documents from uploaded files...")
    db = init_firestore()
    groups, years_stations, available_dates, matrix = build_firestore_payload(
        uploaded_files
    )
    write_firestore_documents(db, groups, years_stations, available_dates, matrix)

    print("Program finished.")


if __name__ == "__main__":
    typer.run(main)
