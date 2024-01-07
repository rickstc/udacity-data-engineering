from zipfile import PyZipFile
import httpx
import glob
import os

DATA_DIR = "/opt/airflow/data_dir"


def download_pl_data(**kwargs):
    """
    Downloads the powerlifting data
    """
    url = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"
    download_fp = os.path.join(DATA_DIR, "opl.zip")

    """
    If it has already been downloaded, skip and return the previously
    downloaded file.

    In a real project we probably wouldn't want to persist this data on disk,
    and we would want to be reacquiring data as it changed.

    However, while the student is testing this project, it didn't make sense or
    seem kind to continually redownload the dataset from the data source.
    """
    print(f"Checking to see if {download_fp} exists")
    if not os.path.exists(download_fp):
        print("Downloading from remote server")
        with httpx.stream("GET", url) as r:
            with open(download_fp, "wb") as download_file:
                for chunk in r.iter_bytes():
                    download_file.write(chunk)

    print(f"Unzipping the zip file at {download_fp}")
    # Unzip the file to the 'opl' directory
    pzf = PyZipFile(download_fp)
    pzf.extractall(os.path.join(DATA_DIR, "opl"))

    # There should be a csv file inside the zip; only one, but we don't know
    # the name of it. This will return the first CSV inside the zip
    csv_files = glob.glob(f"{DATA_DIR}/opl/**/*.csv")

    for csv_file in csv_files:
        print(f"CSV File found at: {csv_file}")
        task_instance = kwargs["task_instance"]
        task_instance.xcom_push(key="powerlifting_csv_file_path", value=csv_file)

        return csv_file
