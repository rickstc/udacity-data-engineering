from zipfile import PyZipFile
import httpx
import glob
import os

DATA_DIR = "/opt/airflow/data_dir"


def download_location_data(**kwargs):
    """
    Downloads location data
    """
    url = "https://gist.githubusercontent.com/curran/13d30e855d48cdd6f22acdf0afe27286/raw/0635f14817ec634833bb904a47594cc2f5f9dbf8/worldcities_clean.csv"
    download_fp = os.path.join(DATA_DIR, "cities.csv")

    """
    If it has already been downloaded, skip and return the previously
    downloaded file.

    In a real project we probably wouldn't want to persist this data on disk,
    and we would want to be reacquiring data as it changed.

    However, while the student is testing this project, it didn't make sense or
    seem kind to continually download the dataset from the data source.
    """
    print(f"Checking to see if {download_fp} exists")
    if not os.path.exists(download_fp):
        print("Downloading from remote server")
        with httpx.stream("GET", url) as r:
            with open(download_fp, "wb") as download_file:
                for chunk in r.iter_bytes():
                    download_file.write(chunk)

    task_instance = kwargs["task_instance"]
    task_instance.xcom_push(key="location_csv_file_path", value=download_fp)
