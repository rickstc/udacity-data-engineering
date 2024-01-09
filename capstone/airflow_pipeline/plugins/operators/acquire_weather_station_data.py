from zipfile import PyZipFile
import httpx
import glob
import os

DATA_DIR = "/opt/airflow/data_dir"


def download_weather_station_data(**kwargs):
    """
    Downloads the weather station data
    """
    url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
    download_fp = os.path.join(DATA_DIR, "stations.txt")

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
    task_instance.xcom_push(key="weather_station_text_file_path", value=download_fp)
