import httpx
import os


def download_file(url, output_fp):
    os.makedirs(os.path.dirname(output_fp), exist_ok=True)
    with httpx.stream("GET", url) as r:
        if r.status_code == 200:
            with open(output_fp, "wb") as download_file:
                for chunk in r.iter_bytes():
                    download_file.write(chunk)

            return output_fp
    print(f"Response status code: {r.status_code} for request to URL: {url}")
    return None


def download_readme():
    url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt"
    download_fp = os.path.join("./weather/readme.txt")
    # Download if it doesn't already exist locally
    if not os.path.exists(download_fp):
        return download_file(url, download_fp)
    return download_fp


def download_stations():
    url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
    download_fp = os.path.join("./weather/stations.txt")
    # Download if it doesn't already exist locally
    if not os.path.exists(download_fp):
        return download_file(url, download_fp)
    return download_fp


def download_cities_db():
    url = "https://gist.githubusercontent.com/curran/13d30e855d48cdd6f22acdf0afe27286/raw/0635f14817ec634833bb904a47594cc2f5f9dbf8/worldcities_clean.csv"
    download_fp = os.path.join("./weather/cities.csv")
    # Download if it doesn't already exist locally
    if not os.path.exists(download_fp):
        return download_file(url, download_fp)
    return download_fp


def start():
    # NOAA GHCN Readme - data dictionary
    download_readme()

    # Weather Stations
    download_stations()

    # Cities Info
    download_cities_db()


if __name__ == "__main__":
    start()
