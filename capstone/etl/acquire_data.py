import sys
from zipfile import PyZipFile
import httpx
import glob
import os

def download_pl_data():
    url = 'https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip'
    download_fp = 'opl.zip'

    if os.path.exists(download_fp):
        return download_fp
    
    with httpx.stream("GET", url) as r:
        with open('opl.zip', 'wb') as download_file:
            for chunk in r.iter_bytes():
                download_file.write(chunk)

    return download_fp

def extract_data(fp, dirfp):
    pzf = PyZipFile(fp)
    pzf.extractall(dirfp)
            
def import_pl_data():
    extracted_dirname = 'opl'
    fp = download_pl_data()
    extract_data(fp, extracted_dirname)
    csv_files = glob.glob(f"{extracted_dirname}/**/*.csv")
    for csv_file in csv_files
    print(csv_files)

def start():
    import_pl_data()

if __name__ == '__main__':
    start()