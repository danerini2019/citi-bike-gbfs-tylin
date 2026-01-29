from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import os
from os import makedirs
from datetime import datetime
import time
import glob

from toolkit.sharepoint_connection import upload_file_to_sharepoint

makedirs('data', exist_ok=True)

# copying a retry strategy from: https://www.peterbe.com/plog/best-practice-with-retries-with-requests
def requests_retry_session(
    retries=5,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

stations_get = requests_retry_session().get('https://gbfs.citibikenyc.com/gbfs/en/station_information.json')
status_get = requests_retry_session().get('https://gbfs.citibikenyc.com/gbfs/en/station_status.json')

status_get.raise_for_status() # should raise error and end execution if there was a connection error

status_json = status_get.json() # should raise error if the json is unparsable 
station_json = stations_get.json() # should raise error if the json is unparsable

timestamp = status_json['last_updated']

# Sharepoint retry function
def retry(func, retries=3, delay=5, *args, **kwargs):
    """ Retry function to handle transient failures """
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    raise Exception(f"Function {func.__name__} failed after {retries} attempts")

# cleanup temp folder
def cleanup_files(directory):
    """ Cleans up files in the given directory """
    files = glob.glob(os.path.join(directory, '*'))
    for file in files:
        if os.path.isfile(file):
            os.remove(file)

current_directory = os.getcwd()
station_file_name = os.path.join(current_directory, "data", f"{timestamp}_stations.json")
status_file_name = os.path.join(current_directory, "data", f"{timestamp}_status.json")

with open(station_file_name, 'w') as stations_file:
    json.dump(stations_get.json(), stations_file)

with open(status_file_name, 'w') as status_file:
    json.dump(status_json, status_file)

# Validate the file exists before attempting upload
print(f"Uploading file to sharepoint.")
site_url = 'https://tylin1.sharepoint.com/teams/SSC23-05-0400/'
library_name = 'Documents'
folder_path = f'23-05-0400/04 - Analysis/01 - Data/_temp/azure_test'

retry(
    upload_file_to_sharepoint,  # func
    3,                          # retries
    10,                         # delay
    site_url,                   # site_url
    library_name,               # library_name  
    station_file_name,
    folder_path                 # folder_path (destination in SharePoint)
)

print(f"Wrote {station_file_name} to {folder_path}")

retry(
    upload_file_to_sharepoint,  # func
    3,                          # retries
    10,                         # delay
    site_url,                   # site_url
    library_name,               # library_name  
    status_file_name,
    folder_path                 # folder_path (destination in SharePoint)
)

print(f"Wrote {status_file_name} to {folder_path}")

cleanup_files(os.path.dirname(station_file_name))
cleanup_files(os.path.dirname(status_file_name))

print("Cleaned up temporary files.")