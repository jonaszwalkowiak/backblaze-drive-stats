import requests
from airflow.sdk import dag, task
from google.cloud import storage
from datetime import datetime
from bs4 import BeautifulSoup
import json


@dag(
    dag_id="backblaze_drive_stats",
    tags=["backblaze"],
    default_args={"owner": "JW"},
    schedule="20 4 25 * *",
    catchup=False,
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2030, 1, 1),
)
def dag_creator():
    url = "https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data"
    destination_blob_name = "zip_links.json"
    bucket_name = "backblaze-drive-stats"

    @task
    def list_reader(bucket_name, destination_blob_name):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        try:
            json_data = blob.download_as_bytes().decode("utf-8")
            links_list = json.loads(json_data)
            print(
                f"Downloaded storage object {destination_blob_name} from bucket {bucket_name}."
            )
            return set(links_list)
        except Exception as e:
            print(f"File not found or error occurred: {e}. Returning empty set.")
            return set()

    @task
    def data_scraper(url):
        raw_data = requests.get(url)
        soup = BeautifulSoup(raw_data.text, "html.parser")

        zip_links = []

        for a in soup.find_all("a", href=True):
            link = a["href"]
            if link.endswith(".zip"):
                zip_links.append(link)

        print(zip_links)
        return set(zip_links)

    @task.short_circuit
    def list_uploader(
        list_from_bucket_xcom, zip_links_xcom, bucket_name, destination_blob_name
    ):
        if list_from_bucket_xcom != zip_links_xcom:
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            links_list = sorted(list(zip_links_xcom))
            json_data = json.dumps(links_list, indent=2)

            blob.upload_from_string(json_data, content_type="application/json")
            print(f"[#] File {destination_blob_name} uploaded to {bucket_name}! [#]")
            print(f"[#] Found new links! [#]")
            print((zip_links_xcom - list_from_bucket_xcom))
            return True
        else:
            print("[#] There are no new links! [#]")
            return False

    @task
    def download_zip_to_gcs(list_from_bucket_xcom, zip_links_xcom, bucket_name):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        only_new_links = set(zip_links_xcom - list_from_bucket_xcom)

        print(f"[#] Processing {len(only_new_links)} new files [#]")

        for url in only_new_links:
            file_name_from_url = url[-16:]
            if "/" in file_name_from_url:
                file_name_from_url = url[-13:]
            blob_name = f"{file_name_from_url}"
            blob = bucket.blob(blob_name)
            file = requests.get(url)
            blob.upload_from_string(file.content, content_type="application/zip")
            print(f"[#] File {blob_name} saved to GCS! [#]")
            del file

    list_from_bucket_xcom = list_reader(bucket_name, destination_blob_name)
    zip_links_xcom = data_scraper(url)
    list_uploader_decision = list_uploader(
        list_from_bucket_xcom, zip_links_xcom, bucket_name, destination_blob_name
    )
    download_zip_task = download_zip_to_gcs(
        list_from_bucket_xcom, zip_links_xcom, bucket_name
    )

    (
        [list_from_bucket_xcom, zip_links_xcom]
        >> list_uploader_decision
        >> download_zip_task
    )


dag = dag_creator()
