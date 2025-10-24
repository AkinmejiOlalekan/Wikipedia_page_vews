from airflow.providers.postgres.hooks.postgres import PostgresHook

import os
import logging
from dotenv import load_dotenv

import requests
import gzip
import csv

load_dotenv()


logging.basicConfig(level=logging.INFO, 
                    format="{asctime} - {levelname} - {message}",
                    style="{",
                    datefmt="%Y-%m-%d %H:%M",
                    filename="pipeline.log",
                    encoding="utf-8",
                    filemode="a"
                    )

TARGET_COMPANIES = ["Google", "Facebook", "Apple", "Amazon", "Microsoft"]

DB_CONN_ID = "conn_id"
TABLE_NAME = "Wikipedia_views"

target_date=os.getenv("TARGET_DATE")
target_hour=os.getenv("TARGET_HOUR")
base_url=os.getenv("WIKI_BASE_URL")
year=os.getenv("TARGET_DATE")[:-4]
year_month=f"{year}-{target_date[4:6]}"


dir_path = os.getenv("DIR", "./tmp/wikipedia_pageviews")

file_download = os.path.join(dir_path, f"pageviews-{target_date}-{target_hour}.gz")
file_extracted = os.path.join(dir_path, f"pageviews-{target_date}-{target_hour}.txt")
file_filtered = os.path.join(dir_path, "pageviews.csv")

filename=f"pageviews-{target_date}-{target_hour}.gz"
url=f"{base_url}/{year}/{year_month}/{filename}"

def setup_environment() -> None:
    """
    Creates all the neccessary directories for the file storage.
    This ensures the pipeline has a place to store downloaded files
    """
    os.makedirs(os.getenv("DIR"), exist_ok=True)
    logging.info(f"Creating a path directory for pageview data")

def download_data(**kwargs) -> str:
    """
    Download the Wikipedia pageview gzip file for the specified date and
    hour in the .env file
    """
    setup_environment()

    logging.info(f"Downloading Wikipedia pageview data from {url}")

    try:
        response=requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        total_size=0
        with open(file_download, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    total_size += len(chunk)

        if total_size == 0:
            raise Exception("Downloded file is empty")
            
        logging.info(f"Successfully downloaded {total_size} bytes of Wikipedia pageview data to {file_download}")
        
        return file_download
    
    except Exception as e:
        logging.exception(f"Failed to download file to {file_download} due to: {str(e)}")
        raise

def extract_gzip_file(ti=None) -> str:
    """
    Extracts the donwloaded gzip file to a text file.
    """
    message=ti.xcom_pull(task_ids='download_data')
    
    logging.info(f"Extracting gzip file: {message}")
    
    try:
        with gzip.open(message, 'rb') as f_in:
            with open(file_extracted, 'wb') as f_out:

                chunk_size = 8192
                while True:
                    chunk = f_in.read(chunk_size)
                    if not chunk:
                        break
                    f_out.write(chunk)

        file_size = os.path.getsize(file_extracted)
        if file_size == 0:
            raise Exception("Extracted file is empty. Look into your process to see where the issue is coming from")

        logging.info(f"Successfully extracted {file_size} bytes to {file_extracted}")

        return file_extracted

    except Exception as e:
        logging.exception(f"Failed to extract gzip file: {str(e)}")
        raise

def filter_to_csv(ti=None) -> str:
    """
    Reads the extracted Wikipedia data text file for the target companies,
    and saves the results to a csv
    """
    extracted_data=ti.xcom_pull(task_ids='extracted_gzip_file')

    if not extracted_data or not os.path.exists(file_extracted):
        raise Exception("Extracted text file not found")

    target_companies = set(TARGET_COMPANIES)
    lines_processed = 0
    matches_found = 0

    with open(file_extracted, 'r', encoding='utf-8') as in_file:
        with open(file_filtered, 'w', newline='', encoding='utf-8') as csvfile:

            writer = csv.writer(csvfile)
            writer.writerow(['domain_code', 'page_title', 'view_count', 'target_date', 'target_hour'])

            for line in in_file:
                lines_processed += 1
                parts = line.strip().split()
                if len(parts) < 3:
                    continue

                domain_code, page_title, view_count = parts[0], parts[1], parts[2]
                #if not domain_code.startswith('en'):
                   # continue

                if page_title in target_companies:
                    writer.writerow([domain_code, page_title, view_count, target_date, target_hour])
                    matches_found += 1

    logging.info(f"Processed {lines_processed} lines, found {matches_found} matches")

    return file_filtered

def csv_to_database(ti=None, **context) -> None:
    """
    Loads the filtered CSV data into PostgreSQL
    """
    csv_data = ti.xcom_pull(task_ids='filtered_csv')

    if not csv_data or not os.path.exists(file_filtered):
        raise Exception("Filtered CSV file not found")

    pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    logging.info("=" * 80)
    logging.info(f"Loading data from {file_filtered} into Postgres database with the table nameD: {TABLE_NAME}")
    logging.info("=" * 80)
    
    execution_date = context['logical_date']

    with open(file_filtered, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            cursor.execute(
                f"""
                INSERT INTO {TABLE_NAME} (domain_code, page_title, view_count, target_date, target_hour, execution_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    row['domain_code'],
                    row['page_title'],
                    int(row['view_count']),
                    row['target_date'],
                    row['target_hour'],
                    execution_date
                )
            )

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Data successfully loaded into PostgreSQL")

def cleanup_files(**kwargs) -> None:
    """
    Deletes temporary files created during the data pipeline exceution
    """
    logging.info("-" * 80)
    logging.info("Cleaning up temporary files...")
    logging.info("-" * 80)

    files_to_remove = [file_download, file_extracted, file_filtered]

    for file_path in files_to_remove:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logging.info("+" * 40)
                logging.info(f"Removed: {file_path}")
                logging.info("+" * 40)
        except Exception as e:
            logging.warning(f"Failed to remove {file_path}: {str(e)}")

    logging.info("Successfully cleanup the existing files")