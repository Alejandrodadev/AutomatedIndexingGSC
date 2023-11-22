import os
import ssl
import time
import json
import hashlib
import threading
import pandas as pd
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials

# Constants used in the application
MAIN_SERVICE_ACCOUNT = 'main_service_account'
SCOPES = ['https://www.googleapis.com/auth/webmasters']
DATA_DIR = 'data'
JSON_DIR = 'json'
OUTPUT_DIR = 'processed'
CACHE_DIR = 'cache'
EXCEL_FILE = None
request_counters = {}
REQUIRED_COLUMNS = [
    'STATUS', 'ESTADO DE INDEXACIÓN', 'STATUS DE COBERTURA',
    'ROBOTS.TXT STATUS', 'PAGEFETCH STATUS', 'ÚLTIMO RASTREO', 'CRAWLED AS'
]

# Dictionary storing credentials to avoid multiple authentications
authenticated_clients = {}

# Find the Excel file
def find_excel_file(directory):
    for file_name in os.listdir(directory):
        if file_name.endswith('.xlsx'):
            return os.path.join(directory, file_name)
    return None


''' Functions to handle a 'cache' system. Saves a .json for each processed URL,
 so a 'backup' is obtained in case the program stops or fails, and the progress can be recovered.
'''


def load_from_cache(url):
    cache_file_path = os.path.join(
        CACHE_DIR, f'{hashlib.md5(url.encode()).hexdigest()}.json')
    if os.path.exists(cache_file_path):
        with open(cache_file_path, 'r') as f:
            return json.load(f)
    return None


def save_to_cache(url, data):
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)

    cache_file_path = os.path.join(
        CACHE_DIR, f'{hashlib.md5(url.encode()).hexdigest()}.json')
    with open(cache_file_path, 'w') as f:
        json.dump(data, f)

# Read DataFrames from the Excel file

def read_data_frames(excel_file_path):
    return pd.read_excel(excel_file_path, sheet_name=0), pd.read_excel(excel_file_path, sheet_name=1)


# Authenticate the client with Google Search Console
def authenticate_client(service_account):
    if service_account in authenticated_clients:
        return authenticated_clients[service_account]

    credentials_path = os.path.join(JSON_DIR, f'{service_account}.json')
    credentials = Credentials.from_service_account_file(
        credentials_path, scopes=SCOPES)
    client = build('searchconsole', 'v1', credentials=credentials)
    authenticated_clients[service_account] = client
    return client


# Make a request to Google Search Console and get the data
def fetch_from_google(inspection_url, site_url, search_console):
    request_body = {
        "inspectionUrl": inspection_url,
        "siteUrl": site_url
    }

    cache = load_from_cache(inspection_url)

    if cache:
        return cache

    try:
        request = search_console.urlInspection().index().inspect(body=request_body)
        response = request.execute()
        index_result = response.get(
            'inspectionResult', {}).get('indexStatusResult', {})
        save_to_cache(inspection_url, index_result)
        return index_result
    except HttpError as e:
        if e.resp.status == 429:
            print(f"Request limit exceeded. Sleeping for 24 hours.")
            time.sleep(86400)
            print("Resuming execution after the pause.")
        else:
            print(f"HTTP Error occurred: {e}")
        return None
    except ssl.SSLError as e:
        print(f"SSL Error: {e}")
        return None

# Function to handle each thread

def handle_group(group_name, group_df):
    search_console = authenticate_client(MAIN_SERVICE_ACCOUNT)
    return update_single_group(group_df, group_name, search_console)


# Function to handle a single group (all URLs for a property)
def update_single_group(group_df, property_name, search_console):
    global request_counters

    if property_name not in request_counters:
        request_counters[property_name] = 0

    for index, row in group_df.iterrows():
        # Check if we have reached the limit of 2000 requests per property
        if request_counters[property_name] >= 2000:
            print(
                f"Limit of 2000 requests reached for {property_name}. Sleeping for 24 hours.")
            time.sleep(86400)  # Pause the app for 24 hours
            request_counters[property_name] = 0  # Reset the counter

        user_email = row['USER']
        site_url = row['PROPERTY']
        inspection_url = row['URL']

        # Normalize the URL to avoid errors
        if site_url.startswith(('http://', 'https://')):
            if not site_url.endswith('/'):
                site_url += '/'
        else:
            if site_url.endswith('/'):
                site_url = site_url[:-1]
            site_url = f'sc-domain:{site_url}'

        print(f"Inspecting URL: {inspection_url} for {property_name}")

        index_result = fetch_from_google(
            inspection_url, site_url, search_console)

        if index_result:
            # Extract and update information in df2
            extracted_data = {
                'STATUS': index_result.get('verdict'),
                'INDEXING STATE': index_result.get('indexingState'),
                'COVERAGE STATE': index_result.get('coverageState'),
                'ROBOTS.TXT STATE': index_result.get('robotsTxtState'),
                'PAGEFETCH STATE': index_result.get('pageFetchState'),
                'LAST CRAWL': index_result.get('lastCrawlTime'),
                'CRAWLED AS': index_result.get('crawledAs'),
            }

            for key, value in extracted_data.items():
                group_df.loc[index, key] = value

            # Update information in df1
            user_row = df1.loc[df1['USER'] == user_email].index[0]
            df1.loc[user_row, 'LAST ACCESS DATE'] = datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S")
            df1.loc[user_row, 'TOTAL COUNT'] += 1

            # Increment the request counter for this property
            request_counters[property_name] += 1

    return group_df


# Save DataFrames to an Excel file in the "processed" directory
def save_to_excel(df1, df2, output_dir):
    # Create the directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Output Excel file name with timestamp
    output_file = os.path.join(
        output_dir, f'processed_data_{datetime.now().strftime("%Y%m%d%H%M%S")}.xlsx')

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df1.to_excel(writer, sheet_name='Sheet1', index=False)
        df2.to_excel(writer, sheet_name='Sheet2', index=False)

        # Adjust column width in Excel
        wb = writer.book
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            for column in ws.columns:
                max_length = 0
                column = [cell for cell in column]
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(cell.value)
                    except:
                        pass
                adjusted_width = (max_length + 5)
                ws.column_dimensions[column[0].column_letter].width = adjusted_width

        wb.save(output_file)


# Preparation and execution order of the program
start_time = time.time()

if __name__ == "__main__":
    EXCEL_FILE = find_excel_file(DATA_DIR)
    if EXCEL_FILE is None:
        print("No Excel file found in the 'data' folder.")
        exit()

    df1, df2 = read_data_frames(EXCEL_FILE)

    # DataFrame preprocessing
    df1['LAST ACCESS DATE'] = df1['LAST ACCESS DATE'].astype(str)
    df1['TOTAL COUNT'] = df1['TOTAL COUNT'].fillna(0)

    # Check if the required columns exist and have the correct data type
    for col in REQUIRED_COLUMNS:
        if col not in df2.columns:
            df2[col] = None
        df2[col] = df2[col].astype('object')

    # Create a dictionary to store modified DataFrames
    updated_groups = {}

    # Group df2 DataFrame by property and handle each group in a separate thread
    grouped = df2.groupby('PROPERTY')
    threads = []
    for name, group in grouped:
        t = threading.Thread(target=lambda q, arg1, arg2: q.update({arg1: handle_group(arg1, arg2)}),
                             args=(updated_groups, name, group))
        t.start()
        # Save the thread to wait for its completion later
        threads.append(t)

    # Wait for all threads to finish before continuing
    for t in threads:
        t.join()

    # Replace the groups in df2 with the modified versions
    for name, updated_group in updated_groups.items():
        df2.loc[df2['PROPERTY'] == name] = updated_group

    # Save changes to Excel in the "processed" directory
    save_to_excel(df1, df2, OUTPUT_DIR)

    end_time = time.time()
    execution_time = end_time - start_time
    execution_time_minutes = round(execution_time / 60, 2)
    print(f"Total execution time {execution_time_minutes} minutes.")


