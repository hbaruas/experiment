import requests
import pandas as pd
from tqdm import tqdm
import time
import urllib3
import os

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Your REED API Key here
API_KEY = 'f28399a0-0590-48ad-b00c-3d5483f78c9d'

# Constants
BASE_URL = 'https://www.reed.co.uk/api/1.0/search'
PAGE_SIZE = 25
MAX_EMPTY_RESPONSES = 5  # stop after multiple empty pages

# Output files
FINAL_FILE = 'reed_jobs_uk_extended.csv'
PARTIAL_FILE = 'reed_jobs_partial.csv'

# Resumable storage
all_jobs = []
page = 1
empty_page_count = 0

print(" Starting job scraping...")

# Create progress bar without knowing total jobs ahead of time
progress = tqdm(desc='Fetching Jobs', ncols=100)

try:
    while True:
        params = {
            'resultsToTake': PAGE_SIZE,
            'resultsToSkip': (page - 1) * PAGE_SIZE
        }

        response = requests.get(
            BASE_URL,
            headers={'Accept': 'application/json'},
            auth=(API_KEY, ''),
            params=params,
            verify=False
        )

        if response.status_code != 200:
            print(f" Error {response.status_code}: {response.text}")
            break

        jobs = response.json().get('results', [])

        if not jobs:
            empty_page_count += 1
            print(f"Empty page #{empty_page_count} (Page {page})")
            if empty_page_count >= MAX_EMPTY_RESPONSES:
                print("Too many empty pages, assuming end of data.")
                break
        else:
            empty_page_count = 0

        for job in jobs:
            if job.get('jobId') and job.get('jobTitle'):
                all_jobs.append(job)
                progress.update(1)

        # Save progress after every page
        pd.DataFrame(all_jobs).to_csv(PARTIAL_FILE, index=False)

        page += 1
        time.sleep(0.5)

except Exception as e:
    print(f"\n Script failed with error: {e}")
    print(" Saving partial progress...")

finally:
    progress.close()
    pd.DataFrame(all_jobs).to_csv(FINAL_FILE, index=False)
    print(f"\n Finished. Total jobs scraped: {len(all_jobs)}")
    print(f" Final saved to: {FINAL_FILE}")
