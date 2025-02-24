import requests
import os
from PyPDF2 import PdfMerger
from datetime import datetime, timedelta
import time
import logging
from concurrent.futures import ThreadPoolExecutor

# Setup logging
logging.basicConfig(
    filename="aip_download.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Base settings
current_date = datetime.now()  # Today: 2025-02-23
output_dir = "aip_uruguay_pdfs"
os.makedirs(output_dir, exist_ok=True)
MIN_FILE_SIZE = 1024  # Minimum file size in bytes (1KB)
MAX_WORKERS = 10  # Number of parallel download workers
DATE_LIMIT_YEARS = 2  # Limit to 2 years back
MAX_CONSECUTIVE_404S = 5  # Tolerance for gaps

# URL templates for iterable groups
url_templates = {
    "General": "https://www.dinacia.gub.uy/sites/default/files/aip/{date}/Gen{iter}.pdf",
    "EnRoute": "https://www.dinacia.gub.uy/sites/default/files/aip/{date}/Enr{iter}.pdf",
    "Aerodromes": "https://www.dinacia.gub.uy/sites/default/files/aip/{date}/Ad{iter}.pdf",
    "Additional_Aerodromes": "https://www.dinacia.gub.uy/sites/default/files/aip/{date}/Ad2-{iter}.pdf"
}

# Fixed URLs for non-iterable groups
fixed_urls = {
    "Heading": "https://www.dinacia.gub.uy/sites/default/files/aip/{date}/AIP%20Uruguay%20%20CaraEspa%C3%B1ol%20.pdf",
    "Amendment": "https://www.dinacia.gub.uy/sites/default/files/aip/{date}/AIPAMDT.pdf"
}

# Headers to mimic a browser request
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Function to check if a URL exists (explicit 404 check with retries)
def url_exists(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.head(url, headers=headers, timeout=5)
            status = response.status_code
            logging.info(f"Checked {url}: HTTP {status}")
            if status == 200:
                return True
            elif status == 404:
                return False
            # Other statuses (e.g., 500, 503) trigger retry
            logging.warning(f"Unexpected status {status} for {url}, attempt {attempt + 1}/{max_retries}")
        except requests.exceptions.RequestException as e:
            logging.warning(f"Network error for {url}, attempt {attempt + 1}/{max_retries}: {e}")
            if attempt + 1 == max_retries:
                logging.error(f"Exhausted retries for {url}, treating as unavailable but not 404")
                return True  # Treat as "exists" to keep going, avoid false 404
            time.sleep(2 ** attempt)  # Exponential backoff
    return True  # Default to True after retries to avoid false 404

# Function to find the most recent valid date for a group
def find_valid_date_for_group(test_url_template, is_iterable=True):
    test_date = current_date
    limit_date = test_date.replace(year=test_date.year - DATE_LIMIT_YEARS)
    
    while test_date >= limit_date:
        date_str = test_date.strftime("%Y-%m")
        
        if is_iterable:
            iter_num = 0
            consecutive_404s = 0
            while consecutive_404s < 2:
                test_url = test_url_template.format(date=date_str, iter=iter_num)
                if url_exists(test_url):
                    logging.info(f"Found valid date for group: {date_str} (URL: {test_url})")
                    print(f"Found valid date: {date_str}")
                    return date_str
                consecutive_404s += 1
                logging.info(f"No PDF at {test_url}, iter={iter_num}, {consecutive_404s}/2 404s")
                iter_num += 1
            logging.info(f"No PDFs found for {date_str} after 2 consecutive 404s, checking previous month...")
            print(f"No PDFs for {date_str}, checking previous month...")
        else:
            test_url = test_url_template.format(date=date_str)
            if url_exists(test_url):
                logging.info(f"Found valid date for group: {date_str} (URL: {test_url})")
                print(f"Found valid date: {date_str}")
                return date_str
            logging.info(f"No PDF found for {date_str} at {test_url}, checking previous month...")
            print(f"No PDF found for {date_str}, checking previous month...")
        
        test_date = test_date.replace(day=1) - timedelta(days=1)
    
    logging.warning(f"No valid date found within {DATE_LIMIT_YEARS} years for {test_url_template}")
    print(f"No valid date found within {DATE_LIMIT_YEARS} years")
    return None

# Function to download a single PDF with retries
def download_pdf(url, filename, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            with open(filename, "wb") as f:
                f.write(response.content)
            file_size = os.path.getsize(filename)
            if file_size < MIN_FILE_SIZE:
                logging.warning(f"File {filename} too small ({file_size} bytes), skipping")
                print(f"File {filename} too small, skipping")
                os.remove(filename)
                return False
            logging.info(f"Downloaded {url} -> {filename} ({file_size} bytes)")
            print(f"Downloaded {url} -> {filename}")
            return True
        except requests.exceptions.RequestException as e:
            if attempt + 1 == max_retries:
                logging.error(f"Failed to download {url} after {max_retries} attempts: {e}")
                print(f"Failed to download {url} after {max_retries} attempts: {e}")
                return False
            logging.warning(f"Attempt {attempt + 1} failed for {url}, retrying...")
            time.sleep(2 ** attempt)
    return False

# Function to download PDFs for an iterable group, stopping after MAX_CONSECUTIVE_404S
def download_iterable_pdfs(group_name, url_template, date):
    downloaded_files = []
    iter_num = 0
    consecutive_404s = 0
    tasks = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while consecutive_404s < MAX_CONSECUTIVE_404S:
            url = url_template.format(date=date, iter=iter_num)
            filename = os.path.join(output_dir, f"{group_name}_{iter_num}.pdf")
            
            if not url_exists(url):
                consecutive_404s += 1
                logging.info(f"{group_name}: 404 at iter={iter_num} ({consecutive_404s}/{MAX_CONSECUTIVE_404S} consecutive)")
                print(f"{group_name}: 404 at iter={iter_num} ({consecutive_404s}/{MAX_CONSECUTIVE_404S} consecutive)")
            else:
                consecutive_404s = 0
                future = executor.submit(download_pdf, url, filename)
                tasks.append((future, filename))
            
            iter_num += 1
            time.sleep(0.0)
        
        logging.info(f"{group_name}: Stopped at iter={iter_num} ({MAX_CONSECUTIVE_404S} consecutive 404s)")
        print(f"{group_name}: Stopped at iter={iter_num} ({MAX_CONSECUTIVE_404S} consecutive 404s)")
        
        for future, filename in tasks:
            success = future.result()
            if success:
                downloaded_files.append(filename)
    
    return downloaded_files

# Function to download a single fixed PDF
def download_fixed_pdf(group_name, url_template, date):
    url = url_template.format(date=date)
    filename = os.path.join(output_dir, f"{group_name}.pdf")
    if url_exists(url):
        if download_pdf(url, filename):
            return [filename]
    else:
        logging.warning(f"Fixed PDF not found: {url}")
        print(f"Fixed PDF not found: {url}")
    return []

# Step 1: Download PDFs in specified order
all_downloaded_files = []

# Heading (first)
logging.info("Searching for valid date for Heading")
print("\nSearching for valid date for Heading...")
heading_date = find_valid_date_for_group(fixed_urls["Heading"], is_iterable=False)
if heading_date:
    logging.info(f"Starting download for Heading with date {heading_date}")
    print(f"Starting download for Heading with date {heading_date}...")
    heading_files = download_fixed_pdf("Heading", fixed_urls["Heading"], heading_date)
    all_downloaded_files.extend(heading_files)

# Iterable groups (General, EnRoute, Aerodromes, Additional_Aerodromes)
for group_name, url_template in url_templates.items():
    logging.info(f"Searching for valid date for {group_name}")
    print(f"\nSearching for valid date for {group_name}...")
    group_date = find_valid_date_for_group(url_template, is_iterable=True)
    if group_date:
        logging.info(f"Starting downloads for {group_name} with date {group_date}")
        print(f"Starting downloads for {group_name} with date {group_date}...")
        group_files = download_iterable_pdfs(group_name, url_template, group_date)
        all_downloaded_files.extend(group_files)

# Amendment (last)
logging.info("Searching for valid date for Amendment")
print("\nSearching for valid date for Amendment...")
amendment_date = find_valid_date_for_group(fixed_urls["Amendment"], is_iterable=False)
if amendment_date:
    logging.info(f"Starting download for Amendment with date {amendment_date}")
    print(f"Starting download for Amendment with date {amendment_date}...")
    amendment_files = download_fixed_pdf("Amendment", fixed_urls["Amendment"], amendment_date)
    all_downloaded_files.extend(amendment_files)

# Step 2: Merge all PDFs in download order
if all_downloaded_files:
    merger = PdfMerger()
    for pdf_file in all_downloaded_files:
        merger.append(pdf_file)
    
    output_pdf = f"aip_uruguay_compiled_{current_date.strftime('%Y-%m')}.pdf"
    merger.write(output_pdf)
    merger.close()
    logging.info(f"Compiled {len(all_downloaded_files)} PDFs into: {output_pdf}")
    print(f"\nCompiled {len(all_downloaded_files)} PDFs into: {output_pdf}")
else:
    logging.warning("No PDFs were downloaded to compile")
    print("\nNo PDFs were downloaded to compile.")

# Optional: Clean up individual PDFs
# for pdf_file in all_downloaded_files:
#     os.remove(pdf_file)
# os.rmdir(output_dir)
