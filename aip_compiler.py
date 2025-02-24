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

# URL templates for each group
url_templates = {
    "General": "https://www.dinacia.gub.uy/sites/default/files/aip/{date}/Gen{iter}.pdf",
    "EnRoute": "https://www.dinacia.gub.uy/sites/default/files/aip/{date}/Enr{iter}.pdf",
    "Aerodromes": "https://www.dinacia.gub.uy/sites/default/files/aip/{date}/Ad{iter}.pdf",
    "Additional_Aerodromes": "https://www.dinacia.gub.uy/sites/default/files/aip/{date}/Ad2-{iter}.pdf"
}

# Headers to mimic a browser request
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Function to check if a URL exists (returns True if 200, False if 404 or error)
def url_exists(url):
    try:
        response = requests.head(url, headers=headers, timeout=5)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

# Function to find the most recent valid date
def find_valid_date():
    test_date = current_date
    while test_date.year >= 2020:
        date_str = test_date.strftime("%Y-%m")
        for group_name, url_template in url_templates.items():
            test_url = url_template.format(date=date_str, iter=0)
            if url_exists(test_url):
                logging.info(f"Found valid date: {date_str} (PDF exists at {test_url})")
                print(f"Found valid date: {date_str}")
                return date_str
        test_date = test_date.replace(day=1) - timedelta(days=1)
        logging.info(f"No PDFs found for {date_str}, checking previous month...")
        print(f"No PDFs found for {date_str}, checking previous month...")
    raise Exception("No valid PDFs found back to 2020.")

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
            time.sleep(2 ** attempt)  # Exponential backoff
    return False

# Function to download PDFs for a group, stopping after two consecutive 404s
def download_pdfs(group_name, url_template, date):
    downloaded_files = []
    iter_num = 0
    consecutive_404s = 0
    tasks = []  # List of (future, filename) pairs for ordered processing
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while consecutive_404s < 2:
            url = url_template.format(date=date, iter=iter_num)
            filename = os.path.join(output_dir, f"{group_name}_{iter_num}.pdf")
            
            if not url_exists(url):
                consecutive_404s += 1
                logging.info(f"{group_name}: 404 at iter={iter_num} ({consecutive_404s}/2 consecutive)")
                print(f"{group_name}: 404 at iter={iter_num} ({consecutive_404s}/2 consecutive)")
            else:
                consecutive_404s = 0
                future = executor.submit(download_pdf, url, filename)
                tasks.append((future, filename))
            
            iter_num += 1
            time.sleep(0.0)  # Rate limiting between checks
        
        logging.info(f"{group_name}: Stopped at iter={iter_num} (2 consecutive 404s)")
        print(f"{group_name}: Stopped at iter={iter_num} (2 consecutive 404s)")
        
        # Collect results in submission order
        for future, filename in tasks:
            success = future.result()  # Wait for completion
            if success:
                downloaded_files.append(filename)
    
    return downloaded_files

# Step 1: Find the most recent valid date
logging.info("Starting search for most recent valid date")
print("Searching for the most recent valid date...")
base_date = find_valid_date()

# Step 2: Download PDFs for each group in order
all_downloaded_files = []
for group_name, url_template in url_templates.items():
    logging.info(f"Starting downloads for {group_name} with date {base_date}")
    print(f"\nStarting downloads for {group_name} with date {base_date}...")
    group_files = download_pdfs(group_name, url_template, base_date)
    all_downloaded_files.extend(group_files)

# Step 3: Merge all PDFs in download order
if all_downloaded_files:
    merger = PdfMerger()
    for pdf_file in all_downloaded_files:
        merger.append(pdf_file)
    
    output_pdf = f"aip_uruguay_compiled_{base_date}.pdf"
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