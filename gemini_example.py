import requests
import os
import time
import re
import gzip
import shutil

# --- Configuration ---
# List of hospital systems/domains to check in the Seattle area.
# You may need to refine this list or find more specific domains.
HOSPITAL_SYSTEMS = {
    "UW Medicine": "https://www.uwmedicine.org",
    "Swedish (Providence)": "https://www.swedish.org", # Swedish is part of Providence
    "Providence Washington": "https://www.providence.org", # General Providence, might cover multiple WA hospitals
    "Virginia Mason Franciscan Health": "https://www.vmfh.org",
    "Overlake Hospital Medical Center": "https://www.overlakehospital.org",
    "EvergreenHealth": "https://www.evergreenhealth.com",
    "MultiCare Health System": "https://www.multicare.org",
    "Seattle Children's Hospital": "https://www.seattlechildrens.org",
    "Kaiser Permanente Washington": "https://healthy.kaiserpermanente.org" # KP often has specific state subdomains
}

# Directory to save downloaded files
OUTPUT_DIR = "hospital_mrf_seattle"

# User agent to mimic a browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Delay between requests to a domain to be respectful
REQUEST_DELAY = 2  # seconds

# --- Helper Functions ---

def ensure_dir(directory):
    """Creates the directory if it doesn't exist."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

def download_file(url, output_path):
    """Downloads a file from a URL to the specified output path."""
    print(f"Attempting to download: {url}")
    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=120) # Increased timeout for potentially very large files
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192 # 8KB
        
        with open(output_path, "wb") as f:
            downloaded_size = 0
            for chunk in response.iter_content(chunk_size=block_size):
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    if total_size > 0:
                        progress = (downloaded_size / total_size) * 100
                        print(f"  Downloading... {downloaded_size / (1024*1024):.2f}MB / {total_size / (1024*1024):.2f}MB ({progress:.2f}%) \r", end="")
        print(f"\n  Successfully downloaded to: {output_path}")
        
        if output_path.endswith(".gz"):
            decompressed_path = output_path[:-3]
            print(f"  Gzipped file detected. Decompressing to: {decompressed_path}")
            try:
                with gzip.open(output_path, 'rb') as f_in:
                    with open(decompressed_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                print(f"  Successfully decompressed to: {decompressed_path}")
                os.remove(output_path)
                print(f"  Removed original .gz file: {output_path}")
            except Exception as e:
                print(f"  Error decompressing {output_path}: {e}")
                print(f"  The gzipped file remains at: {output_path}")
        
        return True
    except requests.exceptions.Timeout:
        print(f"  Timeout downloading {url}. The file might be too large or the server too slow.")
        return False
    except requests.exceptions.RequestException as e:
        print(f"  Error downloading {url}: {e}")
        return False
    except Exception as e:
        print(f"  An unexpected error occurred while downloading {url}: {e}")
        return False

def parse_cms_hpt_txt(content):
    """
    Parses the content of a cms-hpt.txt file to extract MRF URLs and their likely format.
    Returns a list of dictionaries: [{'url': str, 'format': str ('csv', 'json', 'unknown')}]
    """
    parsed_entries = []
    # Regex to find mrf-url lines, case insensitive, handling potential extra spaces
    # Example line: mrf-url: https://example.com/123456789_hospital_standardcharges.json
    regex = re.compile(r"^\s*mrf-url\s*:\s*(https?://\S+)", re.IGNORECASE | re.MULTILINE)
    matches = regex.findall(content)
    
    raw_urls = []
    for url in matches:
        raw_urls.append(url.strip())
    
    # Fallback for simpler parsing if regex finds nothing or to catch additional direct links
    if not matches:
        lines = content.splitlines()
        for line in lines:
            stripped_line = line.strip()
            line_lower = stripped_line.lower()
            # Check for lines that look like mrf-url definitions
            if "mrf-url:" in line_lower:
                try:
                    url_part = stripped_line.split(":", 1)[1].strip()
                    if url_part.startswith("http"):
                        raw_urls.append(url_part)
                except IndexError:
                    continue # Malformed line
            # Check for lines that are just URLs (less common for cms-hpt.txt main MRF links but possible)
            elif stripped_line.startswith("http") and \
                 (".json" in line_lower or ".csv" in line_lower) and \
                 ("standardcharges" in line_lower or "price-transparency" in line_lower or "mrf" in line_lower):
                 raw_urls.append(stripped_line)

    unique_urls = sorted(list(set(raw_urls))) # Process unique URLs, sorted for consistency

    for url_str in unique_urls:
        url_lower = url_str.lower()
        file_format = 'unknown'
        # Prioritize .csv.gz or .csv
        if '.csv.gz' in url_lower or url_lower.endswith('.csv'):
            file_format = 'csv'
        elif '.json.gz' in url_lower or url_lower.endswith('.json'):
            file_format = 'json'
        # Add more specific checks if needed, e.g., for .xml
        
        parsed_entries.append({'url': url_str, 'format': file_format})
        
    return parsed_entries

# --- Main Script ---
def main():
    print("Starting Seattle Hospital MRF Downloader (CSV Prioritized)...")
    ensure_dir(OUTPUT_DIR)

    for hospital_name, base_url in HOSPITAL_SYSTEMS.items():
        print(f"\n--- Processing: {hospital_name} ({base_url}) ---")
        
        clean_base_url = base_url.rstrip('/')
        cms_hpt_txt_url = f"{clean_base_url}/cms-hpt.txt"
        
        print(f"Attempting to fetch cms-hpt.txt from: {cms_hpt_txt_url}")
        
        try:
            time.sleep(REQUEST_DELAY)
            response = requests.get(cms_hpt_txt_url, headers=HEADERS, timeout=30)
            
            if response.status_code == 200:
                print("  cms-hpt.txt found. Parsing for MRF URLs...")
                mrf_entries = parse_cms_hpt_txt(response.text)
                
                csv_links = [entry for entry in mrf_entries if entry['format'] == 'csv']
                json_links = [entry for entry in mrf_entries if entry['format'] == 'json']
                unknown_links = [entry for entry in mrf_entries if entry['format'] == 'unknown' and entry not in csv_links and entry not in json_links]
                
                # Order: CSVs first, then JSONs, then Unknowns
                ordered_mrf_links = csv_links + json_links + unknown_links
                
                if ordered_mrf_links:
                    print(f"  Found {len(ordered_mrf_links)} potential MRF URL(s) (prioritizing CSV):")
                    for i, mrf_entry in enumerate(ordered_mrf_links):
                        mrf_url = mrf_entry['url']
                        mrf_format = mrf_entry['format']
                        print(f"    {i+1}. {mrf_url} (Detected format: {mrf_format})")
                        
                        try:
                            file_name_from_url = mrf_url.split("/")[-1]
                            # Remove query parameters from filename if any
                            file_name_from_url = file_name_from_url.split("?")[0]
                            file_name_safe = "".join(c if c.isalnum() or c in ('.', '_', '-') else '_' for c in file_name_from_url)
                            
                            if not file_name_safe or file_name_safe == "_": # if the url ends with / or only had non-alphanum characters
                                extension = mrf_format if mrf_format != 'unknown' else 'dat' # Use 'dat' for truly unknown
                                file_name_safe = f"{hospital_name.replace(' ','_')}_mrf_{i+1}.{extension}"
                        except Exception:
                             extension = mrf_format if mrf_format != 'unknown' else 'dat'
                             file_name_safe = f"{hospital_name.replace(' ','_')}_mrf_{i+1}.{extension}"

                        # Ensure the filename has an extension if the original URL part didn't
                        if '.' not in file_name_safe and mrf_format != 'unknown':
                            file_name_safe = f"{file_name_safe}.{mrf_format}"
                        elif '.' not in file_name_safe and mrf_format == 'unknown':
                             file_name_safe = f"{file_name_safe}.dat"


                        output_file_path = os.path.join(OUTPUT_DIR, f"{hospital_name.replace(' ','_')}_{file_name_safe}")
                        
                        decompressed_output_path = output_file_path[:-3] if output_file_path.endswith(".gz") else output_file_path
                        final_file_exists = os.path.exists(decompressed_output_path)
                        if output_file_path.endswith(".gz") and os.path.exists(output_file_path): # Original .gz still exists
                            final_file_exists = True


                        if final_file_exists:
                            print(f"  File {decompressed_output_path} (or its gzipped original) already exists. Skipping.")
                            continue
                        
                        time.sleep(REQUEST_DELAY)
                        download_file(mrf_url, output_file_path)
                else:
                    print(f"  No MRF URLs found in {cms_hpt_txt_url}. You might need to check their website manually for a 'Price Transparency' page.")
            
            elif response.status_code == 404:
                print(f"  cms-hpt.txt not found at {cms_hpt_txt_url} (404 Error).")
                print(f"  Consider manually checking the '{hospital_name}' website for 'Price Transparency' or 'Standard Charges'.")
            else:
                print(f"  Failed to fetch cms-hpt.txt. Status code: {response.status_code}")

        except requests.exceptions.Timeout:
            print(f"  Timeout while trying to access {cms_hpt_txt_url}.")
        except requests.exceptions.ConnectionError:
            print(f"  Connection error while trying to access {cms_hpt_txt_url}. Check the domain or your internet connection.")
        except requests.exceptions.RequestException as e:
            print(f"  Error fetching cms-hpt.txt for {hospital_name}: {e}")
        except Exception as e:
            print(f"  An unexpected error occurred while processing {hospital_name}: {e}")

    print("\n--- Script Finished ---")
    print(f"Downloaded files (if any) are in the '{OUTPUT_DIR}' directory.")
    print("Please note: ")
    print(" - This script prioritizes downloading files identified as CSV format.")
    print(" - Files are downloaded, and .gz archives are automatically decompressed.")
    print(" - Not all hospitals may have a 'cms-hpt.txt' or easily discoverable/parsable MRFs.")
    print(" - For hospitals where files were not found, manual checking of their website is recommended.")
    print(" - **Combining the content of these diverse files into a single, unified table is a complex data transformation task that typically requires custom parsing and mapping logic for each file structure, and is beyond the scope of this download script.**")

if __name__ == "__main__":
    main()
