"""
===============================================================================
Script Name: 04_download_labeled_s3_gpu_logs.py
Description: Connects to MIT Supercloud S3 anonymously, lists all GPU files, 
             matches them against labeled job IDs, selectively downloads 
             only the required files into a single flat directory, and 
             creates a shortcut in the outputs folder.
===============================================================================
"""

import os
import sys
import time
import warnings
import pandas as pd
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from pathlib import Path
from tqdm import tqdm
import concurrent.futures
from datetime import timedelta

# --- 1. CONFIGURATION ---
# Suppress pandas/numpy warnings for cleaner console output
warnings.filterwarnings('ignore')

MAX_THREADS = 10         # Number of simultaneous downloads
GENERATES_FILES = True   # SET TO FALSE if this script ONLY prints to terminal
CREATE_DATA_SYMLINK = True # SET TO TRUE to create a shortcut to the data folder in logs

# Domain-specific constants
BUCKET_NAME = "mit-supercloud-dataset"
PREFIX = "datacenter-challenge/202201/gpu/"


# --- 2. TERMINAL LOGGING UTILITY ---
class DualLogger:
    """Intercepts print statements and routes them to both terminal and a text file."""
    def __init__(self, filepath):
        self.terminal = sys.stdout
        self.log = open(filepath, "w", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()


# --- 3. WORKER FUNCTION ---
def process_single_item(item_args):
    """
    Worker function to process a single file download.
    Must remain at the top level for parallel processing.
    """
    s3_key, actual_output_dir_str = item_args
    try:
        # Initialize client inside the worker to ensure thread safety
        s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        
        # 1. Extract ONLY the filename to flatten the directory structure
        filename = s3_key.split('/')[-1]
        local_path_str = os.path.join(actual_output_dir_str, filename)
        
        # 2. Ensure directory exists (mostly a safety net, main() handles this)
        os.makedirs(os.path.dirname(local_path_str), exist_ok=True)
        
        # 3. Skip or Download
        if os.path.exists(local_path_str):
            return True, filename, None
            
        s3_client.download_file(BUCKET_NAME, s3_key, local_path_str)
        return True, filename, None

    except Exception as e:
        return False, s3_key, str(e)


# --- 4. MAIN EXECUTION ---
def main():
    # --- Path Resolution ---
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    project_root = script_path.parent.parent.parent
    scripts_dir = script_path.parent.parent
    
    # Base dataset directory
    data_dir = project_root / "data" / "mit-supercloud-dataset"
    labels_file = data_dir / "labelled_jobids.parquet"
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
    # --- Dynamic Output Routing ---
    if GENERATES_FILES:
        log_dir = outputs_base_dir / f"{script_name}_output"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Target specific folder requested: data/mit-supercloud-dataset/labelled_jobs/
        actual_output_dir = data_dir / "labelled_jobs"
        actual_output_dir.mkdir(parents=True, exist_ok=True)

        if CREATE_DATA_SYMLINK:
            # Create shortcut in the log directory pointing to our labelled_jobs folder
            shortcut_path = log_dir / f"{script_name}_data_shortcut"
            if not shortcut_path.exists() and not shortcut_path.is_symlink():
                try:
                    # Depending on your OS, creating symlinks might require admin privileges (Windows)
                    shortcut_path.symlink_to(actual_output_dir, target_is_directory=True)
                except OSError as e:
                    print(f"[!] Warning: Could not create shortcut: {e}")
    else:
        log_dir = outputs_base_dir
        log_dir.mkdir(parents=True, exist_ok=True)
        actual_output_dir = None

    # --- Initialize Dual Logging ---
    log_path = log_dir / f"{script_name}_terminal_output.txt"
    sys.stdout = DualLogger(log_path)
    
    # --- START TIMER ---
    start_time = time.perf_counter()
    
    print("=" * 70)
    print(f"--- STARTING: {script_name.upper()} ---")
    print("=" * 70)

    # --- Data Discovery (Loading Labels) ---
    if not labels_file.exists():
        print(f"[ERROR] Labels file not found at:\n{labels_file}")
        sys.stdout = sys.stdout.terminal
        return

    try:
        if labels_file.suffix == '.parquet':
            df = pd.read_parquet(labels_file)
        else:
            df = pd.read_csv(labels_file)
            
        target_job_ids = set(df['id_job'].dropna().astype(int).astype(str))
        print(f"[*] Loaded {len(target_job_ids)} target job IDs from labels file.")
    except Exception as e:
        print(f"[ERROR] Could not load labels file: {e}")
        sys.stdout = sys.stdout.terminal
        return

    # --- S3 Scanning ---
    print("\n[*] Connecting to S3 (Anonymous) to locate files...")
    s3_client_main = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    paginator = s3_client_main.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX)

    download_tasks = []
    print("[*] Scanning S3 bucket... (This may take a few minutes)")
    
    for page in pages:
        if 'Contents' not in page:
            continue
        for obj in page['Contents']:
            s3_key = obj['Key']
            filename = s3_key.split('/')[-1]
            job_id_from_file = filename.split('-')[0] 
            
            if job_id_from_file in target_job_ids:
                download_tasks.append(s3_key)

    total_files = len(download_tasks)
    print(f"[*] Found {total_files} matching files on S3!")

    if total_files == 0:
        print("[!] No files to download. Exiting.")
        sys.stdout = sys.stdout.terminal
        return

    # --- Parallel Processing ---
    print(f"\nStarting parallel downloads on {total_files} files using {MAX_THREADS} threads...\n")
    
    # Prepare arguments for the worker function
    tasks = [(key, str(actual_output_dir)) for key in download_tasks]
    
    overall_success = 0
    overall_fail = 0

    # Swapped to ThreadPoolExecutor for I/O bound network operations
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Downloading", unit="file"):
            success, filename, error_msg = future.result()
            
            if success:
                overall_success += 1
            else:
                overall_fail += 1
                tqdm.write(f"[ERROR] Failed on {filename}: {error_msg}")

    # --- END TIMER & CALCULATE ---
    end_time = time.perf_counter()
    elapsed_seconds = end_time - start_time
    formatted_time = str(timedelta(seconds=int(elapsed_seconds)))

    # --- Final Summary ---
    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total processed : {total_files}")
    print(f"Successful      : {overall_success}")
    print(f"Failed          : {overall_fail}")
    print(f"Total Time      : {formatted_time}")
    
    if GENERATES_FILES:
        print(f"\n[!] Data downloaded to: {actual_output_dir}")
        if CREATE_DATA_SYMLINK:
            print(f"[*] Shortcut to data saved in: {log_dir}")
        print(f"[*] Terminal logs saved to: {log_path}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    # Restore standard output just in case
    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()