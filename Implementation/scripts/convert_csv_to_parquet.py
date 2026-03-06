"""
===============================================================================
Script Name: convert_csv_to_parquet.py
Description: Recursively converts CSV files to Parquet using PyArrow for strict 
             data typing, utilizing multicore processing.
===============================================================================
"""

import sys
import warnings
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm
import concurrent.futures

# --- 1. CONFIGURATION ---
# Suppress pandas/numpy warnings for cleaner console output
warnings.filterwarnings('ignore')

N_CORES = 8             # Default cores for parallel processing
GENERATES_FILES = True  # Set to True as we generate Parquet files


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
    Worker function to process a single file or task.
    Must remain at the top level for multiprocessing pickling.
    """
    csv_path, dest_file = item_args
    try:
        # Ensure the destination directory exists
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Read CSV using PyArrow for strict data typing and integrity
        df = pd.read_csv(
            csv_path, 
            engine="pyarrow", 
            dtype_backend="pyarrow"
        )
        
        # Save output
        if GENERATES_FILES:
            df.to_parquet(dest_file, engine="pyarrow", index=False)
            
        return True, csv_path.name, None

    except Exception as e:
        return False, csv_path.name, str(e)


# --- 4. MAIN EXECUTION ---
def main():
    # --- Path Resolution ---
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    base_path = script_path.parent.parent
    data_dir = base_path / "data"
    outputs_base_dir = script_path.parent / "outputs"
    
    # --- Dynamic Output Routing ---
    if GENERATES_FILES:
        output_dir = outputs_base_dir / f"{script_name}_output"
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = outputs_base_dir
        output_dir.mkdir(parents=True, exist_ok=True)

    # --- Initialize Dual Logging ---
    log_path = output_dir / f"{script_name}_terminal_output.txt"
    sys.stdout = DualLogger(log_path)
    
    print("=" * 70)
    print(f"--- STARTING: {script_name.upper()} ---")
    print("=" * 70)

    # --- Data Discovery ---
    if not data_dir.exists():
        print(f"[ERROR] Target directory does not exist: {data_dir}")
        return

    print(f"Scanning for CSV files in: {data_dir}...")
    
    all_csv_files = list(data_dir.rglob('*.csv'))
    
    if len(all_csv_files) == 0:
        print("[!] No target files found. Exiting.")
        return

    tasks = []
    already_converted_count = 0

    # Categorize and Check Existing Conversions
    for csv_path in all_csv_files:
        # Handle specific logic for 'gpu' subdirectories
        if csv_path.parent.parent.name == 'gpu':
            dest_dir_name = f"{csv_path.parent.name}_parquet"
            dest_dir = csv_path.parent.parent / dest_dir_name
        else:
            dest_dir = csv_path.parent
            
        dest_file = dest_dir / f"{csv_path.stem}.parquet"
        
        # Smart Skipping
        if dest_file.exists():
            already_converted_count += 1
        else:
            tasks.append((csv_path, dest_file))

    total_files = len(tasks)

    # Summary of the scan
    print("-" * 50)
    print(f"Total CSVs found: {len(all_csv_files)}")
    print(f"Already converted: {already_converted_count}")
    print(f"Pending conversions: {total_files}")
    print("-" * 50)

    if total_files == 0:
        print("[!] All files are up to date. Exiting.")
        return

    # --- Parallel Processing ---
    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    success_count = 0
    fail_count = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing", unit="file"):
            success, filename, error_msg = future.result()
            
            if success:
                success_count += 1
            else:
                fail_count += 1
                tqdm.write(f"[ERROR] Failed on {filename}: {error_msg}")

    # --- Final Summary ---
    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total processed : {total_files}")
    print(f"Successful      : {success_count}")
    print(f"Failed          : {fail_count}")
    
    if GENERATES_FILES:
        print(f"\n[!] Outputs and terminal logs saved to: {output_dir}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    # Restore standard output just in case
    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()