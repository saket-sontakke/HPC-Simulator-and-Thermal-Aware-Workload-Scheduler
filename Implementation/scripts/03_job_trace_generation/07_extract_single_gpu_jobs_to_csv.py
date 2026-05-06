"""
===============================================================================
Script Name: 07_extract_single_gpu_jobs_to_csv.py
Description: Scans thousands of nvidia-smi parquet logs, filters for jobs that 
             ran on a single GPU, and converts those specific files from 
             .parquet to .csv format in a new sibling directory.
===============================================================================
"""

import sys
import time
import warnings
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm
import concurrent.futures
from datetime import timedelta

# --- 1. CONFIGURATION ---
# Suppress pandas/numpy warnings for cleaner console output
warnings.filterwarnings('ignore')

N_CORES = 8             # Default cores for parallel processing
GENERATES_FILES = True  # SET TO FALSE if this script ONLY prints to terminal
CREATE_DATA_SYMLINK = True # SET TO TRUE for large outputs (saves data to data_dir, puts shortcut in outputs)

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
    file_path, output_dir = item_args
    try:
        # 1. Read ONLY the required column first to evaluate the condition fast
        df_check = pd.read_parquet(file_path, columns=['gpu_index'])
        
        # 2. Check for single GPU constraint
        unique_gpus = df_check['gpu_index'].dropna().unique()
        
        # 3. Save output if it meets the criteria
        if len(unique_gpus) == 1:
            if GENERATES_FILES:
                # Now that it passed the check, read the full dataframe
                df_full = pd.read_parquet(file_path)
                
                # Swap the .parquet extension for .csv
                output_file = output_dir / f"{file_path.stem}.csv"
                
                # Save to CSV
                df_full.to_csv(output_file, index=False)
                
            return True, file_path.name, "Converted to CSV"
        else:
            return True, file_path.name, "Skipped"

    except Exception as e:
        return False, file_path.name, str(e)

# --- 4. MAIN EXECUTION ---
def main():
    # --- Path Resolution ---
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    # Assumes standard project layout: project_root/scripts/preprocessing/script.py
    project_root = script_path.parent.parent.parent
    scripts_dir = script_path.parent.parent
    
    # Explicitly mapping to the requested input directory
    data_dir = project_root / "data" / "mit-supercloud-dataset" / "labelled_jobs_parquet"
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
    # --- Dynamic Output Routing ---
    if GENERATES_FILES:
        log_dir = outputs_base_dir / f"{script_name}_output"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        if CREATE_DATA_SYMLINK:
            # Route heavy data to a sibling directory named for the CSV outputs
            actual_output_dir = data_dir.parent / "labelled_jobs_single_gpu_csv"
            actual_output_dir.mkdir(parents=True, exist_ok=True)
            
            # Create shortcut in the log directory
            shortcut_path = log_dir / "labelled_jobs_single_gpu_csv_shortcut"
            
            # Handle pre-existing symlinks
            if shortcut_path.is_symlink():
                shortcut_path.unlink()
                
            if not shortcut_path.exists():
                try:
                    shortcut_path.symlink_to(actual_output_dir, target_is_directory=True)
                except OSError as e:
                    print(f"[!] Warning: Could not create shortcut: {e} (Note: Windows may require Admin rights for symlinks)")
        else:
            actual_output_dir = log_dir
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

    # --- Data Discovery ---
    if not data_dir.exists():
        print(f"[ERROR] Target directory does not exist: {data_dir}")
        sys.stdout = sys.stdout.terminal
        return

    print(f"Scanning {data_dir} for target files...")
    
    # Find all parquet files in the source directory
    target_files = list(data_dir.rglob("*.parquet"))
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        sys.stdout = sys.stdout.terminal
        return

    # --- Parallel Processing ---
    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    # Prepare arguments for the worker function
    tasks = [(fp, actual_output_dir) for fp in target_files]
    
    overall_converted = 0
    overall_skipped = 0
    overall_fail = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing", unit="file"):
            success, filename, status_msg = future.result()
            
            if success:
                if status_msg == "Converted to CSV":
                    overall_converted += 1
                else:
                    overall_skipped += 1
            else:
                overall_fail += 1
                tqdm.write(f"[ERROR] Failed on {filename}: {status_msg}")

    # --- END TIMER & CALCULATE ---
    end_time = time.perf_counter()
    elapsed_seconds = end_time - start_time
    formatted_time = str(timedelta(seconds=int(elapsed_seconds)))

    # --- Final Summary ---
    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total scanned   : {total_files}")
    print(f"Single GPU Jobs : {overall_converted} (Converted to CSV)")
    print(f"Multi GPU Jobs  : {overall_skipped} (Skipped)")
    print(f"Failed to read  : {overall_fail}")
    print(f"Total Time      : {formatted_time}")
    
    if GENERATES_FILES:
        if CREATE_DATA_SYMLINK:
             print(f"\n[!] CSV outputs saved to: {actual_output_dir}")
             print(f"[*] Shortcut and logs saved to: {log_dir}")
        else:
             print(f"\n[!] Outputs and logs saved to: {log_dir}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    # Restore standard output just in case
    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()