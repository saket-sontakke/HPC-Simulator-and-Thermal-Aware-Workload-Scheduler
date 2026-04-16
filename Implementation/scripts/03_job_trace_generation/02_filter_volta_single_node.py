"""
===============================================================================
Script Name: 02_filter_volta_single_node.py
Description: Filters the Slurm log (Parquet format) to retain only GPU Volta 
             jobs (tres_req contains resource ID 1002) on a single node. 
             Saves output in the source directory and creates a shortcut in outputs.
===============================================================================
"""

import sys
import os
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

# Tres ID for GPU Volta per tres-mapping.txt
VOLTA_TRES_ID = "1002"


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


# --- 3. HELPER FUNCTION ---
def is_volta_job(tres_req_str: str) -> bool:
    """Helper to check if the tres_req string contains the Volta GPU ID."""
    if pd.isna(tres_req_str) or str(tres_req_str).strip() == "":
        return False
    entries = str(tres_req_str).split(",")
    for entry in entries:
        parts = entry.strip().split("=")
        if len(parts) == 2 and parts[0].strip() == VOLTA_TRES_ID:
            return True
    return False


# --- 4. WORKER FUNCTION ---
def process_single_item(item_args):
    """
    Worker function to process a single file or task.
    Must remain at the top level for multiprocessing pickling.
    """
    file_path, output_dir, script_name = item_args
    try:
        # 1. Read data
        df = pd.read_parquet(file_path, engine='pyarrow')
        
        # 2. Perform operations (Filters)
        # Filter: single node only (nodes_alloc == 1)
        df_filtered = df[df["nodes_alloc"] == 1].copy()
        
        # Filter: GPU Volta jobs
        df_filtered = df_filtered[df_filtered["tres_req"].apply(is_volta_job)].copy()
        
        # Skip saving if no matching jobs were found
        if df_filtered.empty:
            return True, file_path.name, "No single-node Volta jobs found (skipped saving)"
        
        # 3. Save output
        if GENERATES_FILES:
            # Save the actual file in the SAME directory as the original file
            output_filename = f"filtered_volta_single_node_{file_path.name}"
            actual_output_file = file_path.parent / output_filename
            df_filtered.to_parquet(actual_output_file, index=False)
            
            # Create a shortcut (symlink) in the central outputs folder
            shortcut_file = output_dir / output_filename
            
            # Remove existing shortcut if it exists to avoid FileExistsError
            if shortcut_file.exists() or shortcut_file.is_symlink():
                shortcut_file.unlink()
                
            try:
                shortcut_file.symlink_to(actual_output_file)
            except OSError as e:
                # Catch permission errors for symlinks without failing the whole process
                return True, file_path.name, f"Saved actual file, but symlink failed: {e}"
                
        return True, file_path.name, None

    except Exception as e:
        return False, file_path.name, str(e)


# --- 5. MAIN EXECUTION ---
def main():
    # --- Path Resolution ---
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    project_root = script_path.parent.parent.parent
    scripts_dir = script_path.parent.parent
    
    data_dir = project_root / "data" / "mit-supercloud-dataset"
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
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
    
    # --- START TIMER ---
    start_time = time.perf_counter()
    
    print("=" * 70)
    print(f"--- STARTING: {script_name.upper()} ---")
    print("=" * 70)

    # --- Data Discovery ---
    if not data_dir.exists():
        print(f"[ERROR] Target directory does not exist: {data_dir}")
        sys.stdout = sys.stdout.terminal  # Restore stdout before returning
        return

    print(f"Scanning {data_dir} for target files...")
    
    # Target specific 'slurm-log.parquet' files or just 'slurm-log'
    target_files = list(data_dir.rglob("slurm-log*"))
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        sys.stdout = sys.stdout.terminal  # Restore stdout before returning
        return

    # --- Parallel Processing ---
    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    # Prepare arguments for the worker function
    tasks = [(fp, output_dir, script_name) for fp in target_files]
    
    success_count = 0
    fail_count = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing", unit="file"):
            success, filename, status_msg = future.result()
            
            if success:
                success_count += 1
                if status_msg: # Print helpful skips or symlink warnings
                    tqdm.write(f"[-] {filename}: {status_msg}")
            else:
                fail_count += 1
                tqdm.write(f"[ERROR] Failed on {filename}: {status_msg}")

    # --- END TIMER & CALCULATE ---
    end_time = time.perf_counter()
    elapsed_seconds = end_time - start_time
    # Format to HH:MM:SS (ignoring microseconds for a cleaner summary)
    formatted_time = str(timedelta(seconds=int(elapsed_seconds)))

    # --- Final Summary ---
    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total processed : {total_files}")
    print(f"Successful      : {success_count}")
    print(f"Failed          : {fail_count}")
    print(f"Total Time      : {formatted_time}")
    
    if GENERATES_FILES:
        print(f"\n[!] Actual files saved to: Respective source directories")
        print(f"[!] Shortcuts and logs saved to: {output_dir}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    # Restore standard output just in case
    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()