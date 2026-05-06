"""
===============================================================================
Script Name: 06_gpu_folder_stats.py
Description: Scans 26 subfolders to count files with single GPU indices 
             (0 or 1) vs dual GPU indices (0 and 1) per subfolder.
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
from collections import defaultdict

# --- 1. CONFIGURATION ---
# Suppress pandas/numpy warnings for cleaner console output
warnings.filterwarnings('ignore')

N_CORES = 8             # Default cores for parallel processing
GENERATES_FILES = False  # Set to False as we only need the log/terminal report
CREATE_DATA_SYMLINK = False 

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
    Worker function to process a single file.
    Returns success status, subfolder name, category, and error message.
    """
    file_path, _ = item_args
    subfolder_name = file_path.parent.name
    try:
        # Load only the GPU index column for speed
        df = pd.read_csv(file_path, usecols=['gpu_index'])
        unique_indices = set(df['gpu_index'].unique())

        if unique_indices == {0, 1}:
            category = "dual"
        elif unique_indices == {0}:
            category = "single_0"
        elif unique_indices == {1}:
            category = "single_1"
        else:
            category = "other"

        return True, subfolder_name, category, None

    except Exception as e:
        return False, subfolder_name, file_path.name, str(e)


# --- 4. MAIN EXECUTION ---
def main():
    # --- Path Resolution ---
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    # Implementation/scripts/03_job_trace_generation
    scripts_dir = script_path.parent.parent 
    project_root = scripts_dir.parent
    
    # Specific data path provided by user
    data_dir = project_root / "data" / "mit-supercloud-dataset" / "labelled_jobs_single_node_csv_categorized"
    
    # Following boilerplate: outputs/parent_dir_of_script
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
    # --- Dynamic Output Routing ---
    if GENERATES_FILES:
        log_dir = outputs_base_dir / f"{script_name}_output"
        log_dir.mkdir(parents=True, exist_ok=True)
        actual_output_dir = log_dir # Simplification for this specific task
    else:
        log_dir = outputs_base_dir / f"{script_name}_output"
        log_dir.mkdir(parents=True, exist_ok=True)
        actual_output_dir = None

    # --- Initialize Dual Logging ---
    log_path = log_dir / f"{script_name}_terminal_output.txt"
    sys.stdout = DualLogger(log_path)
    
    # --- START TIMER ---
    start_time = time.perf_counter()
    
    print("=" * 80)
    print(f"--- STARTING: {script_name.upper()} ---")
    print("=" * 80)

    # --- Data Discovery ---
    if not data_dir.exists():
        print(f"[ERROR] Target directory does not exist: {data_dir}")
        sys.stdout = sys.stdout.terminal
        return

    print(f"Scanning {data_dir}...")
    target_files = list(data_dir.rglob("*.csv"))
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        sys.stdout = sys.stdout.terminal
        return

    # --- Parallel Processing ---
    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    tasks = [(fp, actual_output_dir) for fp in target_files]
    
    overall_success = 0
    overall_fail = 0
    
    # stats[subfolder][category] = count
    stats = defaultdict(lambda: {"single_0": 0, "single_1": 0, "dual": 0, "other": 0})

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing", unit="file"):
            success, subfolder, result_val, error_msg = future.result()
            
            if success:
                overall_success += 1
                stats[subfolder][result_val] += 1
            else:
                overall_fail += 1
                tqdm.write(f"[ERROR] Failed on {subfolder}/{result_val}: {error_msg}")

    # --- Subfolder breakdown ---
    print("\n" + "-" * 80)
    print(f"{'SUBFOLDER':<45} | {'S-0':<8} | {'S-1':<8} | {'DUAL':<8}")
    print("-" * 80)
    
    grand_s0, grand_s1, grand_dual = 0, 0, 0
    
    for folder in sorted(stats.keys()):
        s0 = stats[folder]['single_0']
        s1 = stats[folder]['single_1']
        dual = stats[folder]['dual']
        
        grand_s0 += s0
        grand_s1 += s1
        grand_dual += dual
        
        print(f"{folder:<45} | {s0:<8} | {s1:<8} | {dual:<8}")

    # --- END TIMER & CALCULATE ---
    end_time = time.perf_counter()
    elapsed_seconds = end_time - start_time
    formatted_time = str(timedelta(seconds=int(elapsed_seconds)))

    # --- Final Summary ---
    print("\n" + "=" * 80)
    print("=== FINAL OVERALL SUMMARY ===")
    print("=" * 80)
    print(f"Total Folders   : {len(stats)}")
    print(f"Total Single (0): {grand_s0}")
    print(f"Total Single (1): {grand_s1}")
    print(f"Total Dual (0&1): {grand_dual}")
    print(f"Total Success   : {overall_success}")
    print(f"Total Failed    : {overall_fail}")
    print(f"Total Time      : {formatted_time}")
    print("=" * 80)
    
    print(f"\n[!] Terminal logs saved to: {log_path}")

    # Restore standard output
    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()