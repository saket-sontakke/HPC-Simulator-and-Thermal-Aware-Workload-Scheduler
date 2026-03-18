"""
===============================================================================
Script Name: 06_extract_normalization_metadata.py
Description: Scans local cleaned Parquet files to extract mean, std, and t_max
             for physics-informed de-normalization during PINN training.
             Outputs a single mapping CSV file.
===============================================================================
"""

import sys
import time
import warnings
import pandas as pd
import numpy as np
import re
from pathlib import Path
from tqdm import tqdm
import concurrent.futures
from datetime import timedelta

# --- 1. CONFIGURATION ---
# Suppress pandas/numpy warnings for cleaner console output
warnings.filterwarnings('ignore')

N_CORES = 8             # Default cores for parallel processing
GENERATES_FILES = True  # SET TO FALSE if this script ONLY prints to terminal
CREATE_DATA_SYMLINK = False # SET TO TRUE for large outputs (saves data to data_dir, puts shortcut in outputs)

# Columns to extract stats from
TEMP_COLS = [
    "temperature_gpu_0", "temperature_gpu_1",
    "temperature_memory_0", "temperature_memory_1"
]

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
    Extracts scaling metadata and returns it as a dictionary.
    """
    file_path, output_dir = item_args
    try:
        # 1. Read data (only the columns we need to save RAM/Time)
        df = pd.read_parquet(file_path, columns=["timestamp"] + TEMP_COLS)
        
        # 2. Perform operations & validations
        if len(df) < 2:
            return False, file_path.name, "File has fewer than 2 rows"
            
        tmax = df["timestamp"].max()
        if tmax <= 0:
            return False, file_path.name, "Invalid timestamp max"

        # Build the metadata row
        row_data = {
            "filename": file_path.name,
            "t_max": tmax
        }
        
        # Calculate mean and std for each temperature column
        for col in TEMP_COLS:
            row_data[f"{col}_mean"] = df[col].mean()
            row_data[f"{col}_std"] = df[col].std()
            
        # 3. Return payload back to main thread for aggregation
        return True, file_path.name, row_data

    except Exception as e:
        return False, file_path.name, str(e)


# --- 4. MAIN EXECUTION ---
def main():
    # --- Path Resolution ---
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    project_root = script_path.parent.parent.parent
    scripts_dir = script_path.parent.parent
    
    data_dir = project_root / "data" / "mit-supercloud-dataset" / "gpu"
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
    # --- Dynamic Output Routing ---
    if GENERATES_FILES:
        log_dir = outputs_base_dir / f"{script_name}_output"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        if CREATE_DATA_SYMLINK:
            # Route heavy data to the primary data directory
            actual_output_dir = data_dir / f"{script_name}_processed"
            actual_output_dir.mkdir(parents=True, exist_ok=True)
            
            # Create shortcut in the log directory
            shortcut_path = log_dir / f"{script_name}_data_shortcut"
            if not shortcut_path.exists() and not shortcut_path.is_symlink():
                try:
                    shortcut_path.symlink_to(actual_output_dir, target_is_directory=True)
                except OSError as e:
                    print(f"[!] Warning: Could not create shortcut: {e}")
        else:
            # Route data alongside logs in the standard outputs folder
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

    print(f"Scanning {data_dir} for target directories...")
    
    # Find all cleaned directories
    pattern = re.compile(r"dual_gpu_\d{4}_parquet_to_\d{4}_parquet_cleaned$")
    cleaned_dirs = [d for d in data_dir.iterdir() if d.is_dir() and pattern.match(d.name)]
    
    target_files = []
    for d in cleaned_dirs:
        target_files.extend(list(d.glob("*.parquet")))
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        sys.stdout = sys.stdout.terminal
        return

    # --- Parallel Processing ---
    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    # Prepare arguments for the worker function
    tasks = [(fp, actual_output_dir) for fp in target_files]
    
    overall_success = 0
    overall_fail = 0
    all_metadata = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Extracting", unit="file"):
            success, filename, payload = future.result()
            
            if success:
                overall_success += 1
                all_metadata.append(payload)  # payload is the row_data dictionary
            else:
                overall_fail += 1
                tqdm.write(f"[ERROR] Failed on {filename}: {payload}") # payload is the error_msg here

    # --- Aggregation and Saving ---
    if all_metadata and GENERATES_FILES:
        final_csv_path = actual_output_dir / "normalization_metadata.csv"
        print(f"\n[*] Aggregating metadata into a single dataframe...")
        df_map = pd.DataFrame(all_metadata)
        df_map.to_csv(final_csv_path, index=False)
        print(f"[+] Successfully generated: {final_csv_path.name}")

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
        if CREATE_DATA_SYMLINK:
             print(f"\n[!] Heavy outputs saved to: {actual_output_dir}")
             print(f"[*] Shortcut and logs saved to: {log_dir}")
        else:
             print(f"\n[!] Outputs and logs saved to: {log_dir}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    # Restore standard output just in case
    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()