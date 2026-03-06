"""
===============================================================================
Script Name: detect_throttling.py
Description: Scans Parquet files for GPU throttling events based on temperature,
             power, and clock speed limits.
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
GENERATES_FILES = False # Set to FALSE as this script ONLY prints to terminal and logs

# Domain-specific thresholds/constants
# NOTE: Thresholds sourced from: https://forums.developer.nvidia.com/t/tesla-v100-sw-thermal-slowdown-active/160924
TEMP_LIMIT = 83.0        # (Max Operating Temp) Temperature >= 83°C  
POWER_LIMIT = 100.0      # (Minimum working load) Power >= 100W
CLOCK_DROP_LIMIT = 1230  # (Standard Application Clock) Clock Speed <= 1230MHz


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
    Must remain at the top level for multiprocessing pickling.
    """
    file_path, output_dir = item_args
    try:
        # 1. Read data
        df = pd.read_parquet(file_path)
        
        # 2. Perform operations
        # Check for necessary columns
        if 'clocks_current_sm_MHz' not in df.columns:
            return True, file_path.name, 'missing_clock', None
        
        if 'temperature_gpu' not in df.columns or 'power_draw_W' not in df.columns:
            return True, file_path.name, 'has_clock', None
            
        # Filter for throttling events
        throttled = df[
            (df['temperature_gpu'] >= TEMP_LIMIT) & 
            (df['power_draw_W'] >= POWER_LIMIT) &
            (df['clocks_current_sm_MHz'] <= CLOCK_DROP_LIMIT)
        ]
        
        if not throttled.empty:
            event_data = {
                'file_name': file_path.name,
                'folder_name': file_path.parent.name,
                'throttle_duration_samples': len(throttled),
                'max_temp_recorded': throttled['temperature_gpu'].max(),
                'min_clock_recorded': throttled['clocks_current_sm_MHz'].min(),
                'max_power_recorded': throttled['power_draw_W'].max()
            }
            return True, file_path.name, 'has_clock', event_data
            
        return True, file_path.name, 'has_clock', None

    except Exception as e:
        return False, file_path.name, "error", str(e)


# --- 4. MAIN EXECUTION ---
def main():
    # --- Path Resolution ---
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    base_path = script_path.parent.parent
    data_dir = base_path / "data" / "mit-supercloud-dataset" / "gpu"
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
        print("Please ensure your data folder structure matches: data/mit-supercloud-dataset/gpu")
        return

    print(f"Scanning {data_dir} for '####_parquet' folders...")
    
    target_files = []
    for p in data_dir.glob("[0-9][0-9][0-9][0-9]_parquet"):
        if p.is_dir():
            target_files.extend(list(p.glob("*.parquet")))
    
    total_files = len(target_files)
    if total_files == 0:
        print(f"[!] No target files found in any ####_parquet/ subfolders inside {data_dir}. Exiting.")
        return

    # --- Parallel Processing ---
    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    # Prepare arguments for the worker function
    tasks = [(fp, output_dir) for fp in target_files]
    
    success_count = 0
    fail_count = 0
    files_missing_clock = 0
    files_with_clock = 0
    throttling_events = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing", unit="file"):
            success, filename, status, data_or_error = future.result()
            
            if success:
                success_count += 1
                if status == 'missing_clock':
                    files_missing_clock += 1
                elif status == 'has_clock':
                    files_with_clock += 1
                    if data_or_error is not None:
                        throttling_events.append(data_or_error)
            else:
                fail_count += 1
                tqdm.write(f"[ERROR] Failed on {filename}: {data_or_error}")

    # --- Throttling Analysis Results ---
    print("\n" + "="*70)
    print("--- THROTTLING ANALYSIS COMPLETE ---")
    print("="*70)
    
    print(f"{'Total Parquet files scanned':<45} : {total_files}")
    print(f"{'Files containing clocks_current_sm_MHz data':<45} : {files_with_clock}")
    print(f"{'Files missing clocks_current_sm_MHz data':<45} : {files_missing_clock}")
    print(f"{'Files showing signs of throttling':<45} : {len(throttling_events)}")

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