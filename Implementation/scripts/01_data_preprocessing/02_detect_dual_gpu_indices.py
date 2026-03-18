"""
===============================================================================
Script Name: 02_detect_dual_gpu_indices.py
Description: Scans Parquet files to detect consistent dual GPU logs (GPU 0 and 1),
             filters by time overlap/count differences, outputs a summary CSV, 
             and copies valid files to a unified dataset folder.
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
from collections import defaultdict
import shutil
from datetime import timedelta

# --- 1. CONFIGURATION ---
# Suppress pandas/numpy warnings for cleaner console output
warnings.filterwarnings('ignore')

N_CORES = 8             # Default cores for parallel processing
GENERATES_FILES = True  # Set to True as we generate CSVs and copy files

# Domain-specific thresholds/constants
COUNT_DIFF_THRESHOLD = 0.05
TIME_OVERLAP_TOLERANCE = 5.0


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
def process_single_item(file_path):
    """
    Worker function to process a single file.
    Must remain at the top level for multiprocessing pickling.
    """
    try:
        # 1. Read data
        df = pd.read_parquet(file_path, columns=['timestamp', 'gpu_index'])
        
        # 2. Perform operations
        stats = df.groupby('gpu_index')['timestamp'].agg(['count', 'min', 'max'])
       
        if 0 in stats.index and 1 in stats.index:
            count_0, min_0, max_0 = stats.loc[0]
            count_1, min_1, max_1 = stats.loc[1]
           
            max_count = max(count_0, count_1)
            count_diff_pct = abs(count_0 - count_1) / max_count if max_count > 0 else 0
            time_overlap_valid = (abs(min_0 - min_1) <= TIME_OVERLAP_TOLERANCE) and (abs(max_0 - max_1) <= TIME_OVERLAP_TOLERANCE)
           
            if count_diff_pct <= COUNT_DIFF_THRESHOLD and time_overlap_valid:
                folder_name = file_path.parent.name
                status_str = f"{folder_name}/{file_path.name}"
                
                # 3. Return success and extracted metadata
                return True, file_path, folder_name, status_str, None
            else:
                return False, file_path, None, "partial", None
        
        return False, file_path, None, "missing", None

    except Exception as e:
        return False, file_path, None, "error", str(e)


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

    print(f"Scanning {data_dir} for target files in '*_parquet' folders...")
    
    target_files = []
    for p in data_dir.glob("[0-9][0-9][0-9][0-9]_parquet"):
        if p.is_dir():
            target_files.extend(list(p.glob("*.parquet")))
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        sys.stdout = sys.stdout.terminal  # Restore stdout before returning
        return

    # --- Parallel Processing ---
    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    # Counters and Lists
    success_count = 0
    fail_count = 0
    files_with_partial_dual_gpus = 0  
    folder_counts = defaultdict(int)
    multi_gpu_files = []
    valid_file_paths = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        # Submit tasks (just passing the file path)
        futures = {executor.submit(process_single_item, fp): fp for fp in target_files}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Analyzing GPU indices", unit="file"):
            is_valid, file_path, folder_name, status, error_msg = future.result()
            
            if is_valid:
                success_count += 1
                folder_counts[folder_name] += 1
                multi_gpu_files.append(status) 
                valid_file_paths.append(file_path)
            else:
                if status == "partial":
                    files_with_partial_dual_gpus += 1
                elif status == "error":
                    fail_count += 1
                    tqdm.write(f"[ERROR] Failed on {file_path.name}: {error_msg}")
                # Status "missing" is ignored as it just means the file isn't dual-gpu

    # --- Final Results Formatting & Export ---
    print("\n" + "="*70)
    print("--- CONSISTENT DUAL-GPU FILE ANALYSIS COMPLETE ---")
    print("="*70)
    print(f"{'Total Parquet files scanned':<50} : {total_files}")
    print(f"{'Files with CONSISTENT GPU 0 and 1 logs':<50} : {success_count}")
    print(f"{'Files with PARTIAL/INCONSISTENT dual GPU logs':<50} : {files_with_partial_dual_gpus}")
    print("-" * 70)
   
    if folder_counts:
        print("Breakdown per subfolder (Consistent Files Only):")
        for folder in sorted(folder_counts.keys()):
            print(f"  {folder:<48} : {folder_counts[folder]}")
        print("-" * 70)

    if multi_gpu_files:
        print("Sample files containing consistent dual GPUs (up to 5):")
        for sample in multi_gpu_files[:5]:
            print(f"  - {sample}")
           
        # 1. Output CSV list directly to outputs/
        csv_out_path = output_dir / f"{script_name}_file_list.csv"
        pd.DataFrame({'file_path': multi_gpu_files}).to_csv(csv_out_path, index=False)
        print(f"\n[!] A full list of the {success_count} files was saved to: {csv_out_path}")

        # 2. Create the dual_gpu folder directly in the gpu data directory
        sorted_folders = sorted(folder_counts.keys())
        copy_dir_name = f"dual_gpu_{sorted_folders[0]}_to_{sorted_folders[-1]}"
        copy_dir_path = data_dir / copy_dir_name
        copy_dir_path.mkdir(parents=True, exist_ok=True)
       
        # 3. Copy files (kept sequential to prevent disk thrashing)
        print(f"\nCopying {len(valid_file_paths)} files to {copy_dir_path}...")
        for src_file in tqdm(valid_file_paths, desc="Copying Parquet files", unit="file"):
            shutil.copy2(src_file, copy_dir_path / src_file.name)
           
        print("[!] File copying complete.")

    # --- END TIMER & CALCULATE ---
    end_time = time.perf_counter()
    elapsed_seconds = end_time - start_time
    formatted_time = str(timedelta(seconds=int(elapsed_seconds)))

    # --- Final Summary ---
    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total processed : {total_files}")
    print(f"Successful      : {success_count}")
    print(f"Errors/Failed   : {fail_count}")
    print(f"Total Time      : {formatted_time}")
    
    if GENERATES_FILES:
        print(f"\n[!] Outputs and terminal logs saved to: {output_dir}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    # Restore standard output just in case
    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()