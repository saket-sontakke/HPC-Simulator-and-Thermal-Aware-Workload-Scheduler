"""
===============================================================================
Script Name: extract_single_node_volta_logs.py
Description: Filters the master Slurm log parquet file to extract jobs that 
             specifically utilized a single node and a Volta GPU (Tres ID 1002),
             exporting the result as a CSV.
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

N_CORES = 1             # Set to 1 since we are processing a single large file
GENERATES_FILES = True  # Set to True as we generate a filtered CSV

# Domain-specific thresholds/constants
TRES_VOLTA_REGEX = r'\b1002='


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
        print(f"Loading {file_path} into memory...")
        df = pd.read_parquet(file_path)
        
        initial_count = len(df)
        print(f"Initial number of jobs: {initial_count}")

        # 2. Perform operations
        # Filter for Volta GPU (tres_alloc contains 1002=)
        df_volta = df[df['tres_alloc'].astype(str).str.contains(TRES_VOLTA_REGEX, na=False)]
        volta_count = len(df_volta)
        print(f"Jobs using Volta GPU (1002): {volta_count}")

        # Filter for Single Node (negate condition finding a comma)
        final_df = df_volta[~df_volta['nodelist'].astype(str).str.contains(',', na=False)]
        final_count = len(final_df)
        print(f"Jobs on a single node using Volta: {final_count}")

        # 3. Save output
        if GENERATES_FILES:
            output_file = output_dir / f"{script_name}.csv"
            print(f"\nSaving filtered data to {output_file}...")
            final_df.to_csv(output_file, index=False)
            
        return True, file_path.name, None

    except Exception as e:
        return False, file_path.name, str(e)


# --- 4. MAIN EXECUTION ---
def main():
    # --- Path Resolution ---
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    base_path = script_path.parent.parent
    data_dir = base_path / "data" / "mit-supercloud-dataset"
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

    slurm_log_path = data_dir / "slurm-log.parquet"
    if not slurm_log_path.exists():
        print(f"[ERROR] Could not find the master slurm log at {slurm_log_path}")
        return
        
    target_files = [slurm_log_path]
    total_files = len(target_files)

    # --- Parallel Processing ---
    print(f"Starting single-file execution using {N_CORES} core...\n")
    
    # Prepare arguments for the worker function
    tasks = [(fp, output_dir) for fp in target_files]
    
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