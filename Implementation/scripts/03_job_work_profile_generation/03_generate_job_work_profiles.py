"""
===============================================================================
Script Name: 03_generate_job_work_profiles.py
Description: Calculates hardware-independent computational work (Joules) using 
             trapezoidal integration. Rigorously filters for single-node 
             Volta jobs using a two-stage ground-truth and telemetry check.
===============================================================================
"""

import sys
import time
import warnings
import pandas as pd
import numpy as np
import os
from pathlib import Path
from tqdm import tqdm
import concurrent.futures
from datetime import timedelta
from collections import defaultdict

# --- 1. CONFIGURATION ---
warnings.filterwarnings('ignore')

N_CORES = 8             # Default cores for parallel processing
GENERATES_FILES = True  # SET TO FALSE if this script ONLY prints to terminal


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
    """
    file_path, output_path = item_args
    try:
        # 1. Read data
        df = pd.read_parquet(file_path)
        
        # 2. Perform operations: Trapezoidal Integration
        # Calculate time difference between rows (Delta t). 
        # Assume 0.1s for the first reading based on 100ms nvidia-smi polling rate.
        delta_t = df['timestamp'].diff().fillna(0.1)
        
        # Average the power between the current and previous timestep.
        # For the first row, fill NaN with its own power reading.
        avg_pwr_0 = df['power_draw_gpu_0_W'].rolling(window=2).mean().fillna(df['power_draw_gpu_0_W'])
        avg_pwr_1 = df['power_draw_gpu_1_W'].rolling(window=2).mean().fillna(df['power_draw_gpu_1_W'])
        
        # Calculate Work/Energy (Joules)
        df['work_done_gpu_0'] = avg_pwr_0 * delta_t
        df['work_done_gpu_1'] = avg_pwr_1 * delta_t
        
        # Keep only the requested columns
        df_out = df[['timestamp', 'work_done_gpu_0', 'work_done_gpu_1']]
        
        # 3. Save output
        if GENERATES_FILES:
            df_out.to_parquet(output_path, index=False)
            
        return True, file_path.name, None

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
    
    # Path to the filtered ground-truth slurm log
    filtered_slurm_path = project_root / "data" / "mit-supercloud-dataset" / "filtered_volta_single_node_slurm-log.parquet"
    
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

    # --- Load Valid Job IDs (Stage 1 Filter) ---
    if not filtered_slurm_path.exists():
        print(f"[ERROR] Filtered slurm log not found at: {filtered_slurm_path}")
        sys.stdout = sys.stdout.terminal
        return

    print(f"Loading ground-truth single-node Volta job IDs from:\n{filtered_slurm_path}")
    slurm_df = pd.read_parquet(filtered_slurm_path, columns=['id_job'])
    
    # Convert to set for fast O(1) lookups
    valid_job_ids = set(slurm_df['id_job'].astype(str))
    print(f"-> Loaded {len(valid_job_ids)} valid job IDs.\n")

    # --- Data Discovery & Filtering ---
    if not data_dir.exists():
        print(f"[ERROR] Target GPU directory does not exist: {data_dir}")
        sys.stdout = sys.stdout.terminal
        return

    print(f"Scanning {data_dir} for target files in '_cleaned' folders...")
    
    job_to_files = defaultdict(list)
    skipped_not_in_ground_truth = 0
    skipped_multi_node_telemetry = 0
    symlink_errors = 0
    
    # 1. Gather all files and apply Stage 1 filter
    for fp in data_dir.rglob("*.parquet"):
        if fp.parent.name.endswith("_cleaned"):
            job_id = fp.stem.split('-')[0]
            
            # STAGE 1: Check against ground truth
            if job_id not in valid_job_ids:
                skipped_not_in_ground_truth += 1
                continue
                
            job_to_files[job_id].append(fp)

    # 2. Apply Stage 2 filter and build tasks
    tasks = []
    target_files_count = 0
    
    for job_id, file_list in job_to_files.items():
        # STAGE 2: Telemetry check for multi-node bleed-through
        if len(file_list) > 1:
            print(f"[SKIP] Job {job_id} is in ground-truth but generated {len(file_list)} node logs. Rejecting.")
            skipped_multi_node_telemetry += len(file_list)
            continue
            
        # If it passes both filters, prepare it for processing
        fp = file_list[0]
        target_files_count += 1
        
        # Construct target directory
        out_dir_name = fp.parent.name.replace("_cleaned", "_cleaned_job_work_profile")
        out_dir = fp.parent.parent / out_dir_name
        out_dir.mkdir(parents=True, exist_ok=True)
        
        # Construct symlink
        shortcut_path = output_dir / out_dir_name
        if GENERATES_FILES and not shortcut_path.exists():
            try:
                shortcut_path.symlink_to(out_dir.resolve(), target_is_directory=True)
            except OSError:
                symlink_errors += 1
                
        # Construct output file path
        out_name = f"{fp.stem.replace('_cleaned', '')}_job_work_profile{fp.suffix}"
        out_path = out_dir / out_name
        
        tasks.append((fp, out_path))
    
    if target_files_count == 0:
        print("\n[!] No target files matched the strict filters. Exiting.")
        sys.stdout = sys.stdout.terminal
        return
        
    if symlink_errors > 0:
        print(f"\n[WARNING] Failed to create {symlink_errors} shortcuts/symlinks.")
        print("          If on Windows, try running your IDE/Terminal as Administrator.\n")

    # --- Parallel Processing ---
    print(f"\nStarting parallel processing on {target_files_count} verified files using {N_CORES} cores...")
    print(f"(Skipped {skipped_not_in_ground_truth} files not in ground-truth)")
    print(f"(Skipped {skipped_multi_node_telemetry} files due to multi-node telemetry detection)\n")
    
    success_count = 0
    fail_count = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=target_files_count, desc="Processing", unit="file"):
            success, filename, error_msg = future.result()
            
            if success:
                success_count += 1
            else:
                fail_count += 1
                tqdm.write(f"[ERROR] Failed on {filename}: {error_msg}")

    # --- END TIMER & CALCULATE ---
    end_time = time.perf_counter()
    elapsed_seconds = end_time - start_time
    formatted_time = str(timedelta(seconds=int(elapsed_seconds)))

    # --- Final Summary ---
    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total processed : {target_files_count}")
    print(f"Skipped (ID)    : {skipped_not_in_ground_truth}")
    print(f"Skipped (Nodes) : {skipped_multi_node_telemetry}")
    print(f"Successful      : {success_count}")
    print(f"Failed          : {fail_count}")
    print(f"Total Time      : {formatted_time}")
    
    if GENERATES_FILES:
        print(f"\n[!] Data outputs saved to respective '*_cleaned_job_work_profile' directories.")
        print(f"[!] Folder shortcuts and terminal logs saved to: {output_dir}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()