"""
===============================================================================
Script Name: 05_process_labelled_jobs.py
Description: Scans the MIT Supercloud GPU telemetry dataset, matches Parquet 
             files to known labeled deep learning jobs, appends label/jobID 
             columns, and exports them as compressed Parquet files grouped by model.
===============================================================================
"""

import sys
import time
import warnings
import pandas as pd
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
    Reads the Parquet file, appends model labels, and saves as Parquet.
    Must remain at the top level for multiprocessing pickling.
    """
    file_path, output_dir, job_id, model_name = item_args
    try:
        # 1. Read data
        df = pd.read_parquet(file_path)
        
        # Skip empty files
        if df.empty:
             return False, file_path.name, "File is empty"
        
        # 2. Perform operations - tag with label and job id
        df['id_job'] = job_id
        df['model'] = model_name
        
        # 3. Save output
        if GENERATES_FILES:
            # Create a specific subfolder for this model (e.g., outputs/resnet50/)
            model_dir = output_dir / model_name
            model_dir.mkdir(parents=True, exist_ok=True)
            
            output_file = model_dir / f"{file_path.stem}_gpu_trace.parquet"
            df.to_parquet(output_file, index=False)
            
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
    
    # Base Dataset Directory
    dataset_base_dir = project_root / "data" / "mit-supercloud-dataset"
    
    # Target Input Directories
    data_dir = dataset_base_dir / "labelled_jobs_parquet"
    labels_file = dataset_base_dir / "labelled_jobids.parquet"
    
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
    # --- Dynamic Output Routing ---
    if GENERATES_FILES:
        log_dir = outputs_base_dir / f"{script_name}_output"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        if CREATE_DATA_SYMLINK:
            # Route heavy data to the new processed directory requested
            actual_output_dir = dataset_base_dir / "labelled_jobs_parquet_processed"
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

    # --- Data Discovery & Label Mapping ---
    if not data_dir.exists():
        print(f"[ERROR] Target directory does not exist: {data_dir}")
        sys.stdout = sys.stdout.terminal
        return
        
    if not labels_file.exists():
        print(f"[ERROR] Labels file does not exist: {labels_file}")
        sys.stdout = sys.stdout.terminal
        return

    print("Loading job labels...")
    labels_df = pd.read_parquet(labels_file)
    
    # Create a fast lookup dictionary: string(job_id) -> model_name
    job_dict = {str(row['id_job']): row['model'] for _, row in labels_df.iterrows()}
    print(f"Found {len(job_dict)} labeled jobs in mapping file.")

    print(f"Scanning {data_dir} for matching Parquet files...")
    tasks = []
    scanned_folders = set()
    
    for fp in data_dir.rglob("*.parquet"):
        filename_str = fp.stem
        scanned_folders.add(fp.parent.name)
        
        # Extract just the pure Job ID before the first hyphen
        job_id_from_file = filename_str.split('-')[0]
        
        # Check if this exact ID is in our label dictionary
        if job_id_from_file in job_dict:
            model_name = job_dict[job_id_from_file]
            tasks.append((fp, actual_output_dir, job_id_from_file, model_name))

    total_files = len(tasks)
    if total_files == 0:
        print("[!] No matching GPU telemetry Parquet files found for the labeled jobs. Exiting.")
        sys.stdout = sys.stdout.terminal
        return

    # --- Print Folder Scan Statistics ---
    sorted_folders = sorted(list(scanned_folders))
    if sorted_folders:
        print(f"Found files in {len(sorted_folders)} unique matching folder(s).")
        if len(sorted_folders) >= 2:
            print(f"  First folder: {sorted_folders[0]}")
            print(f"  Last folder:  {sorted_folders[-1]}")
        else:
            print(f"  Folder: {sorted_folders[0]}")

    # --- Parallel Processing ---
    print(f"\nStarting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    overall_success = 0
    overall_fail = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing", unit="file"):
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