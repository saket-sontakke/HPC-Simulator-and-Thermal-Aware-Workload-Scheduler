"""
===============================================================================
Script Name: 05_categorize_labelled_jobs.py
Description: Categorizes mixed single-node CSV files into subfolders 
             based on their AI model labels extracted from a parquet file.
             Filters CSVs to only retain ['timestamp', 'gpu_index', 'power_draw_W'].
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
warnings.filterwarnings('ignore')

N_CORES = 8             # Default cores for parallel processing
GENERATES_FILES = True  # SET TO FALSE if this script ONLY prints to terminal
CREATE_DATA_SYMLINK = True # SET TO TRUE for large outputs (saves data to data_dir, puts shortcut in outputs)

# Columns to retain to save space
TARGET_COLUMNS = ['timestamp', 'gpu_index', 'power_draw_W']

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
    Filters the CSV for required columns and saves it into a model-specific subfolder.
    """
    file_path, output_dir, job_to_model_map = item_args
    try:
        # Extract job_id from filename (e.g., "25417044304-r1485405-n976057.csv" -> "25417044304")
        job_id = file_path.name.split('-')[0]
        
        # Look up the model label, default to 'unknown' if not found
        model_label = job_to_model_map.get(job_id, "unknown")
        
        if GENERATES_FILES:
            # Create the specific subfolder for this model if it doesn't exist
            model_subfolder = output_dir / model_label
            model_subfolder.mkdir(parents=True, exist_ok=True)
            
            # Destination path
            dest_file = model_subfolder / file_path.name
            
            # Read only the target columns (lambda prevents crashes if a file is missing a column)
            df = pd.read_csv(file_path, usecols=lambda c: c in TARGET_COLUMNS)
            
            # Save the trimmed dataframe
            df.to_csv(dest_file, index=False)
            
        # Return the model_label so the main thread can tally it
        return True, file_path.name, model_label, None

    except Exception as e:
        return False, file_path.name, None, str(e)


# --- 4. MAIN EXECUTION ---
def main():
    # --- Path Resolution ---
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    project_root = script_path.parent.parent.parent
    scripts_dir = script_path.parent.parent
    
    # Base dataset directory
    data_dir = project_root / "data" / "mit-supercloud-dataset"
    
    # Specific input directories
    input_csv_dir = data_dir / "labelled_jobs_single_node_csv"
    labels_parquet_path = data_dir / "labelled_jobids.parquet"
    
    # Outputs routing
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
    # --- Dynamic Output Routing ---
    if GENERATES_FILES:
        log_dir = outputs_base_dir / f"{script_name}_output"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        if CREATE_DATA_SYMLINK:
            actual_output_dir = data_dir / "labelled_jobs_single_node_csv_categorized"
            actual_output_dir.mkdir(parents=True, exist_ok=True)
            
            # Create shortcut in the log directory
            shortcut_path = log_dir / "labelled_jobs_single_node_csv_categorized_shortcut"
            if not shortcut_path.exists() and not shortcut_path.is_symlink():
                try:
                    shortcut_path.symlink_to(actual_output_dir, target_is_directory=True)
                except OSError as e:
                    print(f"[!] Warning: Could not create shortcut: {e}")
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

    # --- Data Discovery & Label Preparation ---
    if not input_csv_dir.exists():
        print(f"[ERROR] Target directory does not exist: {input_csv_dir}")
        sys.stdout = sys.stdout.terminal
        return
        
    if not labels_parquet_path.exists():
        print(f"[ERROR] Labels parquet file not found: {labels_parquet_path}")
        sys.stdout = sys.stdout.terminal
        return

    print("Loading job labels from parquet file...")
    try:
        df_labels = pd.read_parquet(labels_parquet_path)
        job_to_model_map = dict(zip(df_labels['id_job'].astype(str), df_labels['model']))
        print(f"Loaded {len(job_to_model_map)} unique job labels.")
    except Exception as e:
        print(f"[ERROR] Failed to load parquet file: {e}")
        sys.stdout = sys.stdout.terminal
        return

    print(f"\nScanning {input_csv_dir} for target CSV files...")
    target_files = list(input_csv_dir.glob("*.csv"))
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        sys.stdout = sys.stdout.terminal
        return

    # --- Parallel Processing ---
    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    tasks = [(fp, actual_output_dir, job_to_model_map) for fp in target_files]
    
    overall_success = 0
    overall_fail = 0
    category_counts = {}  # Dictionary to track file counts per category

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing", unit="file"):
            success, filename, model_label, error_msg = future.result()
            
            if success:
                overall_success += 1
                # Tally the successful category assignment
                if model_label:
                    category_counts[model_label] = category_counts.get(model_label, 0) + 1
            else:
                overall_fail += 1
                tqdm.write(f"[ERROR] Failed on {filename}: {error_msg}")

    # --- END TIMER & CALCULATE ---
    end_time = time.perf_counter()
    elapsed_seconds = end_time - start_time
    formatted_time = str(timedelta(seconds=int(elapsed_seconds)))

    # --- Final Summary Tables ---
    print("\n" + "=" * 50)
    print("=== CATEGORY DISTRIBUTION ===")
    print("=" * 50)
    print(f"{'Model / Category':<30} | {'File Count':>15}")
    print("-" * 50)
    
    # Sort categories by count (descending)
    for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"{category:<30} | {count:>15}")
    
    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total processed : {total_files}")
    print(f"Successful      : {overall_success}")
    print(f"Failed          : {overall_fail}")
    print(f"Total Time      : {formatted_time}")
    
    if GENERATES_FILES:
        if CREATE_DATA_SYMLINK:
             print(f"\n[!] Categorized CSVs saved to: {actual_output_dir}")
             print(f"[*] Shortcut and logs saved to: {log_dir}")
        else:
             print(f"\n[!] Outputs and logs saved to: {log_dir}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    # Restore standard output
    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()