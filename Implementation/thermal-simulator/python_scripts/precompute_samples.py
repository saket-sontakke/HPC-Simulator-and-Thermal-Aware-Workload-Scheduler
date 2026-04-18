"""
===============================================================================
Script Name: precompute_samples.py
Description: Pre-parses MIT Supercloud CSV job traces into React-ready JSON 
             objects. Generates a lightweight quickstart bundle and individual 
             JSON files to bypass heavy client-side browser parsing.
             **Updated: Skips existing JSON files, uses random sampling with 
             a logged seed for reproducibility for the quickstart bundle.**
===============================================================================
"""

import sys
import time
import warnings
import json
import random
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import concurrent.futures
from datetime import timedelta
from collections import defaultdict

# --- 1. CONFIGURATION ---
# Suppress pandas/numpy warnings for cleaner console output
warnings.filterwarnings('ignore')

N_CORES = 8             # Default cores for parallel processing
GENERATES_FILES = True  # SET TO FALSE if this script ONLY prints to terminal
CREATE_DATA_SYMLINK = False # SET TO TRUE for large outputs

# Domain-specific settings for this script
JOBS_PER_CATEGORY_FOR_BUNDLE = 2
GENERATE_INDIVIDUAL_JSONS = True


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
    output_file = file_path.with_suffix('.json')
    
    try:
        # Check if the JSON file already exists to skip CSV parsing
        if GENERATES_FILES and GENERATE_INDIVIDUAL_JSONS and output_file.exists():
            with open(output_file, 'r', encoding='utf-8') as f:
                job_data = json.load(f)
            return True, file_path, job_data

        # 1. Read data using pandas
        df = pd.read_csv(file_path)
        
        # Ensure we only look at rows with the required data
        df = df.dropna(subset=['power_draw_W', 'gpu_index'])

        # 2. Extract traces
        trace0 = df[df['gpu_index'] == 0]['power_draw_W'].tolist()
        trace1 = df[df['gpu_index'] == 1]['power_draw_W'].tolist()

        has_gpu0 = len(trace0) > 0
        has_gpu1 = len(trace1) > 0

        # Build the job object exactly as the simulator expects it
        requested_gpus = 1
        final_trace0 = []
        final_trace1 = []

        if has_gpu0 and has_gpu1:
            requested_gpus = 2
            final_trace0 = trace0
            final_trace1 = trace1
        elif has_gpu0:
            requested_gpus = 1
            final_trace0 = trace0
        elif has_gpu1:
            requested_gpus = 1
            final_trace0 = trace1 # Map to 0 for single GPU logic

        job_data = {
            "id": file_path.stem,
            "requested_gpus": requested_gpus,
            "power_trace_0": final_trace0,
            "power_trace_1": final_trace1,
            "currentIndex": 0, "workDeficit_0": 0, "workDeficit_1": 0,
            "timeArrived": 0, "timeStarted": 0,
            "tempHistory_0": [], "tempHistory_1": [],
            "throttledSteps_0": 0, "throttledSteps_1": 0
        }
        
        # 3. Save individual JSON output directly next to the original CSV in public/samples/
        if GENERATES_FILES and GENERATE_INDIVIDUAL_JSONS:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(job_data, f, separators=(',', ':')) # compact JSON
            
        # Return the job data so the main thread can compile the quickstart bundle
        return True, file_path, job_data

    except Exception as e:
        return False, file_path, str(e)


# --- 4. MAIN EXECUTION ---
def main():
    # --- Path Resolution ---
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    # script is in root/python_scripts/
    project_root = script_path.parent.parent 
    scripts_dir = script_path.parent
    
    # Target the public/samples directory directly
    data_dir = project_root / "public" / "samples"
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
    # Final destination for the bundled payload
    bundle_output_path = project_root / "public" / "quickstart_bundle.json"
    
    # --- Dynamic Output Routing ---
    if GENERATES_FILES:
        log_dir = outputs_base_dir / f"{script_name}_output"
        log_dir.mkdir(parents=True, exist_ok=True)
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
    
    # Find all CSV files in the subdirectories
    target_files = list(data_dir.rglob("*.csv"))
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target CSV files found. Exiting.")
        sys.stdout = sys.stdout.terminal
        return

    # --- Parallel Processing ---
    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    # Prepare arguments for the worker function
    tasks = [(fp, actual_output_dir) for fp in target_files]
    
    overall_success = 0
    overall_fail = 0
    
    # Dictionary to categorize results: { "resnet50": [job1, job2, ...], ... }
    categorized_jobs = defaultdict(list)

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing", unit="file"):
            success, file_path, result_data = future.result()
            
            if success:
                overall_success += 1
                # file_path is like public/samples/resnet50/1234.csv, so parent.name is 'resnet50'
                category_name = file_path.parent.name
                categorized_jobs[category_name].append(result_data)
            else:
                overall_fail += 1
                tqdm.write(f"[ERROR] Failed on {file_path.name}: {result_data}")

    # --- Aggregate the Quickstart Bundle ---
    print(f"\nAggregating quickstart bundle ({JOBS_PER_CATEGORY_FOR_BUNDLE} jobs per category)...")
    
    # Generate and apply a random seed for reproducibility
    bundle_seed = random.randint(100000, 999999)
    random.seed(bundle_seed)
    print(f"[*] Bundle Generation Seed: {bundle_seed}")
    
    quickstart_jobs = []
    
    # Sort categories alphabetically just for consistency
    for category in sorted(categorized_jobs.keys()):
        jobs_in_category = categorized_jobs[category]
        
        # Grab N random jobs (or all of them if there are fewer than N)
        if len(jobs_in_category) <= JOBS_PER_CATEGORY_FOR_BUNDLE:
            selected_jobs = jobs_in_category
        else:
            selected_jobs = random.sample(jobs_in_category, JOBS_PER_CATEGORY_FOR_BUNDLE)
            
        quickstart_jobs.extend(selected_jobs)

    # Write the bundle directly to the public folder
    if GENERATES_FILES:
        with open(bundle_output_path, 'w', encoding='utf-8') as f:
            json.dump(quickstart_jobs, f, separators=(',', ':'))
        print(f"[*] Quickstart bundle created with {len(quickstart_jobs)} jobs.")

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
        print(f"\n[!] Individual JSONs saved to : {data_dir}/<category>/")
        print(f"[!] Quickstart Bundle saved to: {bundle_output_path}")
        print(f"[*] Terminal logs saved to    : {log_path}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    # Restore standard output just in case
    sys.stdout = sys.stdout.terminal

if __name__ == "__main__":
    # If you ever want to force a specific seed, you could accept it via sys.argv here 
    # and pass it to main(), but defaulting to a logged random seed for now.
    main()