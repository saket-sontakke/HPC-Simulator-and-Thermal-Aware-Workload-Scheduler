"""
===============================================================================
Script Name: 03_gpu_temperature_analysis.py
Description: Traverses parquet files to analyze GPU temperatures, aggregating
             the occurrence of each observed temperature and printing a 
             descending ordered summary table and overall statistics.
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
from collections import Counter

# --- 1. CONFIGURATION ---
warnings.filterwarnings('ignore')

N_CORES = 8             # Default cores for parallel processing
GENERATES_FILES = True
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
    """Worker function to process a single file."""
    file_path, output_dir = item_args
    try:
        # Read ONLY the target column to save memory
        df = pd.read_parquet(file_path, columns=['temperature_gpu'])
        
        # Drop NaNs and count occurrences
        df = df.dropna(subset=['temperature_gpu'])
        counts = df['temperature_gpu'].value_counts().to_dict()
        
        return True, file_path.name, counts, None

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
    
    # Path to the specific parquet folder
    data_dir = project_root / "data" / "mit-supercloud-dataset" / "gpu" / "dual_gpu_0000_parquet_to_0019_parquet"
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
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

    print(f"Scanning {data_dir} for parquet files...")
    target_files = list(data_dir.rglob("*.parquet"))
    
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
    global_temp_counts = Counter()

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing", unit="file"):
            success, filename, file_counts, error_msg = future.result()
            
            if success:
                overall_success += 1
                global_temp_counts.update(file_counts)
            else:
                overall_fail += 1
                tqdm.write(f"[ERROR] Failed on {filename}: {error_msg}")

    # --- Generate Temperature Summary Table ---
    print("\n" + "=" * 50)
    print(" GPU TEMPERATURE OBSERVATIONS (LARGEST TO SMALLEST)")
    print("=" * 50)
    print(f"| {'Temperature (°C)':<18} | {'Number of Observations':<25} |")
    print("-" * 50)
    
    sorted_temps = sorted(global_temp_counts.items(), key=lambda item: item[0], reverse=True)
    
    for temp, count in sorted_temps:
        print(f"| {f'{temp:g} °C':<18} | {count:<25,} |")
        
    print("=" * 50)

    # --- CALCULATE & PRINT STATISTICS ---
    if global_temp_counts:
        total_datapoints = sum(global_temp_counts.values())
        max_temp_observed = max(global_temp_counts.keys())
        min_temp_observed = min(global_temp_counts.keys())
        
        # Calculate Mean
        sum_of_temps = sum(temp * count for temp, count in global_temp_counts.items())
        mean_temp = sum_of_temps / total_datapoints
        
        # Calculate Median (approximate using the frequency map)
        cumulative = 0
        median_temp = None
        for temp, count in sorted(global_temp_counts.items()): # Sort smallest to largest
            cumulative += count
            if cumulative >= total_datapoints / 2:
                median_temp = temp
                break

        # Find most and least frequently observed
        most_freq = global_temp_counts.most_common(1)[0]
        least_freq_count = min(global_temp_counts.values())
        least_freq_temps = [temp for temp, count in global_temp_counts.items() if count == least_freq_count]
        
        # Format the least frequent nicely if there are multiple ties
        least_freq_str = f"{least_freq_temps[0]:g} °C" if len(least_freq_temps) == 1 else f"Multiple ({len(least_freq_temps)} temps)"

        print("\n" + "=" * 50)
        print(" OVERALL TEMPERATURE STATISTICS")
        print("=" * 50)
        print(f"Total Datapoints Analyzed : {total_datapoints:,}")
        print("-" * 50)
        print(f"Absolute Max Temperature  : {max_temp_observed:g} °C")
        print(f"Absolute Min Temperature  : {min_temp_observed:g} °C")
        print(f"Average (Mean) Temp       : {mean_temp:.2f} °C")
        print(f"Median Temp               : {median_temp:g} °C")
        print("-" * 50)
        print(f"Most Observed Temp        : {most_freq[0]:g} °C (Occurred {most_freq[1]:,} times)")
        print(f"Least Observed Temp       : {least_freq_str} (Occurred {least_freq_count:,} times)")
        print("=" * 50)

    # --- END TIMER & CALCULATE ---
    end_time = time.perf_counter()
    elapsed_seconds = end_time - start_time
    formatted_time = str(timedelta(seconds=int(elapsed_seconds)))

    # --- Final Summary ---
    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total files processed : {total_files}")
    print(f"Successful            : {overall_success}")
    print(f"Failed                : {overall_fail}")
    print(f"Total Execution Time  : {formatted_time}")
    
    if not GENERATES_FILES:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    # Restore standard output
    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()