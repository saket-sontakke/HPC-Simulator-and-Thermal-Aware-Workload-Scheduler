"""
===============================================================================
Script Name: analyze_power_draw_global.py
Description: Lightweight script to find the global maximum and distribution
             of power_draw_W across all specific dual_gpu parquet files.
===============================================================================
"""

import sys
import warnings
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm
import concurrent.futures
import re
from collections import Counter
import matplotlib
# Use Agg backend to prevent GUI thread issues
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# --- 1. CONFIGURATION ---
# Suppress pandas/numpy warnings for cleaner console output
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
    Must remain at the top level for multiprocessing pickling.
    """
    file_path, output_dir = item_args
    try:
        # 1. Read ONLY the target data to save memory and time
        df = pd.read_parquet(file_path, columns=['power_draw_W'])
        s = df['power_draw_W'].dropna()
        
        if s.empty:
            return True, file_path.name, (Counter(), None)
            
        # 2. Perform operations: Round to nearest Watt, count instances, find max
        counts = Counter(s.round().astype(int).tolist())
        max_val = s.max()
        
        # 3. Return the aggregated data for this file back to the main thread
        return True, file_path.name, (counts, max_val)

    except Exception as e:
        return False, file_path.name, str(e)


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
        return

    print(f"Scanning {data_dir} for target files...")
    
    # Find directories matching the specific regex pattern
    folder_pattern = re.compile(r"dual_gpu_\d{4}_parquet_to_\d{4}_parquet$")
    target_dirs = [d for d in data_dir.iterdir() if d.is_dir() and folder_pattern.match(d.name)]
    
    target_files = []
    for d in target_dirs:
        target_files.extend(list(d.rglob("*.parquet")))
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        return

    # --- Parallel Processing ---
    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    # Prepare arguments for the worker function
    tasks = [(fp, output_dir) for fp in target_files]
    
    success_count = 0
    fail_count = 0
    
    # Global tracking variables
    global_counts = Counter()
    global_max = 0.0

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing", unit="file"):
            success, filename, result_data = future.result()
            
            if success:
                success_count += 1
                counts, file_max = result_data
                
                # Update global variables
                if counts:
                    global_counts.update(counts)
                if file_max is not None and file_max > global_max:
                    global_max = file_max
            else:
                fail_count += 1
                # result_data contains the error string on failure
                tqdm.write(f"[ERROR] Failed on {filename}: {result_data}")

    # --- Global Distribution Analysis ---
    print("\n" + "=" * 70)
    print(f"ABSOLUTE MAX RECORDED: {global_max:.2f} W")
    print("=" * 70)
    print("Top 15 Highest Power Draws (Rounded to nearest Watt) and their global frequencies:")
    
    if global_counts:
        sorted_wattages = sorted(global_counts.keys(), reverse=True)
        for watt in sorted_wattages[:15]:
            print(f"{watt} W : {global_counts[watt]:,} instances")

    # --- Optional: Generate ONE Global Plot ---
    if global_counts and GENERATES_FILES:
        watts = list(global_counts.keys())
        freqs = list(global_counts.values())
        
        plt.figure(figsize=(12, 6))
        plt.bar(watts, freqs, color='skyblue', width=1.0)
        plt.yscale('log')
        plt.title('Global Distribution of Power Draw (Log Scale)')
        plt.xlabel('Power Draw (Watts)')
        plt.ylabel('Frequency (Log Scale)')
        plt.grid(axis='y', alpha=0.3)
        plt.xticks(np.arange(0, max(watts) + 10, 10), rotation=45)
        
        plot_path = output_dir / f"{script_name}_global_distribution.png"
        plt.savefig(plot_path, dpi=150)
        plt.close()

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