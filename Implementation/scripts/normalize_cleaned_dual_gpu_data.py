"""
===============================================================================
Script Name: normalize_cleaned_dual_gpu_data.py
Description: Normalizes GPU power and temperature data, discarding non-excited 
             files, using a dynamically calculated global power scale.
===============================================================================
"""

import sys
import warnings
import pandas as pd
import numpy as np
import math
import re
import os
from pathlib import Path
from tqdm import tqdm
import concurrent.futures

# --- 1. CONFIGURATION ---
# Suppress pandas/numpy warnings for cleaner console output
warnings.filterwarnings('ignore')

N_CORES = 8             # Default cores for parallel processing
GENERATES_FILES = True  # SET TO FALSE if this script ONLY prints to terminal

# Histogram and Power config
POWER_PERCENTILE = 99.9
POWER_MIN = 0
BIN_WIDTH = 0.5


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


# --- 3. HELPER FUNCTIONS ---
def compute_global_power_scale(parquet_files):
    """Calculates the dynamic power scale using a 2-pass approach."""
    actual_max_power = 0.0

    print("\nPass 1: Finding ACTUAL global max power...")
    for file in tqdm(parquet_files, desc="Finding Max Power"):
        try:
            df = pd.read_parquet(
                file,
                columns=["power_draw_gpu_0_W", "power_draw_gpu_1_W"],
            )
            values = np.concatenate([
                df["power_draw_gpu_0_W"].values,
                df["power_draw_gpu_1_W"].values,
            ])
            actual_max_power = max(actual_max_power, values.max())
        except Exception:
            continue
            
    print(f"\nACTUAL GLOBAL MAX POWER DRAW = {actual_max_power:.3f} W")

    dynamic_power_max = math.ceil(actual_max_power)
    bins = np.arange(
        POWER_MIN,
        dynamic_power_max + (2 * BIN_WIDTH), 
        BIN_WIDTH,
    )

    histogram = np.zeros(len(bins) - 1)

    print("\nPass 2: Analyzing GLOBAL power draw distribution...")
    for file in tqdm(parquet_files, desc="Building Histogram"):
        try:
            df = pd.read_parquet(
                file,
                columns=["power_draw_gpu_0_W", "power_draw_gpu_1_W"],
            )
            values = np.concatenate([
                df["power_draw_gpu_0_W"].values,
                df["power_draw_gpu_1_W"].values,
            ])
            h, _ = np.histogram(values, bins=bins)
            histogram += h
        except Exception:
            continue

    cumulative = np.cumsum(histogram)
    total = cumulative[-1]
    threshold = POWER_PERCENTILE / 100 * total
    idx = np.searchsorted(cumulative, threshold)
    power_scale = bins[idx]

    print(f"\nGLOBAL POWER DRAW SCALE (P{POWER_PERCENTILE}) = {power_scale:.3f} W")
    return power_scale


# --- 4. WORKER FUNCTION ---
def process_single_item(item_args):
    """
    Worker function to process a single file or task.
    Must remain at the top level for multiprocessing pickling.
    """
    file_path, output_dir, power_scale = item_args
    try:
        # 1. Read data
        df = pd.read_parquet(file_path)

        if len(df) < 2:
            return False, file_path.name, "File has fewer than 2 rows"

        # 2. Perform operations
        # TIME NORMALIZATION
        tmax = df["timestamp"].max()
        if tmax <= 0:
            return False, file_path.name, "Invalid timestamp max"
        df["timestamp"] /= tmax

        # TEMPERATURE Z-SCORE
        temp_cols = [
            "temperature_gpu_0", "temperature_gpu_1",
            "temperature_memory_0", "temperature_memory_1",
        ]
        for col in temp_cols:
            mean = df[col].mean()
            std = df[col].std()
            if std == 0:
                return False, file_path.name, f"Zero std dev in {col}"
            df[col] = (df[col] - mean) / std

        # POWER NORMALIZATION
        df["power_draw_gpu_0_W"] /= power_scale
        df["power_draw_gpu_1_W"] /= power_scale

        # DISCARD NON-EXCITED FILES
        if "excited" in df.columns and df["excited"].sum() == 0:
            return False, file_path.name, "No excited state found"

        # 3. Save output (if applicable)
        if GENERATES_FILES:
            output_file = output_dir / f"{file_path.stem}_normalized.parquet"
            df.to_parquet(output_file, index=False)
            
        return True, file_path.name, None

    except Exception as e:
        return False, file_path.name, str(e)


# --- 5. MAIN EXECUTION ---
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
        base_output_dir = outputs_base_dir / f"{script_name}_output"
        base_output_dir.mkdir(parents=True, exist_ok=True)
    else:
        base_output_dir = outputs_base_dir
        base_output_dir.mkdir(parents=True, exist_ok=True)

    # --- Initialize Dual Logging ---
    log_path = base_output_dir / f"{script_name}_terminal_output.txt"
    sys.stdout = DualLogger(log_path)
    
    print("=" * 70)
    print(f"--- STARTING: {script_name.upper()} ---")
    print("=" * 70)

    # --- Data Discovery ---
    if not data_dir.exists():
        print(f"[ERROR] Target directory does not exist: {data_dir}")
        return

    pattern = re.compile(r"dual_gpu_\d{4}_parquet_to_\d{4}_parquet_cleaned$")
    cleaned_dirs = [d for d in data_dir.iterdir() if d.is_dir() and pattern.match(d.name)]
    
    n_dirs = len(cleaned_dirs)
    if n_dirs == 0:
        print("[!] No target directories found. Exiting.")
        return

    print(f"Found {n_dirs} cleaned dual GPU directories. Using {N_CORES} cores.\n")

    overall_success = 0
    overall_fail = 0

    # Process each directory
    for cleaned_dir in cleaned_dirs:
        print(f"\n--- Processing Directory: {cleaned_dir.name} ---")
        
        # Clean up directory name (swap '_cleaned' with '_normalized')
        normalized_dir_name = cleaned_dir.name.replace("_cleaned", "_normalized")
        if "_normalized" not in normalized_dir_name:
            normalized_dir_name += "_normalized"

        # 1. Primary Output Dir: Create alongside the _cleaned folder
        dir_output = cleaned_dir.parent / normalized_dir_name
        
        if GENERATES_FILES:
            dir_output.mkdir(exist_ok=True, parents=True)

            # 2. Create shortcut in the standard outputs folder
            shortcut_path = base_output_dir / normalized_dir_name
            if not shortcut_path.exists() and not shortcut_path.is_symlink():
                try:
                    shortcut_path.symlink_to(dir_output, target_is_directory=True)
                    print(f"[*] Created shortcut for {normalized_dir_name} in outputs folder.")
                except OSError as e:
                    print(f"[!] Warning: Could not create shortcut in outputs folder: {e}")

        target_files = list(cleaned_dir.glob("*.parquet"))
        total_files = len(target_files)
        
        if total_files == 0:
            print(f"[!] No parquet files found in {cleaned_dir.name}. Skipping.")
            continue

        # Compute dynamic scale for this directory's files
        power_scale = compute_global_power_scale(target_files)

        # --- Parallel Processing ---
        print(f"\nStarting parallel processing on {total_files} files...")
        tasks = [(fp, dir_output, power_scale) for fp in target_files]
        
        success_count = 0
        fail_count = 0

        with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
            futures = {executor.submit(process_single_item, task): task for task in tasks}
            
            for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Normalizing", unit="file"):
                success, filename, error_msg = future.result()
                
                if success:
                    success_count += 1
                else:
                    fail_count += 1
                    tqdm.write(f"[DEBUG] Skipped {filename}: {error_msg}")

        overall_success += success_count
        overall_fail += fail_count
        
        print(f"Directory Complete | normalized {success_count}/{total_files} (Skipped: {fail_count})")

    # --- Final Summary ---
    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total processed : {overall_success + overall_fail}")
    print(f"Successful      : {overall_success}")
    print(f"Skipped/Failed  : {overall_fail}")
    
    if GENERATES_FILES:
        print(f"\n[!] Outputs saved near _cleaned folders, shortcuts and logs saved to: {base_output_dir}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    # Restore standard output just in case
    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()