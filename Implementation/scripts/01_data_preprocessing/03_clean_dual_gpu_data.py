"""
===============================================================================
Script Name: 03_clean_dual_gpu_data_v2.py
Description: Cleans dual GPU parquet data by aligning timestamps, filtering for
             dynamic activity, and retaining causal workload metrics (utilization) 
             alongside thermodynamic state variables.
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
import re
from datetime import timedelta

# --- 1. CONFIGURATION ---
# Suppress pandas/numpy warnings for cleaner console output
warnings.filterwarnings('ignore')

N_CORES = 6             # Default cores for parallel processing
GENERATES_FILES = True  # SET TO FALSE if this script ONLY prints to terminal
CREATE_DATA_SYMLINK = False # SET TO TRUE for large outputs

# Domain-specific thresholds/constants
BIN_SIZE = 0.1                 # seconds: nearest-match tolerance = BIN_SIZE / 2
DT_THRESHOLD = 1.0             # degC change threshold (>=)
DP_THRESHOLD = 5.0             # W change threshold (>=)
MIN_LENGTH = 50                # minimal paired rows to consider file valid


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
    file_path, specific_output_dir = item_args
    try:
        output_file = specific_output_dir / f"{file_path.stem}_cleaned.parquet"
        if output_file.exists():
            return True, file_path.name, None # Skip existing to avoid overwrite

        # 1. Read data
        df = pd.read_parquet(file_path)

        # required columns check (Now including causal variables)
        required_cols = [
            "timestamp",
            "gpu_index",
            "temperature_gpu",
            "temperature_memory",
            "power_draw_W",
            "utilization_gpu_pct",
            "utilization_memory_pct"
        ]
        if not all(c in df.columns for c in required_cols):
            return False, file_path.name, "Missing required columns"

        # keep only columns we need
        df = df[required_cols].copy()

        # ensure numeric timestamp
        df["timestamp"] = df["timestamp"].astype(float)

        # split data by GPU index and sort
        df0 = df[df["gpu_index"] == 0].sort_values("timestamp").reset_index(drop=True)
        df1 = df[df["gpu_index"] == 1].sort_values("timestamp").reset_index(drop=True)

        if df0.empty or df1.empty:
            return False, file_path.name, "Missing data for one or both GPUs"

        # prepare left (gpu0) and right (gpu1) frames for merge_asof
        left = df0.rename(columns={
            "temperature_gpu": "temperature_gpu_0",
            "temperature_memory": "temperature_memory_0",
            "power_draw_W": "power_draw_gpu_0_W",
            "utilization_gpu_pct": "utilization_gpu_0_pct",
            "utilization_memory_pct": "utilization_memory_0_pct"
        })[["timestamp", "temperature_gpu_0", "temperature_memory_0", 
            "power_draw_gpu_0_W", "utilization_gpu_0_pct", "utilization_memory_0_pct"]]

        right = df1.rename(columns={
            "timestamp": "timestamp_1",
            "temperature_gpu": "temperature_gpu_1",
            "temperature_memory": "temperature_memory_1",
            "power_draw_W": "power_draw_gpu_1_W",
            "utilization_gpu_pct": "utilization_gpu_1_pct",
            "utilization_memory_pct": "utilization_memory_1_pct"
        })[["timestamp_1", "temperature_gpu_1", "temperature_memory_1", 
            "power_draw_gpu_1_W", "utilization_gpu_1_pct", "utilization_memory_1_pct"]]

        # nearest-neighbor match each left row to closest right row within tolerance
        tolerance = BIN_SIZE / 2.0

        paired = pd.merge_asof(
            left.sort_values("timestamp"),
            right.sort_values("timestamp_1"),
            left_on="timestamp",
            right_on="timestamp_1",
            direction="nearest",
            tolerance=tolerance
        )

        paired.dropna(subset=["timestamp_1"], inplace=True)
        if paired.empty:
            return False, file_path.name, "No matching timestamps within tolerance"

        paired["timestamp"] = (paired["timestamp"] + paired["timestamp_1"]) / 2.0

        # select and order final columns
        pivot = paired[[
            "timestamp",
            "utilization_gpu_0_pct", "utilization_gpu_1_pct",
            "utilization_memory_0_pct", "utilization_memory_1_pct",
            "power_draw_gpu_0_W", "power_draw_gpu_1_W",
            "temperature_gpu_0", "temperature_gpu_1",
            "temperature_memory_0", "temperature_memory_1"
        ]].copy()

        # sort and convert to elapsed time relative to first sample
        pivot = pivot.sort_values("timestamp").reset_index(drop=True)
        pivot["timestamp"] = pivot["timestamp"] - pivot["timestamp"].iloc[0]

        if len(pivot) < MIN_LENGTH:
            return False, file_path.name, f"Length {len(pivot)} < MIN_LENGTH {MIN_LENGTH}"

        # compute diffs and excitation mask (based strictly on thermodynamics)
        dT0 = pivot["temperature_gpu_0"].diff().abs().fillna(0.0)
        dT1 = pivot["temperature_gpu_1"].diff().abs().fillna(0.0)
        dP0 = pivot["power_draw_gpu_0_W"].diff().abs().fillna(0.0)
        dP1 = pivot["power_draw_gpu_1_W"].diff().abs().fillna(0.0)

        excitation_mask = (
            (dT0 >= DT_THRESHOLD)
            | (dT1 >= DT_THRESHOLD)
            | (dP0 >= DP_THRESHOLD)
            | (dP1 >= DP_THRESHOLD)
        )

        pivot["excited"] = excitation_mask.astype(int)

        if pivot["excited"].sum() == 0:
            return False, file_path.name, "No dynamic information (no excited rows)"

        # 3. Save output
        if GENERATES_FILES:
            pivot.to_parquet(output_file, index=False)

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
    
    # --- Dynamic Output Routing ---
    if GENERATES_FILES:
        log_dir = outputs_base_dir / f"{script_name}_output"
        log_dir.mkdir(parents=True, exist_ok=True)
        # Bypassing CREATE_DATA_SYMLINK global to match original script's per-folder logic
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

    print(f"Scanning {data_dir} for target directories...")
    
    pattern = re.compile(r"dual_gpu_\d{4}_parquet_to_\d{4}_parquet$")
    dual_gpu_dirs = [d for d in data_dir.iterdir() if d.is_dir() and pattern.match(d.name)]
    
    if len(dual_gpu_dirs) == 0:
        print("[!] No target directories found. Exiting.")
        sys.stdout = sys.stdout.terminal
        return

    print(f"Found {len(dual_gpu_dirs)} valid dual GPU directories.")

    target_files = [] 
    for d in dual_gpu_dirs:
        specific_output_dir = d.parent / f"{d.name}_cleaned"
        specific_output_dir.mkdir(parents=True, exist_ok=True)
        
        shortcut_path = log_dir / f"{d.name}_cleaned"
        if not shortcut_path.exists() and not shortcut_path.is_symlink():
            try:
                shortcut_path.symlink_to(specific_output_dir, target_is_directory=True)
            except OSError as e:
                print(f"[WARNING] Could not create shortcut for {d.name} in outputs: {e}")
        
        for p in d.glob("*.parquet"):
            target_files.append((p, specific_output_dir))
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        sys.stdout = sys.stdout.terminal
        return

    # --- Parallel Processing ---
    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    overall_success = 0
    overall_fail = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in target_files}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing", unit="file"):
            success, filename, error_msg = future.result()
            
            if success:
                overall_success += 1
            else:
                overall_fail += 1
                if error_msg: 
                    tqdm.write(f"[SKIPPED/FAILED] {filename}: {error_msg}")

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
        print(f"\n[!] Cleaned files saved to actual locations alongside original directories.")
        print(f"[!] Shortcuts and logs saved to: {log_dir}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    # Restore standard output just in case
    sys.stdout = sys.stdout.terminal

if __name__ == "__main__":
    main()