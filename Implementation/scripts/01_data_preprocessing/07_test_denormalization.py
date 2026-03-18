"""
===============================================================================
Script Name: 07_test_denormalization.py
Description: Tests the de-normalization logic by applying the inverse scale 
             from normalization_metadata.csv to 10 sample normalized files.
             Outputs reconstructed files for manual verification.
===============================================================================
"""

import sys
import time
import warnings
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import concurrent.futures
import re
from datetime import timedelta

# --- 1. CONFIGURATION ---
warnings.filterwarnings('ignore')

N_CORES = 4             # Fewer cores needed for a small test
GENERATES_FILES = True  
CREATE_DATA_SYMLINK = False # Enforced False to keep outputs strictly in the logs folder

MAX_TEST_FILES = 10     # Only test on a small subset
GLOBAL_POWER_SCALE = 267.500 # From your previous terminal logs

TEMP_COLS = [
    "temperature_gpu_0", "temperature_gpu_1",
    "temperature_memory_0", "temperature_memory_1"
]


# --- 2. TERMINAL LOGGING UTILITY ---
class DualLogger:
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
    Worker function: De-normalizes a single file using the metadata dictionary.
    """
    file_path, output_dir, metadata_dict = item_args
    try:
        # Script 05 added "_normalized" to the filename. 
        # We must strip it to look up the original name in the CSV.
        lookup_name = file_path.name.replace("_normalized.parquet", ".parquet")
        
        if lookup_name not in metadata_dict:
            return False, file_path.name, f"Metadata key '{lookup_name}' not found in CSV"

        meta = metadata_dict[lookup_name]

        # 1. Read the normalized data
        df = pd.read_parquet(file_path)

        # 2. De-normalize Time
        df["timestamp"] = df["timestamp"] * meta["t_max"]

        # 3. De-normalize Power
        df["power_draw_gpu_0_W"] = df["power_draw_gpu_0_W"] * GLOBAL_POWER_SCALE
        df["power_draw_gpu_1_W"] = df["power_draw_gpu_1_W"] * GLOBAL_POWER_SCALE

        # 4. De-normalize Temperatures (Z-score inverse: X = (Z * std) + mean)
        for col in TEMP_COLS:
            df[col] = (df[col] * meta[f"{col}_std"]) + meta[f"{col}_mean"]

        # 5. Save output securely in the outputs directory
        if GENERATES_FILES:
            output_file = output_dir / f"reconstructed_{lookup_name}"
            df.to_parquet(output_file, index=False)
            
        return True, file_path.name, None

    except Exception as e:
        return False, file_path.name, str(e)


# --- 4. MAIN EXECUTION ---
def main():
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    project_root = script_path.parent.parent.parent
    scripts_dir = script_path.parent.parent
    
    data_dir = project_root / "data" / "mit-supercloud-dataset" / "gpu"
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
    # Strictly route outputs to the local logs folder
    log_dir = outputs_base_dir / f"{script_name}_output"
    log_dir.mkdir(parents=True, exist_ok=True)
    actual_output_dir = log_dir

    log_path = log_dir / f"{script_name}_terminal_output.txt"
    sys.stdout = DualLogger(log_path)
    
    start_time = time.perf_counter()
    
    print("=" * 70)
    print(f"--- STARTING: {script_name.upper()} ---")
    print("=" * 70)

    # --- 1. Load Metadata Map ---
    # We expect the previous script 06 to have saved it here:
    metadata_csv_path = outputs_base_dir / "06_extract_normalization_metadata_output" / "normalization_metadata.csv"
    
    if not metadata_csv_path.exists():
        print(f"[ERROR] Could not find metadata CSV at: {metadata_csv_path}")
        sys.stdout = sys.stdout.terminal
        return

    print("Loading metadata CSV into memory...")
    meta_df = pd.read_csv(metadata_csv_path)
    # Convert dataframe to a dictionary for lightning-fast row lookups by filename
    metadata_dict = meta_df.set_index('filename').to_dict('index')
    print(f"Loaded metadata for {len(metadata_dict)} files.\n")

    # --- 2. Find Normalized Data ---
    print(f"Scanning {data_dir} for NORMALIZED directories...")
    pattern = re.compile(r"dual_gpu_\d{4}_parquet_to_\d{4}_parquet_cleaned_normalized$")
    normalized_dirs = [d for d in data_dir.iterdir() if d.is_dir() and pattern.match(d.name)]
    
    target_files = []
    source_folder_name = ""
    for d in normalized_dirs:
        # Grab files and record the folder name for logging
        files_in_dir = list(d.glob("*.parquet"))
        if files_in_dir:
            source_folder_name = d.name 
            target_files.extend(files_in_dir)
            break # Stop after finding the first valid folder for this test
    
    if len(target_files) == 0:
        print("[!] No normalized parquet files found. Exiting.")
        sys.stdout = sys.stdout.terminal
        return

    # Slice the list to only process a small subset
    target_files = target_files[:MAX_TEST_FILES]
    total_files = len(target_files)

    print(f"Pulling {total_files} files from source folder: [ {source_folder_name} ]\n")

    # --- 3. Parallel Processing ---
    tasks = [(fp, actual_output_dir, metadata_dict) for fp in target_files]
    
    overall_success = 0
    overall_fail = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="De-normalizing", unit="file"):
            success, filename, error_msg = future.result()
            
            if success:
                overall_success += 1
            else:
                overall_fail += 1
                tqdm.write(f"[ERROR] Failed on {filename}: {error_msg}")

    # --- Final Summary ---
    elapsed = time.perf_counter() - start_time
    
    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total processed : {total_files}")
    print(f"Successful      : {overall_success}")
    print(f"Failed          : {overall_fail}")
    print(f"Total Time      : {str(timedelta(seconds=int(elapsed)))}")
    print(f"\n[!] Reconstructed files safely saved to:\n    {actual_output_dir}")

    sys.stdout = sys.stdout.terminal

if __name__ == "__main__":
    main()