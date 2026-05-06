"""
===============================================================================
Script Name: 07_inspect_pt_tensors.py
Description: Exhaustively explores and logs the structure of specific 
             PyTorch (.pt) tensor files.
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
from datetime import timedelta
import torch

# --- 1. CONFIGURATION ---
# Suppress pandas/numpy/torch warnings for cleaner console output
warnings.filterwarnings('ignore')

N_CORES = 3             # 3 target files, so 3 workers are optimal
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
        

# --- 3. HELPER FUNCTIONS FOR TENSOR EXPLORATION ---
def inspect_tensor(tensor, lines, indent=""):
    """Extracts exhaustive details and the 'head' of a specific tensor."""
    lines.append(f"{indent}Tensor | Shape: {list(tensor.shape)} | DType: {tensor.dtype}")
    
    # Flatten the tensor so we can easily grab the first few elements regardless of its dimensions
    flat_tensor = tensor.flatten()
    num_elements = flat_tensor.numel()
    
    if num_elements > 0:
        # 1. Print the Head
        head_size = min(5, num_elements) 
        head_vals = flat_tensor[:head_size].tolist()
        
        # Format the numbers so they are easy to read
        head_str = ", ".join([f"{v:.4f}" if isinstance(v, float) else str(v) for v in head_vals])
        lines.append(f"{indent}  ├── Head (first {head_size}): [{head_str}{' ...' if num_elements > head_size else ''}]")
        
        # 2. Print Exhaustive Statistics
        try:
            # We cast to float so operations like .mean() and .median() work on integer tensors as well
            float_tensor = tensor.float()
            t_min = float_tensor.min().item()
            t_max = float_tensor.max().item()
            t_mean = float_tensor.mean().item()
            t_median = float_tensor.median().item()
            
            lines.append(f"{indent}  └── Stats: Min = {t_min:.4f}, Max = {t_max:.4f}, Mean = {t_mean:.4f}, Median = {t_median:.4f}")
        except Exception as e:
            lines.append(f"{indent}  └── Stats: [Could not compute statistics: {e}]")
    else:
        lines.append(f"{indent}  └── [Empty Tensor]")

def explore_structure(data, lines, indent=""):
    """Recursively deeply explores the PyTorch file."""
    if isinstance(data, dict):
        lines.append(f"{indent}Dictionary with {len(data)} keys:")
        for count, (k, v) in enumerate(data.items()):
            # Print all keys to be exhaustive, but stop if there are thousands
            if count > 30:
                lines.append(f"{indent}├── ... and {len(data) - 30} more keys omitted for readability.")
                break
            lines.append(f"{indent}├── Key: '{k}'")
            explore_structure(v, lines, indent + "│   ")
            
    elif isinstance(data, list) or isinstance(data, tuple):
        type_name = "List" if isinstance(data, list) else "Tuple"
        lines.append(f"{indent}{type_name} with {len(data)} elements.")
        if len(data) > 0:
            lines.append(f"{indent}├── [Looking inside Item 0]")
            explore_structure(data[0], lines, indent + "│   ")
            if len(data) > 1:
                lines.append(f"{indent}└── ... and {len(data) - 1} more items with similar structure.")
                
    elif torch.is_tensor(data):
        inspect_tensor(data, lines, indent)
        
    else:
        # For strings, ints, floats, etc.
        val_str = str(data)
        if len(val_str) > 200:
            val_str = val_str[:200] + " ... [TRUNCATED]"
        lines.append(f"{indent}Type: {type(data).__name__} | Value: {val_str}")


# --- 4. WORKER FUNCTION ---
def process_single_item(item_args):
    """
    Worker function to process a single file or task.
    Must remain at the top level for multiprocessing pickling.
    """
    file_path, output_dir = item_args
    try:
        data = torch.load(file_path, map_location='cpu', weights_only=False)
        
        # We capture everything into a list of strings so parallel printing doesn't get garbled
        lines = []
        lines.append("="*70)
        lines.append(f"EXHAUSTIVE CONTENTS FOR: {file_path.name}")
        lines.append("="*70)
        
        explore_structure(data, lines)
        
        lines.append("="*70)
        output_str = "\n".join(lines)
        
        # Return success, filename, and the captured output text
        return True, file_path.name, output_str

    except Exception as e:
        return False, file_path.name, str(e)


# --- 5. MAIN EXECUTION ---
def main():
    # --- Path Resolution ---
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    project_root = script_path.parent.parent.parent
    scripts_dir = script_path.parent.parent
    
    # Path updated per user request
    data_dir = project_root / "data" / "mit-supercloud-dataset" / "gpu" / "dual_gpu_0000_parquet_to_0019_parquet_cleaned_split_tensors"
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
    # --- Dynamic Output Routing ---
    if GENERATES_FILES:
        log_dir = outputs_base_dir / f"{script_name}_output"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        if CREATE_DATA_SYMLINK:
            actual_output_dir = data_dir / f"{script_name}_processed"
            actual_output_dir.mkdir(parents=True, exist_ok=True)
            shortcut_path = log_dir / f"{script_name}_data_shortcut"
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

    # --- Data Discovery ---
    if not data_dir.exists():
        print(f"[ERROR] Target directory does not exist: {data_dir}")
        sys.stdout = sys.stdout.terminal
        return

    print(f"Scanning {data_dir} for target files...")
    
    # UPDATED: Dynamically find 1 train, 1 val, and 1 test file based on prefixes
    target_files = []
    for prefix in ["train_", "val_", "test_"]:
        # glob returns a generator, so we can just grab the first match
        matches = list(data_dir.glob(f"{prefix}*.pt"))
        if matches:
            target_files.append(matches[0])
            print(f"[*] Found {prefix} file: {matches[0].name}")
        else:
            print(f"[!] Warning: No file found starting with '{prefix}'")
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        sys.stdout = sys.stdout.terminal
        return

    # --- Parallel Processing ---
    print(f"\nStarting parallel processing on {total_files} files using {min(N_CORES, total_files)} cores...\n")
    print("Calculating statistics on large tensors might take a few extra seconds...\n")
    
    tasks = [(fp, actual_output_dir) for fp in target_files]
    
    overall_success = 0
    overall_fail = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=min(N_CORES, total_files)) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing", unit="file"):
            success, filename, payload = future.result()
            
            if success:
                overall_success += 1
                # Output the string processed by the worker safely through the main thread
                tqdm.write(f"\n{payload}\n")
            else:
                overall_fail += 1
                tqdm.write(f"[ERROR] Failed on {filename}: {payload}")

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