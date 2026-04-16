"""
===============================================================================
Script Name: 06_ode_segment_prep.py
Description: Scans train/val/test parquets, computes per-job ambient 
             temperatures (T_amb), splits timeseries into 5000-row segments, 
             pads short segments, and stacks them into chunked PyTorch (.pt) 
             tensors for GPU calibration to avoid RAM exhaustion.
===============================================================================
"""

import sys
import time
import warnings
import json
import pandas as pd
import numpy as np
import torch
import gc
import re
from pathlib import Path
from tqdm import tqdm
import concurrent.futures
from datetime import timedelta

# --- 1. CONFIGURATION ---
warnings.filterwarnings('ignore')

N_CORES = 2             # Default cores for parallel processing
GENERATES_FILES = True  # SET TO FALSE if this script ONLY prints to terminal
CREATE_DATA_SYMLINK = True # SET TO TRUE because output tensors will be multi-GB

# --- Domain-Specific Constants ---
SEGMENT_LEN = 5000
H_0_PRIOR = 4.8713  # Downstream convective cooling prior
H_1_PRIOR = 5.3871  # Upstream convective cooling prior

# Column mapping (Adjusted to match cleaned parquets)
COL_POWER_0 = 'power_draw_gpu_0_W'
COL_POWER_1 = 'power_draw_gpu_1_W'
COL_TEMP_0  = 'temperature_gpu_0'
COL_TEMP_1  = 'temperature_gpu_1'
COL_UTIL_0  = 'utilization_gpu_0_pct'
COL_UTIL_1  = 'utilization_gpu_1_pct'


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
    Returns the split name, filename, and a list of segment dictionaries.
    """
    file_path, split_name = item_args
    try:
        # 1. Parse Metadata from Filename
        clean_name = file_path.stem.replace('_cleaned', '')
        parts = clean_name.split('-')
        job_id = parts[0]
        rack_id = parts[1] if len(parts) > 1 else "unknown"
        node_id = parts[2] if len(parts) > 2 else "unknown"

        # 2. Read Data
        df = pd.read_parquet(file_path)
        if len(df) == 0:
            return True, split_name, file_path.name, [], None

        # 3. Compute T_amb (The "Hack")
        head_df = df.head(100)
        # Check if there is an idle period at the start
        idle_mask = (head_df[COL_UTIL_0] == 0) & (head_df[COL_UTIL_1] == 0)
        
        if idle_mask.any():
            idx = head_df[idle_mask].index[0] # The "Cold Start" (Ideal)
        else:
            idx = head_df.index[0]            # The "Hot Start" (Fallback)

        t0, t1 = df.at[idx, COL_TEMP_0], df.at[idx, COL_TEMP_1]
        p0, p1 = df.at[idx, COL_POWER_0], df.at[idx, COL_POWER_1]

        # Use the cooler GPU to estimate ambient, applying its specific cooling prior
        if t0 < t1:
            t_amb = t0 - (p0 / H_0_PRIOR)
        else:
            t_amb = t1 - (p1 / H_1_PRIOR)

        # 4. Split into Segments
        total_len = len(df)
        file_segments = []

        for start_idx in range(0, total_len, SEGMENT_LEN):
            end_idx = min(start_idx + SEGMENT_LEN, total_len)
            valid_len = end_idx - start_idx
            
            # Extract arrays as float32
            p0_arr = df[COL_POWER_0].iloc[start_idx:end_idx].values.astype(np.float32)
            p1_arr = df[COL_POWER_1].iloc[start_idx:end_idx].values.astype(np.float32)
            t0_arr = df[COL_TEMP_0].iloc[start_idx:end_idx].values.astype(np.float32)
            t1_arr = df[COL_TEMP_1].iloc[start_idx:end_idx].values.astype(np.float32)
            
            # Pad arrays with 0s if it's the final, short segment
            if valid_len < SEGMENT_LEN:
                pad_len = SEGMENT_LEN - valid_len
                p0_arr = np.pad(p0_arr, (0, pad_len), 'constant', constant_values=0)
                p1_arr = np.pad(p1_arr, (0, pad_len), 'constant', constant_values=0)
                t0_arr = np.pad(t0_arr, (0, pad_len), 'constant', constant_values=0)
                t1_arr = np.pad(t1_arr, (0, pad_len), 'constant', constant_values=0)

            file_segments.append({
                'P0': p0_arr,
                'P1': p1_arr,
                'T0': t0_arr,
                'T1': t1_arr,
                'T_amb': float(t_amb),
                'valid_len': int(valid_len),
                'job_id': job_id,
                'node_id': node_id
            })

        return True, split_name, file_path.name, file_segments, None

    except Exception as e:
        return False, split_name, file_path.name, [], str(e)


# --- 4. MAIN EXECUTION ---
def main():
    # --- Path Resolution ---
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    project_root = script_path.parent.parent.parent
    scripts_dir = script_path.parent.parent
    
    # Base GPU folder
    gpu_base_dir = project_root / "data" / "mit-supercloud-dataset" / "gpu"
    
    # Dynamically find the matching split folder
    data_dir = None
    dir_pattern = re.compile(r"dual_gpu_\d{4}_parquet_to_\d{4}_parquet_cleaned_split")
    
    if gpu_base_dir.exists():
        for path in gpu_base_dir.iterdir():
            if path.is_dir() and dir_pattern.match(path.name):
                data_dir = path
                break
                
    if data_dir is None:
        print(f"[ERROR] Could not find a directory matching the pattern in {gpu_base_dir}")
        sys.exit(1)
        
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
    # --- Dynamic Output Routing ---
    if GENERATES_FILES:
        log_dir = outputs_base_dir / f"{script_name}_output"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        if CREATE_DATA_SYMLINK:
            # Derive target folder name: dual_gpu_XXXX_..._cleaned_tensors
            target_folder_name = data_dir.name.replace("_split", "_tensors")
            actual_output_dir = gpu_base_dir / target_folder_name
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
    print(f"[*] Matched source directory: {data_dir.name}")

    # --- Data Discovery ---
    if not data_dir.exists():
        print(f"[ERROR] Target directory does not exist: {data_dir}")
        sys.stdout = sys.stdout.terminal
        return

    print(f"Scanning {data_dir} for train/val/test parquets...")
    
    target_files = []
    for split in ['train', 'val', 'test']:
        split_dir = data_dir / split
        if split_dir.exists():
            files = list(split_dir.glob("*.parquet"))
            for f in files:
                target_files.append((f, split))
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        sys.stdout = sys.stdout.terminal
        return

    # --- Parallel Processing ---
    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    overall_success = 0
    overall_fail = 0
    
    # Dictionaries to aggregate the results
    aggregated_segments = {'train': [], 'val': [], 'test': []}
    metadata_log = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in target_files}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing files"):
            success, split_name, filename, file_segments, error_msg = future.result()
            
            if success:
                overall_success += 1
                aggregated_segments[split_name].extend(file_segments)
                
                # Optionally track file-level metadata
                if file_segments:
                    metadata_log.append({
                        'job_id': file_segments[0]['job_id'],
                        'split': split_name,
                        't_amb': float(file_segments[0]['T_amb']),
                        'num_segments': len(file_segments)
                    })
            else:
                overall_fail += 1
                tqdm.write(f"[ERROR] Failed on {filename}: {error_msg}")

    # --- Tensor Stacking & Saving (CHUNKED) ---
    print("\n" + "=" * 70)
    print("--- CONVERTING TO PYTORCH TENSORS (CHUNKED) ---")
    print("=" * 70)

    CHUNK_SIZE = 50000  # Process 50k segments at a time (~4.5 GB chunks)

    for split in ['train', 'val', 'test']:
        split_segs = aggregated_segments[split]
        num_segs = len(split_segs)
        
        if num_segs == 0:
            print(f"No segments generated for {split}. Skipping.")
            continue
            
        num_chunks = int(np.ceil(num_segs / CHUNK_SIZE))
        print(f"\nChunking {num_segs} '{split}' segments into {num_chunks} files...")
        
        for chunk_idx in range(num_chunks):
            start_idx = chunk_idx * CHUNK_SIZE
            end_idx = min((chunk_idx + 1) * CHUNK_SIZE, num_segs)
            chunk_data = split_segs[start_idx:end_idx]
            current_chunk_size = len(chunk_data)
            
            print(f"\n  -> Pre-allocating memory for {split} chunk {chunk_idx+1}/{num_chunks}...")
            
            # 1. Pre-allocate empty PyTorch tensors for JUST this chunk
            P0_tensor = torch.empty((current_chunk_size, SEGMENT_LEN), dtype=torch.float32)
            P1_tensor = torch.empty((current_chunk_size, SEGMENT_LEN), dtype=torch.float32)
            T0_tensor = torch.empty((current_chunk_size, SEGMENT_LEN), dtype=torch.float32)
            T1_tensor = torch.empty((current_chunk_size, SEGMENT_LEN), dtype=torch.float32)
            T_amb_tensor = torch.empty((current_chunk_size,), dtype=torch.float32)
            valid_len_tensor = torch.empty((current_chunk_size,), dtype=torch.int32)
            job_ids = []
            node_ids = []

            # 2. Fill them iteratively WITH a progress bar
            for i, s in enumerate(tqdm(chunk_data, desc=f"Stacking chunk {chunk_idx+1}", unit="seg")):
                P0_tensor[i] = torch.from_numpy(s['P0'])
                P1_tensor[i] = torch.from_numpy(s['P1'])
                T0_tensor[i] = torch.from_numpy(s['T0'])
                T1_tensor[i] = torch.from_numpy(s['T1'])
                T_amb_tensor[i] = float(s['T_amb'])
                valid_len_tensor[i] = int(s['valid_len'])
                job_ids.append(s['job_id'])
                node_ids.append(s['node_id'])

            tensor_dict = {
                'P0': P0_tensor,
                'P1': P1_tensor,
                'T0': T0_tensor,
                'T1': T1_tensor,
                'T_amb': T_amb_tensor,
                'valid_len': valid_len_tensor,
                'job_ids': job_ids,
                'node_ids': node_ids
            }
            
            # 3. Save to disk
            out_path = actual_output_dir / f"{split}_segments_part{chunk_idx:03d}.pt"
            torch.save(tensor_dict, out_path)
            print(f"  -> Saved {out_path.name} ({out_path.stat().st_size / (1024*1024):.2f} MB)")
            
            # 4. CRITICAL: Free up RAM before the next loop
            del P0_tensor, P1_tensor, T0_tensor, T1_tensor, T_amb_tensor, valid_len_tensor, tensor_dict, chunk_data
            gc.collect()
            
        # Free the raw list for this split before moving to the next split
        aggregated_segments[split] = []
        gc.collect()

    # Save metadata
    meta_path = actual_output_dir / "calibration_metadata.json"
    with open(meta_path, 'w') as f:
        # Re-calculate totals from the metadata_log since we cleared aggregated_segments
        total_train = sum(1 for m in metadata_log if m['split'] == 'train')
        total_val = sum(1 for m in metadata_log if m['split'] == 'val')
        total_test = sum(1 for m in metadata_log if m['split'] == 'test')
        
        json.dump({
            'total_train_jobs': total_train,
            'total_val_jobs': total_val,
            'total_test_jobs': total_test,
            'segment_length': SEGMENT_LEN,
            'chunk_size': CHUNK_SIZE,
            'job_metadata': metadata_log
        }, f, indent=4)

    # --- END TIMER & CALCULATE ---
    end_time = time.perf_counter()
    elapsed_seconds = end_time - start_time
    formatted_time = str(timedelta(seconds=int(elapsed_seconds)))

    # --- Final Summary ---
    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total files processed : {total_files}")
    print(f"Successful files      : {overall_success}")
    print(f"Failed files          : {overall_fail}")
    print(f"Total Time            : {formatted_time}")
    
    if GENERATES_FILES:
        if CREATE_DATA_SYMLINK:
             print(f"\n[!] Heavy .pt outputs saved to: {actual_output_dir}")
             print(f"[*] Shortcut and logs saved to: {log_dir}")
        else:
             print(f"\n[!] Outputs and logs saved to: {log_dir}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()