"""
===============================================================================
Script Name: 06_ode_dataset_preparation.py
Description: Scans train/val/test parquets, computes per-job ambient 
             temperatures (T_amb), and splits timeseries into 5000-row segments.
             
             BRANCHING STACK STRATEGY:
             - TRAIN: Batched into 10,000-segment chunk files to prevent OOM.
             - VAL:   Bundled into a single file for high-speed evaluation.
             - TEST:  1-to-1 physical mapping (1 pt tensor per parquet file) 
                      to guarantee uncorrupted physical trajectory testing.
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
TRAIN_CHUNK_SIZE = 100000

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
    Returns the split name, run_id (the filename), and a list of segment dictionaries.
    """
    file_path, split_name = item_args
    try:
        # 1. Parse Metadata: Strip _cleaned and keep the raw run identifier
        run_id = file_path.stem.replace('_cleaned', '')

        # 2. Read Data
        df = pd.read_parquet(file_path)
        if len(df) == 0:
            return True, split_name, run_id, [], None

        # 3. Compute T_amb (The "Hack")
        head_df = df.head(100)
        idle_mask = (head_df[COL_UTIL_0] == 0) & (head_df[COL_UTIL_1] == 0)
        
        if idle_mask.any():
            idx = head_df[idle_mask].index[0] # The "Cold Start"
        else:
            idx = head_df.index[0]            # The "Hot Start"

        t0, t1 = df.at[idx, COL_TEMP_0], df.at[idx, COL_TEMP_1]
        p0, p1 = df.at[idx, COL_POWER_0], df.at[idx, COL_POWER_1]

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
            
            p0_arr = df[COL_POWER_0].iloc[start_idx:end_idx].values.astype(np.float32)
            p1_arr = df[COL_POWER_1].iloc[start_idx:end_idx].values.astype(np.float32)
            t0_arr = df[COL_TEMP_0].iloc[start_idx:end_idx].values.astype(np.float32)
            t1_arr = df[COL_TEMP_1].iloc[start_idx:end_idx].values.astype(np.float32)
            
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
                'T_amb': np.float32(t_amb),
                'valid_len': valid_len,
                'run_id': run_id
            })

        return True, split_name, run_id, file_segments, None

    except Exception as e:
        return False, split_name, file_path.stem, [], str(e)


def create_tensor_dict(chunk_data, desc):
    """Helper function to cleanly pre-allocate and populate a tensor chunk."""
    sz = len(chunk_data)
    P0_t = torch.empty((sz, SEGMENT_LEN), dtype=torch.float32)
    P1_t = torch.empty((sz, SEGMENT_LEN), dtype=torch.float32)
    T0_t = torch.empty((sz, SEGMENT_LEN), dtype=torch.float32)
    T1_t = torch.empty((sz, SEGMENT_LEN), dtype=torch.float32)
    Tamb_t = torch.empty((sz,), dtype=torch.float32)
    vlen_t = torch.empty((sz,), dtype=torch.int32)
    run_ids = []

    for i, s in enumerate(tqdm(chunk_data, desc=desc, unit="seg", leave=False)):
        P0_t[i] = torch.from_numpy(s['P0'])
        P1_t[i] = torch.from_numpy(s['P1'])
        T0_t[i] = torch.from_numpy(s['T0'])
        T1_t[i] = torch.from_numpy(s['T1'])
        Tamb_t[i] = float(s['T_amb'])
        vlen_t[i] = int(s['valid_len'])
        run_ids.append(s['run_id'])

    return {
        'P0': P0_t, 'P1': P1_t, 'T0': T0_t, 'T1': T1_t,
        'T_amb': Tamb_t, 'valid_len': vlen_t, 'run_ids': run_ids
    }

# --- 4. MAIN EXECUTION ---
def main():
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    project_root = script_path.parent.parent.parent
    scripts_dir = script_path.parent.parent
    
    data_dir = project_root / "data" / "mit-supercloud-dataset" / "gpu" / "dual_gpu_0000_parquet_to_0019_parquet_cleaned_split"
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
    if GENERATES_FILES:
        log_dir = outputs_base_dir / f"{script_name}_output"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        if CREATE_DATA_SYMLINK:
            actual_output_dir = data_dir.parent / f"dual_gpu_0000_parquet_to_0019_parquet_cleaned_split_tensors"
            actual_output_dir.mkdir(parents=True, exist_ok=True)
            shortcut_path = log_dir / f"{script_name}_data_shortcut"
            if not shortcut_path.exists() and not shortcut_path.is_symlink():
                try: shortcut_path.symlink_to(actual_output_dir, target_is_directory=True)
                except OSError: pass
        else:
            actual_output_dir = log_dir
    else:
        log_dir = outputs_base_dir
        log_dir.mkdir(parents=True, exist_ok=True)
        actual_output_dir = None

    log_path = log_dir / f"{script_name}_terminal_output.txt"
    sys.stdout = DualLogger(log_path)
    start_time = time.perf_counter()
    
    print("=" * 70)
    print(f"--- STARTING: {script_name.upper()} ---")
    print("=" * 70)

    if not data_dir.exists():
        print(f"[ERROR] Target directory does not exist: {data_dir}")
        sys.stdout = sys.stdout.terminal
        return

    print(f"Scanning {data_dir} for train/val/test parquets...")
    target_files = []
    for split in ['train', 'val', 'test']:
        split_dir = data_dir / split
        if split_dir.exists():
            for f in split_dir.glob("*.parquet"):
                target_files.append((f, split))
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        sys.stdout = sys.stdout.terminal
        return

    # --- Data Accumulators ---
    train_segments = []
    val_segments = []
    test_files_dict = {} # run_id -> list of segments
    
    stats = {
        'train': {'jobs': 0, 'segments': 0, 'rows': 0},
        'val':   {'jobs': 0, 'segments': 0, 'rows': 0},
        'test':  {'jobs': 0, 'segments': 0, 'rows': 0}
    }
    
    overall_success = 0
    overall_fail = 0

    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in target_files}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Parsing Parquets"):
            success, split_name, run_id, file_segments, error_msg = future.result()
            
            if success:
                overall_success += 1
                if file_segments:
                    num_segs = len(file_segments)
                    num_rows = sum(s['valid_len'] for s in file_segments)
                    
                    stats[split_name]['jobs'] += 1
                    stats[split_name]['segments'] += num_segs
                    stats[split_name]['rows'] += num_rows
                    
                    if split_name == 'train':
                        train_segments.extend(file_segments)
                    elif split_name == 'val':
                        val_segments.extend(file_segments)
                    elif split_name == 'test':
                        test_files_dict[run_id] = file_segments
            else:
                overall_fail += 1
                tqdm.write(f"[ERROR] Failed on {run_id}: {error_msg}")

    print("\n" + "=" * 70)
    print("--- GENERATING BRANCHED PYTORCH TENSORS ---")
    print("=" * 70)

    # --- 1. TRAIN: CHUNKED ---
    num_train_segs = len(train_segments)
    num_train_chunks = int(np.ceil(num_train_segs / TRAIN_CHUNK_SIZE))
    print(f"\n[TRAIN] Chunking {num_train_segs} segments into {num_train_chunks} files (Max {TRAIN_CHUNK_SIZE} segs/file)...")
    
    for chunk_idx in range(num_train_chunks):
        start_idx = chunk_idx * TRAIN_CHUNK_SIZE
        end_idx = min((chunk_idx + 1) * TRAIN_CHUNK_SIZE, num_train_segs)
        chunk_data = train_segments[start_idx:end_idx]
        
        td = create_tensor_dict(chunk_data, f"Train Chunk {chunk_idx}")
        out_path = actual_output_dir / f"train_chunk_{chunk_idx:03d}.pt"
        torch.save(td, out_path)
        print(f"  -> Saved {out_path.name} ({out_path.stat().st_size / (1024*1024):.2f} MB)")
        
        del td, chunk_data
        gc.collect()
        
    del train_segments
    gc.collect()

    # --- 2. VAL: SINGLE BUNDLE ---
    num_val_segs = len(val_segments)
    if num_val_segs > 0:
        print(f"\n[VAL] Bundling {num_val_segs} segments into a SINGLE validation file...")
        td = create_tensor_dict(val_segments, "Val Bundle")
        out_path = actual_output_dir / "val_chunk.pt"
        torch.save(td, out_path)
        print(f"  -> Saved {out_path.name} ({out_path.stat().st_size / (1024*1024):.2f} MB)")
        
        del td, val_segments
        gc.collect()

    # --- 3. TEST: 1-to-1 MAPPING ---
    num_test_files = len(test_files_dict)
    if num_test_files > 0:
        print(f"\n[TEST] Preserving physical mapping. Generating {num_test_files} distinct test files...")
        for run_id, file_segs in tqdm(test_files_dict.items(), desc="Writing Test Files"):
            td = create_tensor_dict(file_segs, f"Test {run_id}")
            out_path = actual_output_dir / f"test_{run_id}.pt"
            torch.save(td, out_path)
            
        del test_files_dict
        gc.collect()

    # --- Metadata Compilation & Dual Saving ---
    meta_data = {
        'total_train_jobs': stats['train']['jobs'],
        'total_val_jobs': stats['val']['jobs'],
        'total_test_jobs': stats['test']['jobs'],
        'total_train_segments': stats['train']['segments'],
        'total_val_segments': stats['val']['segments'],
        'total_test_segments': stats['test']['segments'],
        'total_train_rows': stats['train']['rows'],
        'total_val_rows': stats['val']['rows'],
        'total_test_rows': stats['test']['rows'],
        'segment_length': SEGMENT_LEN,
        'train_chunk_size': TRAIN_CHUNK_SIZE
    }

    meta_path_tensors = actual_output_dir / "calibration_metadata.json"
    meta_path_logs = log_dir / "calibration_metadata.json"
    
    with open(meta_path_tensors, 'w') as f: json.dump(meta_data, f, indent=4)
    with open(meta_path_logs, 'w') as f: json.dump(meta_data, f, indent=4)

    # --- Final Summary ---
    end_time = time.perf_counter()
    formatted_time = str(timedelta(seconds=int(end_time - start_time)))

    print("\n" + "=" * 70)
    print("=== DATASET STATISTICS ===")
    print(f"{'Split':<10} | {'Original Jobs':<15} | {'Segments Created':<20} | {'Total Real Rows'}")
    print("-" * 70)
    print(f"{'Train':<10} | {stats['train']['jobs']:<15,} | {stats['train']['segments']:<20,} | {stats['train']['rows']:,}")
    print(f"{'Val':<10} | {stats['val']['jobs']:<15,} | {stats['val']['segments']:<20,} | {stats['val']['rows']:,}")
    print(f"{'Test':<10} | {stats['test']['jobs']:<15,} | {stats['test']['segments']:<20,} | {stats['test']['rows']:,}")
    print("=" * 70)
    print(f"Total Time : {formatted_time}")
    print(f"Outputs saved to: {actual_output_dir}")

    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()