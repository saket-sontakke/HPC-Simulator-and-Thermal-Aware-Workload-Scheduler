"""
===============================================================================
Script Name: 05_split_diagnostic_viewer.py
Description: A comprehensive, read-only diagnostic script that performs a dry-run 
             of the Global 2D Stratified split. This version uses Categorical 
             Ordering to ensure terminal output matches the logical matrix flow.
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
import re

# --- 1. CONFIGURATION ---
warnings.filterwarnings('ignore')

N_CORES = 8             
GENERATES_FILES = False 
CREATE_DATA_SYMLINK = False 

TRAIN_FRAC = 0.80
VAL_FRAC = 0.10
TEST_FRAC = 0.10
RANDOM_SEED = 42

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
    file_path, _ = item_args
    try:
        df = pd.read_parquet(file_path, columns=['excited'])
        total_rows = len(df)
        if total_rows == 0:
            return False, file_path.name, "File is empty."
            
        density = df['excited'].sum() / total_rows
        payload = {
            'file_name': file_path.name,
            'total_rows': total_rows,
            'density': density
        }
        return True, file_path.name, payload
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
    
    log_dir = outputs_base_dir / f"{script_name}_output"
    log_dir.mkdir(parents=True, exist_ok=True)
    
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

    pattern = re.compile(r"dual_gpu_\d{4}_parquet_to_\d{4}_parquet_cleaned$")
    cleaned_dirs = [d for d in data_dir.iterdir() if d.is_dir() and pattern.match(d.name)]
    
    target_files = []
    for d in cleaned_dirs:
        target_files.extend(list(d.glob("*.parquet")))
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        sys.stdout = sys.stdout.terminal
        return
        
    print(f"Phase 1: Analyzing {total_files} files metadata...\n")
    tasks = [(fp, None) for fp in target_files]
    metadata_list = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing"):
            success, _, payload = future.result()
            if success: metadata_list.append(payload)

    if not metadata_list:
        sys.stdout = sys.stdout.terminal
        return

    print("PHASE 2: STRATIFICATION ANALYTICS & MATRIX")
    
    df_meta = pd.DataFrame(metadata_list)
    
    # 1. Calculate Bins with Explicit Ordering
    len_labels = ['Short', 'Medium', 'Long']
    den_labels = ['Low', 'Medium', 'High']
    
    df_meta['len_bin'], len_edges = pd.qcut(df_meta['total_rows'], q=3, labels=len_labels, retbins=True, duplicates='drop')
    df_meta['den_bin'], den_edges = pd.qcut(df_meta['density'], q=3, labels=den_labels, retbins=True, duplicates='drop')
    
    # Force categorical order so sorting follows Short -> Medium -> Long
    df_meta['len_bin'] = pd.Categorical(df_meta['len_bin'], categories=len_labels, ordered=True)
    df_meta['den_bin'] = pd.Categorical(df_meta['den_bin'], categories=den_labels, ordered=True)
    
    df_meta['strata'] = df_meta['len_bin'].astype(str) + "_" + df_meta['den_bin'].astype(str)

    # 2. Show the 2D Matrix
    print("\n[ C ] THE 2D STRATIFICATION MATRIX")
    print("-" * 40)
    matrix = pd.crosstab(df_meta['len_bin'], df_meta['den_bin'], margins=True, margins_name="Total")
    print(matrix.to_string())

    # 3. Deterministic Split & Tagging
    df_meta['assigned_split'] = 'Unassigned'
    
    # Create the sorted list of strata based on your requested sequence
    ordered_strata = [f"{l}_{d}" for l in len_labels for d in den_labels]
    
    print("\n[ D ] DETAILED 80/10/10 ROUTING LOGIC (LOGICAL SEQUENCE)")
    print("-" * 65)
    print(f"{'STRATA NAME':<15} | {'TOTAL':<6} | {'TRAIN (80%)':<11} | {'VAL (10%)':<9} | {'TEST (10%)':<9}")
    print("-" * 65)
    
    totals = {'train': 0, 'val': 0, 'test': 0}

    # Iterate through our specifically ordered list
    for s_name in ordered_strata:
        group = df_meta[df_meta['strata'] == s_name]
        if group.empty: continue
        
        shuffled = group.sample(frac=1.0, random_state=RANDOM_SEED)
        n = len(shuffled)
        n_train, n_val = int(TRAIN_FRAC * n), int(VAL_FRAC * n)
        n_test = n - n_train - n_val 
        
        df_meta.loc[shuffled.iloc[:n_train].index, 'assigned_split'] = 'Train'
        df_meta.loc[shuffled.iloc[n_train:n_train+n_val].index, 'assigned_split'] = 'Val'
        df_meta.loc[shuffled.iloc[n_train+n_val:].index, 'assigned_split'] = 'Test'
        
        totals['train'] += n_train
        totals['val'] += n_val
        totals['test'] += n_test

        print(f"{s_name:<15} | {n:<6} | {n_train:<11} | {n_val:<9} | {n_test:<9}")

    print("-" * 65)
    print(f"{'GRAND TOTAL':<15} | {len(df_meta):<6} | {totals['train']:<11} | {totals['val']:<9} | {totals['test']:<9}")

    # 4. Edge Case Traceability
    print("\n[ E ] EDGE CASE TRACEABILITY")
    print("-" * 80)
    print(f"{'METRIC EXTREME':<15} | {'VALUE':<10} | {'STRATA':<15} | {'SET':<6} | {'FILE NAME'}")
    print("-" * 80)

    extremes = [
        ("Min Rows", df_meta['total_rows'].min(), 'total_rows', False),
        ("Max Rows", df_meta['total_rows'].max(), 'total_rows', False),
        ("Min Density", df_meta['density'].min(), 'density', True),
        ("Max Density", df_meta['density'].max(), 'density', True)
    ]

    for label, val, col, is_float in extremes:
        row = df_meta[df_meta[col] == val].iloc[0]
        v_str = f"{val:.4f}" if is_float else str(val)
        print(f"{label:<15} | {v_str:<10} | {row['strata']:<15} | {row['assigned_split']:<6} | {row['file_name']}")

    end_time = time.perf_counter()
    print("-" * 80)
    print(f"\nExecution Complete in: {str(timedelta(seconds=int(end_time - start_time)))}")
    print(f"[*] Logs saved to: {log_path}")

    sys.stdout = sys.stdout.terminal

if __name__ == "__main__":
    main()