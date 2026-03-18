"""
===============================================================================
Script Name: 03_calculate_pinn_ambient_temperature_prior.py
Description: Calculates the baseline ambient temperature prior (T_amb) for a
             Physics-Informed Neural Network (PINN). Scans for "Deep Idle" 
             events where both GPUs draw minimal power for a sustained period,
             allowing heat sinks to fully cool to the ambient baseline.
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
import matplotlib
# Use Agg backend to prevent GUI thread issues during parallel/background execution
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

# --- 1. CONFIGURATION ---
warnings.filterwarnings('ignore')

N_CORES = 8             
GENERATES_FILES = True  

# --- Domain-Specific Physics Thresholds ---
IDLE_POWER_MAX_W = 40.0     # Power must be strictly below this to be considered "idle"
IDLE_TIME_MIN_SEC = 300     # Minimum consecutive seconds of idle to guarantee heat dissipation (5 mins)


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


# --- 3. HELPER FUNCTION ---
def extract_deep_idle_minimums(df):
    """
    Finds continuous periods where BOTH GPUs are drawing less than IDLE_POWER_MAX_W.
    If the period lasts longer than IDLE_TIME_MIN_SEC, it extracts the lowest temperatures.
    """
    mask = (df['power_draw_gpu_0_W'] < IDLE_POWER_MAX_W) & (df['power_draw_gpu_1_W'] < IDLE_POWER_MAX_W)
    blocks = mask.ne(mask.shift()).cumsum()
    idle_groups = df[mask].groupby(blocks)
    
    valid_mins_0 = []
    valid_mins_1 = []
    
    for _, group in idle_groups:
        if len(group) >= IDLE_TIME_MIN_SEC:
            # The heat sinks have soaked out; grab the absolute minimums in this window
            valid_mins_0.append(group['temperature_gpu_0'].min())
            valid_mins_1.append(group['temperature_gpu_1'].min())
            
    return valid_mins_0, valid_mins_1


# --- 4. WORKER FUNCTION ---
def process_single_item(item_args):
    """
    Worker function to process a single file.
    """
    file_path, output_dir = item_args
    try:
        # 1. Read data
        df = pd.read_parquet(file_path)
        
        # 2. Column Validation
        cols_needed = ['timestamp', 'power_draw_gpu_0_W', 'power_draw_gpu_1_W', 'temperature_gpu_0', 'temperature_gpu_1']
        if not all(col in df.columns for col in cols_needed):
            return False, file_path.name, "Missing required columns."
            
        # 3. Resample & Clean
        df_clean = df[cols_needed].copy().astype('float64')
        df_clean['datetime'] = pd.to_timedelta(df_clean['timestamp'], unit='s')
        df_resampled = df_clean.set_index('datetime').resample('1s').mean().interpolate(method='linear').dropna()

        if len(df_resampled) < IDLE_TIME_MIN_SEC:
            return False, file_path.name, "File shorter than required idle window."

        # 4. Extract Minimums
        mins_0, mins_1 = extract_deep_idle_minimums(df_resampled)
        
        if not mins_0 or not mins_1:
            return False, file_path.name, "No sustained deep-idle periods found."

        # 5. Package Results
        result_data = {
            'filename': file_path.name,
            'min_temp_0': np.min(mins_0),
            'min_temp_1': np.min(mins_1),
            'idle_events_found': len(mins_0)
        }
            
        return True, file_path.name, result_data

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
    
    data_dir = project_root / "data" / "mit-supercloud-dataset" / "gpu"
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
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
    
    # --- START TIMER ---
    start_time = time.perf_counter()
    
    print("=" * 70)
    print(f"--- STARTING: AMBIENT TEMPERATURE EXTRACTION ---")
    print("=" * 70)

    # --- Data Discovery ---
    if not data_dir.exists():
        print(f"[ERROR] Target directory does not exist: {data_dir}")
        sys.stdout = sys.stdout.terminal  # Restore stdout before returning
        return

    print(f"Scanning {data_dir} for target files...")
    
    # Match the regex pattern from your previous scripts
    target_pattern = re.compile(r"dual_gpu_\d{4}_parquet_to_\d{4}_parquet_cleaned$")
    target_files = [p for p in data_dir.rglob("*.parquet") if target_pattern.search(p.parent.name)]
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        sys.stdout = sys.stdout.terminal  # Restore stdout before returning
        return

    # --- Parallel Processing ---
    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    tasks = [(fp, output_dir) for fp in target_files]
    
    success_count = 0
    fail_count = 0
    results_list = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=N_CORES) as executor:
        futures = {executor.submit(process_single_item, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_files, desc="Processing", unit="file"):
            success, filename, payload = future.result()
            
            if success:
                success_count += 1
                results_list.append(payload)
            else:
                fail_count += 1
                if "No sustained deep-idle" not in payload:
                    tqdm.write(f"[ERROR] Failed on {filename}: {payload}")

    # --- Analytics & File Generation ---
    if GENERATES_FILES and results_list:
        df_res = pd.DataFrame(results_list)
        csv_path = output_dir / "ambient_temperature_priors.csv"
        df_res.to_csv(csv_path, index=False)
        
        # Calculate robust baseline (Median is safer than Mean to avoid sensor glitches)
        median_ambient_0 = df_res['min_temp_0'].median()
        median_ambient_1 = df_res['min_temp_1'].median()
        global_ambient = np.median([median_ambient_0, median_ambient_1])
        total_events = df_res['idle_events_found'].sum()

        # --- Terminal Summary ---
        print("\n" + "=" * 70)
        print("=== FINAL PINN AMBIENT PRIOR CONCLUSIONS ===")
        print("=" * 70)
        print(f"Global Computed Ambient Prior (T_amb) : {global_ambient:.2f} °C")
        print(f"  - Median GPU 0 Ambient Baseline     : {median_ambient_0:.2f} °C")
        print(f"  - Median GPU 1 Ambient Baseline     : {median_ambient_1:.2f} °C")
        print(f"  - Deep Idle Events Analyzed         : {total_events} events\n")

        # --- Generate Distribution Graph ---
        sns.set_theme(style="whitegrid", palette="muted")
        plt.figure(figsize=(9, 5))
        
        # Plot distributions
        sns.kdeplot(data=df_res['min_temp_0'], label='GPU 0 Minimums', fill=True, color='#4c72b0', alpha=0.5)
        sns.kdeplot(data=df_res['min_temp_1'], label='GPU 1 Minimums', fill=True, color='#c44e52', alpha=0.5)
        
        # Plot Median lines
        plt.axvline(median_ambient_0, color='#4c72b0', linestyle=':', linewidth=2, label=f'GPU 0 Median = {median_ambient_0:.1f}°C')
        plt.axvline(median_ambient_1, color='#c44e52', linestyle=':', linewidth=2, label=f'GPU 1 Median = {median_ambient_1:.1f}°C')
        
        # Plot Global Prior line
        plt.axvline(global_ambient, color='black', linestyle='--', linewidth=2, label=f'Global Prior (T_amb) = {global_ambient:.1f}°C')
        
        plt.xlabel('Air Temperature (°C)', fontsize=12)
        plt.ylabel('Density', fontsize=12)
        plt.title('Distribution of Ambient Temperature Baselines Across Cluster', fontsize=14)
        
        # Move legend outside or adjust if it gets too crowded
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left') 
        plt.tight_layout()
        plt.savefig(output_dir / 'ambient_distribution_kde.png', dpi=150)
        plt.close()

    # --- END TIMER & CALCULATE ---
    end_time = time.perf_counter()
    elapsed_seconds = end_time - start_time
    formatted_time = str(timedelta(seconds=int(elapsed_seconds)))

    # --- Final Summary ---
    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total processed : {total_files}")
    print(f"Successful      : {success_count}")
    print(f"Discarded/Fail  : {fail_count}")
    print(f"Total Time      : {formatted_time}")
    
    if GENERATES_FILES:
        print(f"\n[!] Outputs, terminal logs, and graphs saved to: {output_dir}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    # Restore standard output just in case
    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()