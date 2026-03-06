"""
===============================================================================
Script Name: calculate_pinn_thermal_crosstalk_priors.py
Description: Calculates thermal crosstalk coefficients (k_01 and k_10) for a 
             Physics-Informed Neural Network (PINN). It discards multicollinear 
             noise by strictly browsing for "Asymmetric Events" - periods where 
             one GPU is under heavy load while the adjacent GPU remains idle 
             for a sustained period, isolating the pure thermal bleed.
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
import matplotlib.pyplot as plt
import seaborn as sns

# --- 1. CONFIGURATION ---
warnings.filterwarnings('ignore')

N_CORES = 8             
GENERATES_FILES = True  

# --- Domain-Specific Physics Thresholds ---
STRESS_POWER_MIN_W = 150.0  
IDLE_POWER_MAX_W = 50.0     
STEADY_STATE_SEC = 30       
MIN_DELTA_T = 1.0           


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


# --- 3. HELPER FUNCTION ---
def extract_asymmetric_events(df, active_p_col, idle_p_col, idle_t_col):
    """
    Scans a 1-second resampled dataframe for continuous blocks where one GPU is hot 
    and the other is idle. Calculates crosstalk, delta T, and power for each event.
    """
    mask = (df[active_p_col] > STRESS_POWER_MIN_W) & (df[idle_p_col] < IDLE_POWER_MAX_W)
    blocks = mask.ne(mask.shift()).cumsum()
    groups = df[mask].groupby(blocks)
    
    events = []
    
    for _, group in groups:
        if len(group) >= STEADY_STATE_SEC:
            baseline_temp = group[idle_t_col].iloc[0]
            max_temp = group[idle_t_col].max()
            delta_t = max_temp - baseline_temp
            
            if delta_t >= MIN_DELTA_T:
                avg_active_power = group[active_p_col].mean()
                k_event = delta_t / avg_active_power
                
                # We now store a dictionary to capture the raw physics components
                events.append({
                    'k': k_event,
                    'delta_t': delta_t,
                    'power': avg_active_power
                })
                
    return events


# --- 4. WORKER FUNCTION ---
def process_single_item(item_args):
    file_path, output_dir = item_args
    try:
        df = pd.read_parquet(file_path)
        
        cols_needed = ['timestamp', 'power_draw_gpu_0_W', 'power_draw_gpu_1_W', 'temperature_gpu_0', 'temperature_gpu_1']
        if not all(col in df.columns for col in cols_needed):
            return False, file_path.name, "Missing required columns."
            
        df_clean = df[cols_needed].copy().astype('float64')
        df_clean['datetime'] = pd.to_timedelta(df_clean['timestamp'], unit='s')
        df_resampled = df_clean.set_index('datetime').resample('1s').mean().interpolate(method='linear').dropna()

        if len(df_resampled) < 100:
            return False, file_path.name, "Insufficient data points after resampling."

        # Extract events
        k01_events = extract_asymmetric_events(
            df_resampled, active_p_col='power_draw_gpu_1_W', idle_p_col='power_draw_gpu_0_W', idle_t_col='temperature_gpu_0'
        )
        
        k10_events = extract_asymmetric_events(
            df_resampled, active_p_col='power_draw_gpu_0_W', idle_p_col='power_draw_gpu_1_W', idle_t_col='temperature_gpu_1'
        )

        # Format result payload with new physics metrics
        result_data = {
            'filename': file_path.name,
            'k01_mean': np.mean([e['k'] for e in k01_events]) if k01_events else np.nan,
            'k10_mean': np.mean([e['k'] for e in k10_events]) if k10_events else np.nan,
            'k01_dt_mean': np.mean([e['delta_t'] for e in k01_events]) if k01_events else np.nan,
            'k10_dt_mean': np.mean([e['delta_t'] for e in k10_events]) if k10_events else np.nan,
            'k01_power_mean': np.mean([e['power'] for e in k01_events]) if k01_events else np.nan,
            'k10_power_mean': np.mean([e['power'] for e in k10_events]) if k10_events else np.nan,
            'k01_event_count': len(k01_events),
            'k10_event_count': len(k10_events),
            'is_valid_k01': bool(len(k01_events) > 0),
            'is_valid_k10': bool(len(k10_events) > 0)
        }
            
        return True, file_path.name, result_data

    except Exception as e:
        return False, file_path.name, str(e)


# --- 5. MAIN EXECUTION ---
def main():
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    base_path = script_path.parent.parent
    data_dir = base_path / "data" / "mit-supercloud-dataset" / "gpu"
    outputs_base_dir = script_path.parent / "outputs"
    
    if GENERATES_FILES:
        output_dir = outputs_base_dir / f"{script_name}_output"
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = outputs_base_dir
        output_dir.mkdir(parents=True, exist_ok=True)

    log_path = output_dir / f"{script_name}_terminal_output.txt"
    sys.stdout = DualLogger(log_path)
    
    print("=" * 70)
    print(f"--- STARTING: {script_name.upper()} ---")
    print("=" * 70)

    if not data_dir.exists():
        print(f"[ERROR] Target directory does not exist: {data_dir}")
        return

    print(f"Scanning {data_dir} for target files...")
    target_pattern = re.compile(r"dual_gpu_\d{4}_parquet_to_\d{4}_parquet_cleaned")
    target_files = [p for p in data_dir.rglob("*.parquet") if target_pattern.search(p.parent.name)]
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        return

    print(f"Starting parallel processing on {total_files} files using {N_CORES} cores...\n")
    
    tasks = [(fp, output_dir) for fp in target_files]
    success_count = fail_count = 0
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
                tqdm.write(f"[ERROR] Failed on {filename}: {payload}")

    if GENERATES_FILES and results_list:
        df_res = pd.DataFrame(results_list)
        csv_path = output_dir / "thermal_crosstalk_asymmetric_priors.csv"
        df_res.to_csv(csv_path, index=False)
        
        # --- TERMINAL CONCLUSIONS ---
        print("\n" + "=" * 70)
        print("=== FINAL PINN PRIORS & CROSSTALK CONCLUSIONS ===")
        print("=" * 70)
        
        avg_k01 = df_res['k01_mean'].mean(skipna=True)
        avg_k10 = df_res['k10_mean'].mean(skipna=True)
        total_k01_events = df_res['k01_event_count'].sum()
        total_k10_events = df_res['k10_event_count'].sum()

        print("k_10 (Heat Flow: GPU 0 -> GPU 1)")
        print(f"  - Crosstalk Prior (k_10)     : {avg_k10:.5f} °C/W")
        print(f"  - Isolated Events Analyzed   : {total_k10_events} events\n")

        print("k_01 (Heat Flow: GPU 1 -> GPU 0)")
        print(f"  - Crosstalk Prior (k_01)     : {avg_k01:.5f} °C/W")
        print(f"  - Isolated Events Analyzed   : {total_k01_events} events")
        
        # --- GENERATE GRAPHS ---
        sns.set_theme(style="whitegrid", palette="muted")
        
        # Graph 1: Bar Chart of Averages (Existing)
        plt.figure(figsize=(8, 6))
        plt.grid(False)
        bars = plt.bar(['k_10\n(GPU 0 heats GPU 1)', 'k_01\n(GPU 1 heats GPU 0)'], 
                       [avg_k10 if pd.notna(avg_k10) else 0, avg_k01 if pd.notna(avg_k01) else 0], 
                       color=['#4c72b0', '#c44e52'])
        plt.ylabel('Thermal Crosstalk Coefficient (°C/W)', fontsize=12)
        plt.title('Average Thermal Crosstalk (Asymmetric Events)', fontsize=14)
        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval + (yval*0.02), f"{yval:.5f} °C/W", ha='center', fontsize=11)
        plt.tight_layout()
        plt.savefig(output_dir / '1_bar_chart_asymmetric_priors.png', dpi=150)
        plt.close()

        # Graph 2: Distribution (KDE) Plot
        plt.figure(figsize=(10, 6))
        sns.kdeplot(data=df_res['k10_mean'].dropna(), label='k_10 (GPU 0 heats GPU 1)', fill=True, color='#4c72b0', alpha=0.5)
        sns.kdeplot(data=df_res['k01_mean'].dropna(), label='k_01 (GPU 1 heats GPU 0)', fill=True, color='#c44e52', alpha=0.5)
        plt.xlabel('Crosstalk Coefficient (°C/W)', fontsize=12)
        plt.ylabel('Density', fontsize=12)
        plt.title('Distribution of Crosstalk Coefficients Across Files', fontsize=14)
        plt.legend()
        plt.tight_layout()
        plt.savefig(output_dir / '2_distribution_crosstalk_kde.png', dpi=150)
        plt.close()

        # Graph 3: Scatter Plot (Delta T vs Active Power)
        plt.figure(figsize=(10, 6))
        sns.scatterplot(x='k10_power_mean', y='k10_dt_mean', data=df_res, color='#4c72b0', alpha=0.6, label='GPU 0 -> GPU 1')
        sns.scatterplot(x='k01_power_mean', y='k01_dt_mean', data=df_res, color='#c44e52', alpha=0.6, label='GPU 1 -> GPU 0')
        plt.xlabel('Average Active GPU Power (W)', fontsize=12)
        plt.ylabel('Idle GPU Temperature Rise (°C)', fontsize=12)
        plt.title('Heat Bleed: Temperature Rise vs. Active Power', fontsize=14)
        plt.legend()
        plt.tight_layout()
        plt.savefig(output_dir / '3_scatter_delta_t_vs_power.png', dpi=150)
        plt.close()
        
        # Graph 4: Event Frequency Comparison
        plt.figure(figsize=(8, 6))
        plt.bar(['GPU 0 Heating GPU 1', 'GPU 1 Heating GPU 0'], 
                [total_k10_events, total_k01_events], 
                color=['#4c72b0','#c44e52', ])
        plt.ylabel('Total Asymmetric Events Found', fontsize=12)
        plt.title('Frequency of Directional Thermal Bleed Events', fontsize=14)
        plt.tight_layout()
        plt.savefig(output_dir / '4_bar_chart_event_counts.png', dpi=150)
        plt.close()

    # --- Final Summary ---
    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total processed : {total_files}")
    print(f"Successful      : {success_count}")
    print(f"Failed          : {fail_count}")
    
    if GENERATES_FILES:
        print(f"\n[!] Outputs, terminal logs, and 4 graphs saved to: {output_dir}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    sys.stdout = sys.stdout.terminal

if __name__ == "__main__":
    main()