"""
===============================================================================
Script Name: 02_calculate_pinn_lumped_parameter_thermal_priors.py
Description: Calculates baseline physics priors for a Physics-Informed Neural 
             Network (PINN). Extracts Thermal Capacitance (C), Thermal 
             Resistance (R_th), and Convective Cooling Coefficients (h) 
             by fitting exponential step-response heating curves from dual 
             GPU telemetry logs. Enforces strict thermodynamic boundaries.
             Now also computes the dimensionless thermal crosstalk prior (kappa).
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
from scipy.optimize import curve_fit
from datetime import timedelta
import matplotlib
# Use Agg backend to prevent GUI thread issues during parallel/background execution
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

# --- 1. CONFIGURATION ---
warnings.filterwarnings('ignore')

N_CORES = 6             
GENERATES_FILES = True  

# --- Domain-Specific Physics Thresholds ---
POWER_SPIKE_W = 100.0     # Power threshold to define a "heavy workload" start
CURVE_WINDOW_SEC = 150    # Maximum seconds of data to capture for the heating curve
MIN_T_AMP = 3.0           # Minimum temperature rise required to perform a valid curve fit

# --- Crosstalk Coefficients from Script 01 ---
K_01_COEFFICIENT = 0.01602      # °C/W (Heat Flow: GPU 1 -> GPU 0)
K_10_COEFFICIENT = 0.00522      # °C/W (Heat Flow: GPU 0 -> GPU 1)


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


# --- 3. HELPER FUNCTION: CURVE FITTING ---
def extract_capacitance(df, power_col, temp_col):
    """
    Identifies a heating phase, fits an exponential curve with STRICT PHYSICS BOUNDS, 
    and extracts C, R_th, and h. Returns a dictionary of metrics or None if invalid.
    """
    # 1. Find where power spikes above threshold
    spike_indices = df[df[power_col] > POWER_SPIKE_W].index
    if len(spike_indices) == 0:
        return None
    
    start_idx = spike_indices.min()
    
    # 2. Sustained Workload Check (Lookahead)
    early_power_mean = df[power_col].iloc[start_idx : min(start_idx + 15, len(df))].mean()
    if early_power_mean < (POWER_SPIKE_W * 0.75):
        return None

    # 3. Dynamic Windowing
    end_idx = start_idx
    for i in range(start_idx, min(start_idx + CURVE_WINDOW_SEC, len(df))):
        if df[power_col].iloc[i] < (POWER_SPIKE_W * 0.5): # Power dropped below 50W
            break
        end_idx = i
        
    # We need at least 30 seconds of sustained heat to accurately calculate tau
    if (end_idx - start_idx) < 30:
        return None

    t_heat = df.index.values[start_idx:end_idx].astype(float)
    t_heat = t_heat - t_heat[0]  # Time is already in seconds, start at t=0
    T_heat = df[temp_col].iloc[start_idx:end_idx].values
    
    # Calculate Steady-State Variables
    T_idle = df[temp_col].iloc[:start_idx].mean() if start_idx > 0 else T_heat[0]
    P_idle = df[power_col].iloc[:start_idx].mean() if start_idx > 0 else df[power_col].min()
    P_heat = df[power_col].iloc[start_idx:end_idx].mean()
    
    # Ensure a valid power delta exists to prevent division by zero
    if (P_heat - P_idle) <= 0 or pd.isna(T_idle):
        return None

    def heating_curve(t, T_amp, tau):
        return T_idle + T_amp * (1 - np.exp(-t / tau))
    
    try:
        # Initial guesses: T_amp = 10 degrees, tau = 30 seconds
        p0 = [10.0, 30.0]  
        
        # Thermodynamic Bounds
        physics_bounds = ([MIN_T_AMP, 5.0], [100.0, 400.0])
        
        # Pass the bounds into the curve_fit algorithm
        popt, _ = curve_fit(heating_curve, t_heat, T_heat, p0=p0, bounds=physics_bounds, maxfev=2000)
        
        T_amp_fit, tau_fit = popt
        
        # Boundary Collision Check
        if tau_fit <= 5.01 or tau_fit >= 399.9:
            return None
            
        R_th = T_amp_fit / (P_heat - P_idle)
        C = tau_fit / R_th
        h = 1.0 / R_th  # Convective Cooling Coefficient (W/°C)
        
        # Final Sanity Check: Capacitance between 20 and 1000 Joules/Celsius
        if not (20.0 <= C <= 1000.0):
            return None
        
        return {
            'tau': tau_fit,
            'R_th': R_th,
            'C': C,
            'h': h,
            'T_amp': T_amp_fit,
            'P_delta': P_heat - P_idle
        }
    except Exception:
        return None


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
        df_resampled = df_clean.set_index('datetime').resample('1s').mean().interpolate(method='linear').dropna().reset_index(drop=True)

        metrics_0 = extract_capacitance(df_resampled, 'power_draw_gpu_0_W', 'temperature_gpu_0')
        metrics_1 = extract_capacitance(df_resampled, 'power_draw_gpu_1_W', 'temperature_gpu_1')

        if not metrics_0 and not metrics_1:
             return False, file_path.name, "No valid step-response curves found."

        result_data = {
            'filename': file_path.name,
            'C_0': metrics_0['C'] if metrics_0 else np.nan,
            'tau_0': metrics_0['tau'] if metrics_0 else np.nan,
            'R_th_0': metrics_0['R_th'] if metrics_0 else np.nan,
            'h_0': metrics_0['h'] if metrics_0 else np.nan,
            'T_amp_0': metrics_0['T_amp'] if metrics_0 else np.nan,
            
            'C_1': metrics_1['C'] if metrics_1 else np.nan,
            'tau_1': metrics_1['tau'] if metrics_1 else np.nan,
            'R_th_1': metrics_1['R_th'] if metrics_1 else np.nan,
            'h_1': metrics_1['h'] if metrics_1 else np.nan,
            'T_amp_1': metrics_1['T_amp'] if metrics_1 else np.nan,
        }
            
        return True, file_path.name, result_data

    except Exception as e:
        return False, file_path.name, str(e)


# --- 5. MAIN EXECUTION ---
def main():
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    project_root = script_path.parent.parent.parent
    scripts_dir = script_path.parent.parent
    
    data_dir = project_root / "data" / "mit-supercloud-dataset" / "gpu"
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
    if GENERATES_FILES:
        output_dir = outputs_base_dir / f"{script_name}_output"
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = outputs_base_dir
        output_dir.mkdir(parents=True, exist_ok=True)

    log_path = output_dir / f"{script_name}_terminal_output.txt"
    sys.stdout = DualLogger(log_path)
    
    # --- START TIMER ---
    start_time = time.perf_counter()
    
    print("=" * 70)
    print(f"--- STARTING: THERMAL PRIORS EXTRACTION ---")
    print("=" * 70)

    if not data_dir.exists():
        print(f"[ERROR] Target directory does not exist: {data_dir}")
        sys.stdout = sys.stdout.terminal  # Restore stdout before returning
        return

    print(f"Scanning {data_dir} for target files...")
    target_pattern = re.compile(r"dual_gpu_\d{4}_parquet_to_\d{4}_parquet_cleaned$")
    target_files = [p for p in data_dir.rglob("*.parquet") if target_pattern.search(p.parent.name)]
    
    total_files = len(target_files)
    if total_files == 0:
        print("[!] No target files found. Exiting.")
        sys.stdout = sys.stdout.terminal  # Restore stdout before returning
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
                if "No valid step-response curves" not in payload:
                     tqdm.write(f"[ERROR] Failed on {filename}: {payload}")

    if GENERATES_FILES and results_list:
        df_res = pd.DataFrame(results_list)
        csv_path = output_dir / "thermal_priors.csv"
        df_res.to_csv(csv_path, index=False)
        
        # --- CALCULATE MEDIAN PRIORS ---
        med_C0 = df_res['C_0'].median(skipna=True)
        med_C1 = df_res['C_1'].median(skipna=True)

        med_Rth0 = df_res['R_th_0'].median(skipna=True)
        med_Rth1 = df_res['R_th_1'].median(skipna=True)
        
        med_h0 = df_res['h_0'].median(skipna=True)
        med_h1 = df_res['h_1'].median(skipna=True)

        valid_C0_count = df_res['C_0'].notna().sum()
        valid_C1_count = df_res['C_1'].notna().sum()

        # --- CALCULATE DIMENSIONLESS CROSSTALK PRIOR (kappa) ---
        # Formula: kappa_ij = k_ij_coefficient * h_i
        kappa_01_prior = K_01_COEFFICIENT * med_h0
        kappa_10_prior = K_10_COEFFICIENT * med_h1

        # Calculate Average Capacitance (C)
        avg_C = (med_C0 + med_C1) / 2

        # --- TERMINAL CONCLUSIONS ---
        print("\n" + "=" * 70)
        print("=== FINAL PINN THERMAL PRIORS CONCLUSIONS ===")
        print("=" * 70)

        print("GPU 0 (DOWNSTREAM) Thermal Characteristics")
        print(f"  - Capacitance Prior (C_0)              : {med_C0:.2f} J/°C")
        print(f"  - Resistance Prior (R_th_0)            : {med_Rth0:.4f} °C/W")
        print(f"  - Convective Cooling Prior (h_0)       : {med_h0:.4f} W/°C")
        print(f"  - Dimensionless Crosstalk Prior (κ_01) : {kappa_01_prior:.6f} (Unitless)")
        print(f"  - Valid Curves Fit                     : {valid_C0_count} files\n")

        print("GPU 1 (UPSTREAM) Thermal Characteristics")
        print(f"  - Capacitance Prior (C_1)              : {med_C1:.2f} J/°C")
        print(f"  - Resistance Prior (R_th_1)            : {med_Rth1:.4f} °C/W")
        print(f"  - Convective Cooling Prior (h_1)       : {med_h1:.4f} W/°C")
        print(f"  - Dimensionless Crosstalk Prior (κ_10) : {kappa_10_prior:.6f} (Unitless)")
        print(f"  - Valid Curves Fit                     : {valid_C1_count} files")

        print(f"\nAVERAGE CLUSTER CAPACITANCE (C)          : {avg_C:.2f} J/°C\n")

        # --- GENERATE DISTRIBUTION GRAPH ---
        sns.set_theme(style="whitegrid", palette="muted")
        plt.figure(figsize=(10, 6))
        
        if valid_C0_count > 0:
             sns.kdeplot(data=df_res['C_0'].dropna(), label='C_0 (GPU 0)', fill=True, color='#4c72b0', alpha=0.5)
        if valid_C1_count > 0:
             sns.kdeplot(data=df_res['C_1'].dropna(), label='C_1 (GPU 1)', fill=True, color='#c44e52', alpha=0.5)
             
        plt.xlabel('Thermal Capacitance (Joules/°C)', fontsize=12)
        plt.ylabel('Density', fontsize=12)
        plt.title('Distribution of Capacitance Across Dataset', fontsize=14)
        plt.legend()
        plt.tight_layout()
        plt.savefig(output_dir / 'capacitance_distribution_kde.png', dpi=150)
        plt.close()

        # --- GENERATE MASTER CLUSTER HEATING CURVE ---
        med_tau_0 = df_res['tau_0'].median()
        med_tamp_0 = df_res['T_amp_0'].median()
        med_tau_1 = df_res['tau_1'].median()
        med_tamp_1 = df_res['T_amp_1'].median()

        t_ideal = np.linspace(0, 150, 150)
        
        plt.figure(figsize=(10, 6))
        
        if pd.notna(med_tau_0) and pd.notna(med_tamp_0):
            T_ideal_0 = med_tamp_0 * (1 - np.exp(-t_ideal / med_tau_0))
            plt.plot(t_ideal, T_ideal_0, label=f'GPU 0 Master Curve (tau={med_tau_0:.1f}s)', color='#4c72b0', linewidth=3)
            
        if pd.notna(med_tau_1) and pd.notna(med_tamp_1):
            T_ideal_1 = med_tamp_1 * (1 - np.exp(-t_ideal / med_tau_1))
            plt.plot(t_ideal, T_ideal_1, label=f'GPU 1 Master Curve (tau={med_tau_1:.1f}s)', color='#c44e52', linewidth=3, linestyle='--')

        plt.xlabel('Time (Seconds)', fontsize=12)
        plt.ylabel('Temperature Rise (°C)', fontsize=12)
        plt.title('Idealized Step-Response Master Curve (Cluster Median)', fontsize=14)
        plt.grid(True, linestyle=':', alpha=0.7)
        plt.legend()
        plt.tight_layout()
        plt.savefig(output_dir / 'cluster_master_heating_curve.png', dpi=150)
        plt.close()

        # --- GENERATE BAR CHARTS ---
        sns.set_theme(style="whitegrid")

        # 1. Thermal Capacitance Bar Chart
        plt.figure(figsize=(7, 6))
        bars_C = plt.bar(['GPU 0\n(Downstream)', 'GPU 1\n(Upstream)'], [med_C0, med_C1], color=['#4c72b0', '#c44e52'], width=0.5)
        plt.ylabel('Thermal Capacitance (J/°C)', fontsize=12)
        plt.title('Median Thermal Capacitance ($C$) Prior', fontsize=14, fontweight='bold')
        for bar in bars_C:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval + (yval * 0.02), f'{yval:.2f}', ha='center', va='bottom', fontsize=12, fontweight='bold')
        plt.ylim(0, max(med_C0, med_C1) * 1.15) 
        plt.tight_layout()
        plt.savefig(output_dir / 'capacitance_bar_chart.png', dpi=150)
        plt.close()

        # 2. Thermal Resistance Bar Chart
        plt.figure(figsize=(7, 6))
        bars_R = plt.bar(['GPU 0\n(Downstream)', 'GPU 1\n(Upstream)'], [med_Rth0, med_Rth1], color=['#4c72b0', '#c44e52'], width=0.5)
        plt.ylabel('Thermal Resistance (°C/W)', fontsize=12)
        plt.title('Median Thermal Resistance ($R_{th}$) Prior', fontsize=14, fontweight='bold')
        for bar in bars_R:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval + (yval * 0.02), f'{yval:.4f}', ha='center', va='bottom', fontsize=12, fontweight='bold')
        plt.ylim(0, max(med_Rth0, med_Rth1) * 1.15)
        plt.tight_layout()
        plt.savefig(output_dir / 'resistance_bar_chart.png', dpi=150)
        plt.close()

        # 3. Convective Cooling Coefficient Bar Chart
        plt.figure(figsize=(7, 6))
        bars_h = plt.bar(['GPU 0\n(Downstream)', 'GPU 1\n(Upstream)'], [med_h0, med_h1], color=['#4c72b0', '#c44e52'], width=0.5)
        plt.ylabel('Convective Cooling Coefficient (W/°C)', fontsize=12)
        plt.title('Median Convective Cooling ($h$) Prior', fontsize=14, fontweight='bold')
        for bar in bars_h:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval + (yval * 0.02), f'{yval:.4f}', ha='center', va='bottom', fontsize=12, fontweight='bold')
        plt.ylim(0, max(med_h0, med_h1) * 1.15)
        plt.tight_layout()
        plt.savefig(output_dir / 'convective_cooling_bar_chart.png', dpi=150)
        plt.close()
        
        # 4. Dimensionless Crosstalk Bar Chart
        plt.figure(figsize=(7, 6))
        bars_kappa = plt.bar([r'$\kappa_{01}$' + '\n(GPU 1 -> GPU 0)', r'$\kappa_{10}$' + '\n(GPU 0 -> GPU 1)'], 
                             [kappa_01_prior, kappa_10_prior], 
                             color=['#4c72b0', '#c44e52'], width=0.5)
        plt.ylabel(r'Dimensionless Crosstalk ($\kappa$)', fontsize=12)
        plt.title(r'Dimensionless Thermal Crosstalk ($\kappa$) Prior', fontsize=14, fontweight='bold')
        for bar in bars_kappa:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval + (yval * 0.02), f'{yval:.5f}', ha='center', va='bottom', fontsize=12, fontweight='bold')
        plt.ylim(0, max(kappa_01_prior, kappa_10_prior) * 1.15)
        plt.tight_layout()
        plt.savefig(output_dir / 'dimensionless_crosstalk_bar_chart.png', dpi=150)
        plt.close()

    # --- END TIMER & CALCULATE ---
    end_time = time.perf_counter()
    elapsed_seconds = end_time - start_time
    formatted_time = str(timedelta(seconds=int(elapsed_seconds)))

    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total files scanned : {total_files}")
    print(f"Curves extracted    : {success_count}")
    print(f"Discarded/Failed    : {fail_count}")
    print(f"Total Time          : {formatted_time}")
    
    if GENERATES_FILES:
        print(f"\n[!] Outputs, terminal logs, and graphs saved to: {output_dir}")
    else:
        print(f"\n[!] Terminal logs saved to: {log_path}")

    sys.stdout = sys.stdout.terminal

if __name__ == "__main__":
    main()