"""
===============================================================================
Script Name: 01_thermal_ode_simulator.py
Description: A high-performance, JIT-compiled thermodynamic simulator and 
             cluster scheduler. This script ingests GPU power traces, simulates 
             heat dissipation using a system of Ordinary Differential Equations 
             (ODEs), and dynamically schedules jobs using either a standard 
             queue or a thermal-aware predictive policy. It provides granular
             telemetry and aggregates precise job-level completion metrics.
===============================================================================
"""

import os
import sys
import time
import shutil
import json
import zipfile
import datetime
import copy
import pickle
import math
from pathlib import Path
from collections import OrderedDict, deque
import numpy as np
import pandas as pd
from tqdm import tqdm
from numba import njit
from concurrent.futures import ProcessPoolExecutor, as_completed

# =============================================================================
# 1. SIMULATION CONFIGURATION & CONSTANTS
# =============================================================================
class Config:
    """
    USER CONFIGURATION
    ------------------
    Adjust the variables below to set up your simulation environment.
    """

    # --- Simulation Scale & Environment ---
    
    # List of integers. Defines the number of nodes to simulate. 
    # Provide a single value (e.g., [16]) or multiple for batch runs (e.g., [64, 128, 256]).
    NODES = [2, 4, 8, 16, 32, 64, 128, 256] 
    
    # Integer. Ambient starting temperature of the datacenter (Celsius).
    # Valid range: 20 to 45.
    AMBIENT_TEMP_C = 30 

    # Integer or Float. Defines the cooling efficiency of the datacenter (100 = Original MIT Supercloud).
    # Lowering this simulates constrained cooling environments (e.g., 20 = 80% reduction in cooling).
    COOLING_EFFICIENCY_PCT = 100
    
    # --- Execution & Output Modes ---
    
    # String. The scheduling policy to execute. 
    # Options: 'STANDARD', 'THERMAL_AWARE', or 'AB_TESTING' (runs both for comparison).
    MODE = 'AB_TESTING'
    
    # String. Visual theme for the generated interactive HTML telemetry graphs.
    # Options: 'light' or 'dark'.
    THEME = 'light'
    
    # --- Directory Paths ---
    
    # String. Absolute path pointing to your input dataset (CSV or Parquet files).
    INPUT_DIR = r'C:\Users\Saket Sontakke\Documents\PROJECTS\Capstone\Implementation\data\mit-supercloud-dataset\labelled_jobs_single_gpu_csv' 
    
    # String. Absolute path where simulation result folder will be saved.
    OUTPUT_DIR = r'C:\Users\Saket Sontakke\Documents\PROJECTS\Capstone\Implementation\scripts\outputs\04_simulations\cooling_pct_100'
    
    # --- Performance & Telemetry Settings ---
    
    # Boolean. Controls the generation of massive data artifacts.
    # True: Saves chunked CSV telemetry for every node and generates interactive HTML graphs.
    # False: Disables all granular file I/O and HTML generation. Drastically improves 
    #        simulation speed and eliminates CPU/Disk overhead for large runs. Final 
    #        metadata (JSON) and summary (CSV) files are still generated.
    EXPORT_GRANULAR_TELEMETRY = False
    
    # Boolean. Controls telemetry output resolution (only applies if EXPORT_GRANULAR_TELEMETRY is True).
    # True: Exports data at original 100ms intervals (creates exceptionally large files).
    # False: Samples the data every ~5.5s to save disk space.
    HIGH_RES_TELEMETRY = False 
    
    # Integer or None. Restricts the number of CPU threads used for parallel processing.
    # Set to None to automatically utilize all available system cores.
    NUM_CORES = None
    
    # Boolean. Memory management strategy for the simulation traces.
    # True: Preloads all binary traces into system RAM prior to simulation. 
    #       Yields highest performance for datasets that fit entirely in memory.
    # False: Utilizes memory-mapped (mmap) file access to stream traces on-demand. 
    #        Minimizes RAM footprint, essential for extremely large datasets.
    PRELOAD_TO_RAM = True 

    @classmethod
    def validate(cls):
        valid_modes = ['STANDARD', 'THERMAL_AWARE', 'AB_TESTING']
        if cls.MODE not in valid_modes:
            raise ValueError(f"FATAL: Invalid MODE '{cls.MODE}'.")
        if not isinstance(cls.NODES, list) or len(cls.NODES) == 0:
            raise TypeError(f"FATAL: NODES must be a non-empty Python list.")
        if not isinstance(cls.AMBIENT_TEMP_C, int) or type(cls.AMBIENT_TEMP_C) is bool:
            raise TypeError(f"FATAL: AMBIENT_TEMP_C must be a whole number (integer).")
        if not (20 <= cls.AMBIENT_TEMP_C <= 45):
            raise ValueError(f"FATAL: AMBIENT_TEMP_C must be between 20 °C and 45 °C.")
        valid_themes = ['dark', 'light']
        if cls.THEME not in valid_themes:
            raise ValueError(f"FATAL: Invalid THEME '{cls.THEME}'.")
        if not isinstance(cls.PRELOAD_TO_RAM, bool):
            raise TypeError(f"FATAL: PRELOAD_TO_RAM must be True or False.")
        if not isinstance(cls.EXPORT_GRANULAR_TELEMETRY, bool):
            raise TypeError(f"FATAL: EXPORT_GRANULAR_TELEMETRY must be True or False.")
        if not isinstance(cls.COOLING_EFFICIENCY_PCT, (int, float)) or not (0 < cls.COOLING_EFFICIENCY_PCT <= 100):
            raise ValueError(f"FATAL: COOLING_EFFICIENCY_PCT must be strictly greater than 0 and less than or equal to 100.")

if Config.NUM_CORES is not None:
    os.environ["OMP_NUM_THREADS"] = str(Config.NUM_CORES)
    os.environ["OPENBLAS_NUM_THREADS"] = str(Config.NUM_CORES)
    os.environ["MKL_NUM_THREADS"] = str(Config.NUM_CORES)

STATE_IDLE = 0
STATE_ACTIVE = 1
STATE_THROTTLED = 2
STATE_SHUTDOWN = 3
GLOBAL_TRACE_CACHE = {}

class TerminalLogger(object):
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.filename = filename
        self.buffer = ""
        with open(self.filename, "w", encoding='utf-8') as f:
            pass
        
    def write(self, message):
        self.terminal.write(message)
        for char in message:
            if char == '\r':
                self.buffer = ""  
            elif char == '\n':
                with open(self.filename, "a", encoding='utf-8') as f:
                    f.write(self.buffer + '\n')
                self.buffer = ""
            else:
                self.buffer += char
                
    def flush(self):
        self.terminal.flush()

# =============================================================================
# 2. PHYSICS PARAMETERS & FAST JIT ENGINE
# =============================================================================

# Calculate the cooling multiplier based on user config
_cooling_mult = Config.COOLING_EFFICIENCY_PCT / 100.0

PHYSICS_PARAMS = {
    "DT": 0.11,
    "IDLE_POWER": 25.0,
    "THROTTLE_TEMP": 87.0,
    "RECOVERY_TEMP": 83.0,
    "SHUTDOWN_TEMP": 90.0,
    "THROTTLE_CAP": 100.0,
    "C_die_0": 8.9324045, "C_die_1": 8.8706836,
    "C_sink_0": 4713.588, "C_sink_1": 4831.154,
    "R_paste_0": 0.03658, "R_paste_1": 0.03357,
    "k01": 0.01670, "k10": 0.00283,
    "q0": -8.92023, "q1": -8.94283,
    
    # --- DYNAMIC COOLING PARAMETERS ---
    "h_base_0": 3.97913 * _cooling_mult, 
    "h_base_1": 4.76174 * _cooling_mult,
    "h_active_0": 20.9114 * _cooling_mult, 
    "h_active_1": 19.8178 * _cooling_mult,
    
    "T_thresh_0": 70.2506, "T_thresh_1": 67.5134,
    "beta_0": 1.66880, "beta_1": 1.33509,

    # --- THERMAL AWARE SCHEDULER PARAMS ---
    "PROJECTION_POWER": 250.0,    
    "PROJECTION_SECONDS": 300.0,  
}

# PRE-CALCULATED INVERSES
INV_C_die_0 = 1.0 / PHYSICS_PARAMS["C_die_0"]
INV_C_die_1 = 1.0 / PHYSICS_PARAMS["C_die_1"]
INV_C_sink_0 = 1.0 / PHYSICS_PARAMS["C_sink_0"]
INV_C_sink_1 = 1.0 / PHYSICS_PARAMS["C_sink_1"]
INV_R_paste_0 = 1.0 / PHYSICS_PARAMS["R_paste_0"]
INV_R_paste_1 = 1.0 / PHYSICS_PARAMS["R_paste_1"]

PARAMS_TUPLE = (
    PHYSICS_PARAMS["DT"], INV_C_die_0, INV_C_die_1, 
    INV_C_sink_0, INV_C_sink_1, INV_R_paste_0, 
    INV_R_paste_1, PHYSICS_PARAMS["k01"], PHYSICS_PARAMS["k10"], 
    PHYSICS_PARAMS["q0"], PHYSICS_PARAMS["q1"], PHYSICS_PARAMS["h_base_0"], 
    PHYSICS_PARAMS["h_base_1"], PHYSICS_PARAMS["h_active_0"], PHYSICS_PARAMS["h_active_1"], 
    PHYSICS_PARAMS["T_thresh_0"], PHYSICS_PARAMS["T_thresh_1"], PHYSICS_PARAMS["beta_0"], 
    PHYSICS_PARAMS["beta_1"]
)

@njit(cache=True)
def sigmoid(x):
    return 1.0 / (1.0 + np.exp(-x))

@njit(cache=True)
def step_physics_numba(T_die, T_sink, P_draw, T_amb, params_tuple):
    (DT, inv_C_d0, inv_C_d1, inv_C_s0, inv_C_s1, inv_R_p0, inv_R_p1, 
     k01, k10, q0, q1, h_base_0, h_base_1, h_active_0, h_active_1, 
     T_thresh_0, T_thresh_1, beta_0, beta_1) = params_tuple

    num_nodes = T_die.shape[0]
    T_die_new = np.empty_like(T_die)
    T_sink_new = np.empty_like(T_sink)

    for i in range(num_nodes):
        t_d0, t_d1 = T_die[i, 0], T_die[i, 1]
        t_s0, t_s1 = T_sink[i, 0], T_sink[i, 1]
        p0, p1 = P_draw[i, 0], P_draw[i, 1]

        h0_curr = h_base_0 + h_active_0 * sigmoid(beta_0 * (t_d0 - T_thresh_0))
        h1_curr = h_base_1 + h_active_1 * sigmoid(beta_1 * (t_d1 - T_thresh_1))
        
        dT0_die = (p0 - (t_d0 - t_s0) * inv_R_p0) * inv_C_d0
        dT0_sink = ((t_d0 - t_s0) * inv_R_p0 + k01 * p1 - h0_curr * (t_s0 - T_amb) + q0) * inv_C_s0
        
        dT1_die = (p1 - (t_d1 - t_s1) * inv_R_p1) * inv_C_d1
        dT1_sink = ((t_d1 - t_s1) * inv_R_p1 + k10 * p0 - h1_curr * (t_s1 - T_amb) + q1) * inv_C_s1
        
        T_die_new[i, 0] = t_d0 + DT * dT0_die
        T_die_new[i, 1] = t_d1 + DT * dT1_die
        T_sink_new[i, 0] = t_s0 + DT * dT0_sink
        T_sink_new[i, 1] = t_s1 + DT * dT1_sink

    return T_die_new, T_sink_new

@njit(cache=True)
def find_best_placement_numba(req_gpus, gpu_status, T_die, T_sink,
                               is_thermal_aware,
                               T_amb, params_tuple, proj_power, proj_steps, idle_power):
    num_nodes = gpu_status.shape[0]
    
    cand_nodes = np.empty(num_nodes * 2, dtype=np.int32)
    cand_gpus = np.empty(num_nodes * 2, dtype=np.int32)
    num_cands = 0
    
    for n in range(num_nodes):
        if req_gpus == 2:
            if gpu_status[n, 0] == 0 and gpu_status[n, 1] == 0:
                cand_nodes[num_cands] = n
                cand_gpus[num_cands] = 2
                num_cands += 1
        elif req_gpus == 1:
            if gpu_status[n, 0] == 0:
                cand_nodes[num_cands] = n
                cand_gpus[num_cands] = 0
                num_cands += 1
            if gpu_status[n, 1] == 0:
                cand_nodes[num_cands] = n
                cand_gpus[num_cands] = 1
                num_cands += 1
                
    if num_cands == 0:
        return -1, -1
        
    if not is_thermal_aware:
        return cand_nodes[0], cand_gpus[0]
    
    # --- THERMAL-AWARE: ODE Projection-based ranking ---
    T_die_proj = np.empty((num_cands, 2))
    T_sink_proj = np.empty((num_cands, 2))
    P_draw_proj = np.full((num_cands, 2), idle_power)
    
    for i in range(num_cands):
        n = cand_nodes[i]
        g = cand_gpus[i]
        T_die_proj[i, 0] = T_die[n, 0]
        T_die_proj[i, 1] = T_die[n, 1]
        T_sink_proj[i, 0] = T_sink[n, 0]
        T_sink_proj[i, 1] = T_sink[n, 1]
        
        if g == 2:
            P_draw_proj[i, 0] = proj_power
            P_draw_proj[i, 1] = proj_power
        elif g == 0:
            P_draw_proj[i, 0] = proj_power
            if gpu_status[n, 1] != 0 and gpu_status[n, 1] != 3:
                P_draw_proj[i, 1] = proj_power
        elif g == 1:
            P_draw_proj[i, 1] = proj_power
            if gpu_status[n, 0] != 0 and gpu_status[n, 0] != 3:
                P_draw_proj[i, 0] = proj_power
    
    avg_temps = np.zeros(num_cands)
    
    for step in range(proj_steps):
        T_die_proj, T_sink_proj = step_physics_numba(T_die_proj, T_sink_proj, P_draw_proj, T_amb, params_tuple)
        for i in range(num_cands):
            g = cand_gpus[i]
            if g == 2:
                avg_temps[i] += max(T_die_proj[i, 0], T_die_proj[i, 1])
            elif g == 0:
                avg_temps[i] += T_die_proj[i, 0]
            else:
                avg_temps[i] += T_die_proj[i, 1]
    
    for i in range(num_cands):
        avg_temps[i] /= proj_steps
    
    best_idx = np.argmin(avg_temps)
    return cand_nodes[best_idx], cand_gpus[best_idx]


# =============================================================================
# 3. HTML GENERATION TEMPLATE
# =============================================================================
def generate_html_template(node_id, labels, t0, t1, p0, p1, mode, theme='dark'):
    is_dark = theme == 'dark'
    bg_color = '#0f172a' if is_dark else '#f8fafc'
    card_color = '#1e293b' if is_dark else '#ffffff'
    text_color = '#f8fafc' if is_dark else '#0f172a'
    grid_color = '#334155' if is_dark else '#e2e8f0'
    hint_color = '#94a3b8' if is_dark else '#64748b'
    btn_bg = '#334155' if is_dark else '#e2e8f0'
    btn_hover = '#475569' if is_dark else '#cbd5e1'
    max_label = labels[-1] if labels else 100

    if mode == 'THERMAL_AWARE':
        badge_bg = 'rgba(6, 78, 59, 0.5)' if is_dark else '#d1fae5'
        badge_text = '#34d399' if is_dark else '#065f46'
    else:
        badge_bg = 'rgba(120, 53, 15, 0.5)' if is_dark else '#fef3c7'
        badge_text = '#f59e0b' if is_dark else '#92400e'

    return f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Node {node_id} Telemetry ({mode})</title>
      <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
      <script src="https://cdn.jsdelivr.net/npm/hammerjs@2.0.8"></script>
      <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom"></script>
      <style>
        body {{ font-family: system-ui, -apple-system, sans-serif; background: {bg_color}; color: {text_color}; margin: 0; padding: 20px; }}
        .header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }}
        .header h1 {{ margin: 0; font-size: 1.5rem; }}
        .badge {{ background: {badge_bg}; color: {badge_text}; padding: 4px 10px; border-radius: 6px; font-size: 0.875rem; font-weight: bold; text-transform: uppercase; letter-spacing: 0.05em; }}
        .chart-container {{ position: relative; height: 85vh; width: 100%; background: {card_color}; border-radius: 12px; padding: 20px; box-sizing: border-box; border: 1px solid {grid_color}; }}
        .hint {{ font-size: 0.875rem; color: {hint_color}; }}
        .btn-reset {{ background: {btn_bg}; color: {text_color}; border: none; padding: 6px 12px; border-radius: 6px; cursor: pointer; font-size: 0.875rem; font-weight: bold; transition: background 0.2s; }}
        .btn-reset:hover {{ background: {btn_hover}; }}
        .controls {{ display: flex; gap: 12px; align-items: center; }}
      </style>
    </head>
    <body>
      <div class="header">
        <h1>Node {node_id} Telemetry</h1>
        <div class="controls">
          <span class="hint">Scroll to zoom, drag to pan</span>
          <button id="resetZoomBtn" class="btn-reset">Reset Zoom</button>
          <span class="badge">{mode}</span>
        </div>
      </div>
      <div class="chart-container">
        <canvas id="telemetryChart"></canvas>
      </div>
      <script>
        const ctx = document.getElementById('telemetryChart').getContext('2d');
        const labels = {json.dumps(labels)};
        const t0 = {json.dumps(t0)};
        const t1 = {json.dumps(t1)};
        const p0 = {json.dumps(p0)};
        const p1 = {json.dumps(p1)};
        const mapData = (dataArr) => dataArr.map((y, i) => ({{ x: labels[i], y }}));
        
        const chartInstance = new Chart(ctx, {{
          type: 'line',
          data: {{
            datasets: [
              {{ label: ' GPU 0 Temp (°C)', data: mapData(t0), borderColor: '#ef4444', yAxisID: 'y', tension: 0.2, pointRadius: 0, borderWidth: 2 }},
              {{ label: ' GPU 1 Temp (°C)', data: mapData(t1), borderColor: '#f97316', yAxisID: 'y', tension: 0.2, pointRadius: 0, borderWidth: 2, borderDash: [5, 5] }},
              {{ label: ' GPU 0 Power (W)', data: mapData(p0), borderColor: '#3b82f6', yAxisID: 'y1', tension: 0.1, pointRadius: 0, borderWidth: 1, fill: true, backgroundColor: 'rgba(59, 130, 246, 0.1)' }},
              {{ label: ' GPU 1 Power (W)', data: mapData(p1), borderColor: '#8b5cf6', yAxisID: 'y1', tension: 0.1, pointRadius: 0, borderWidth: 1, fill: true, backgroundColor: 'rgba(139, 92, 246, 0.1)' }}
            ]
          }},
          options: {{
            responsive: true, maintainAspectRatio: false, animation: false,
            interaction: {{ mode: 'index', intersect: false }},
            scales: {{
              x: {{ type: 'linear', grid: {{ display: false }}, ticks: {{ color: '{hint_color}', callback: val => Math.round(val) + 's' }} }},
              y: {{ type: 'linear', position: 'left', min: 20, max: 100, title: {{ display: true, text: 'Temperature (°C)', color: '{hint_color}' }}, grid: {{ color: '{grid_color}' }}, ticks: {{ color: '{hint_color}' }} }},
              y1: {{ type: 'linear', position: 'right', min: 0, max: 300, title: {{ display: true, text: 'Power Draw (W)', color: '{hint_color}' }}, grid: {{ drawOnChartArea: false }}, ticks: {{ color: '{hint_color}' }} }},
            }},
            plugins: {{
              legend: {{ labels: {{ color: '{text_color}', usePointStyle: true, boxWidth: 20 }} }},
              zoom: {{
                limits: {{ x: {{ min: 0, max: {max_label} + 5 }} }},
                zoom: {{ wheel: {{ enabled: true }}, pinch: {{ enabled: true }}, mode: 'x', speed: 0.05 }},
                pan: {{ enabled: true, mode: 'x' }}
              }}
            }}
          }}
        }});
        document.getElementById('resetZoomBtn').addEventListener('click', () => chartInstance.resetZoom());
      </script>
    </body>
    </html>
    """

# =============================================================================
# 4. JOB INGESTION (Memory-Mapped Binary Cache)
# =============================================================================
def _parse_single_file(args):
    """Worker function for parallel parsing. Runs in a separate process."""
    file_path, npy_cache_dir = args
    job_id = Path(file_path).stem
    df = pd.read_csv(file_path) if str(file_path).endswith('.csv') else pd.read_parquet(file_path)
    
    trace0_raw = df[df['gpu_index'] == 0]['power_draw_W'].values.astype(np.float32)
    trace1_raw = df[df['gpu_index'] == 1]['power_draw_W'].values.astype(np.float32)
    
    req_gpus = 2 if (len(trace0_raw) > 0 and len(trace1_raw) > 0) else 1
    t0_final = trace0_raw if len(trace0_raw) > 0 else trace1_raw
    t1_final = trace1_raw if (len(trace0_raw) > 0 and len(trace1_raw) > 0) else np.array([], dtype=np.float32)
    
    np.save(Path(npy_cache_dir) / f"{job_id}_t0.npy", t0_final)
    np.save(Path(npy_cache_dir) / f"{job_id}_t1.npy", t1_final)
    
    return {
        'id': job_id, 'requested_gpus': req_gpus,
        'trace_length_0': len(t0_final), 'trace_length_1': len(t1_final),
        'sort_key': int(job_id.split('-')[0])
    }

def load_jobs_from_directory(directory):
    path = Path(directory).resolve()
    print(f"\n[*] Scanning directory: {path}\n")
    
    if not path.exists():
        print(f"[!] FATAL: Input directory does not exist.")
        sys.exit(1)
    
    npy_cache_dir = path / "_npy_cache"
    metadata_cache = path / "_npy_cache" / "_metadata.pkl"
    
    if metadata_cache.exists():
        print(f"[*] Found binary trace cache! Loading metadata...")
        with open(metadata_cache, 'rb') as f:
            jobs = pickle.load(f)
        print(f"[*] Loaded {len(jobs)} jobs demanding {sum(j['requested_gpus'] for j in jobs)} GPUs from cache.")
        print(f"[*] Traces located at: {npy_cache_dir}")
        return jobs
    
    files = list(path.glob('*.csv')) + list(path.glob('*.parquet'))
    files.sort(key=lambda f: int(f.stem.split('-')[0]))
    
    if not files:
        print(f"[!] No valid trace files found in {directory}")
        sys.exit(1)
    
    npy_cache_dir.mkdir(parents=True, exist_ok=True)
    
    num_workers = min(os.cpu_count() or 1, len(files))
    print(f"[*] Parsing {len(files)} trace files using {num_workers} parallel workers...")
    
    work_items = [(str(f), str(npy_cache_dir)) for f in files]
    results = []
    
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(_parse_single_file, item): item for item in work_items}
        with tqdm(total=len(files), desc="Parsing & Caching Traces ", unit="file") as pbar:
            for future in as_completed(futures):
                results.append(future.result())
                pbar.update(1)
    
    results.sort(key=lambda r: r['sort_key'])
    
    jobs = []
    for r in results:
        jobs.append({
            'id': r['id'], 'requested_gpus': r['requested_gpus'],
            'trace_length_0': r['trace_length_0'], 'trace_length_1': r['trace_length_1'],
            'timeArrived': 0.0, 'timeStarted': 0.0,
            'currentIndex': 0, 'workDeficit_0': 0.0, 'workDeficit_1': 0.0,
            
            'tick_count_0': 0, 'min_temp_0': float('inf'), 'max_temp_0': -float('inf'), 'mean_0': 0.0, 'M2_0': 0.0,
            'tick_count_1': 0, 'min_temp_1': float('inf'), 'max_temp_1': -float('inf'), 'mean_1': 0.0, 'M2_1': 0.0,
            
            'throttledSteps_0': 0, 'throttledSteps_1': 0
        })
    
    print(f"\n[*] Saving job metadata to cache...")
    with open(metadata_cache, 'wb') as f:
        pickle.dump(jobs, f)
    
    print(f"[*] Loaded {len(jobs)} jobs demanding {sum(j['requested_gpus'] for j in jobs)} GPUs.")
    return jobs

# =============================================================================
# 5. CORE SIMULATION FUNCTION 
# =============================================================================
def run_simulation(mode, jobs_list, npy_cache_dir, output_dir, base_filename, num_nodes):
    
    total_jobs = len(jobs_list)
    jobs_queue = deque(jobs_list)
    
    active_traces = {}  
    
    # --- Optional Initialization for Granular Telemetry ---
    if Config.EXPORT_GRANULAR_TELEMETRY:
        telemetry_dir = output_dir / "Telemetry_Data_CSV"
        html_dir = output_dir / "Interactive_Graphs_HTML"
        telemetry_dir.mkdir(parents=True, exist_ok=True)
        html_dir.mkdir(parents=True, exist_ok=True)
        
        chart_labels = []
        chart_datasets = {i: {'t0':[], 't1':[], 'p0':[], 'p1':[]} for i in range(num_nodes)}
        telemetry_buffers = {i: [] for i in range(num_nodes)}
        file_suffix = "" if Config.HIGH_RES_TELEMETRY else "_Sampled"
        
        for n in range(num_nodes):
            with open(telemetry_dir / f"Node_{n}_Telemetry{file_suffix}.csv", "w") as f:
                f.write("time_sec,gpu0_temp_C,gpu1_temp_C,gpu0_power_W,gpu1_power_W\n")

        if total_jobs <= 100:
            chart_sample_rate = 50      # High resolution (1 point per ~5.5s)
        elif total_jobs <= 500:
            chart_sample_rate = 500     # Medium resolution (1 point per ~55s)
        else:
            chart_sample_rate = 5000    # Low resolution (1 point per ~9m)
    
    T_die = np.full((num_nodes, 2), Config.AMBIENT_TEMP_C, dtype=np.float64)
    T_sink = np.full((num_nodes, 2), Config.AMBIENT_TEMP_C, dtype=np.float64)
    P_draw = np.full((num_nodes, 2), PHYSICS_PARAMS["IDLE_POWER"], dtype=np.float64)
    
    gpu_status = np.full((num_nodes, 2), STATE_IDLE, dtype=np.int8)
    gpu_jobs = np.full((num_nodes, 2), None, dtype=object)
    
    projection_steps = int(PHYSICS_PARAMS['PROJECTION_SECONDS'] / PHYSICS_PARAMS['DT'])

    time_elapsed = 0.0
    tick_counter = 0
    jobs_completed = 0
    jobs_failed = 0
    
    failed_job_ids = set()
    completed_stats = []
    is_thermal_aware = mode == 'THERMAL_AWARE'

    with tqdm(total=total_jobs, desc=f"[{mode}] Simulating", unit="job", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]") as pbar:
        while jobs_queue or np.any((gpu_status == STATE_ACTIVE) | (gpu_status == STATE_THROTTLED)):
            
            # --- 1. JOB PLACEMENT ---
            while jobs_queue:
                job = jobs_queue[0]
                cand_n, cand_g = find_best_placement_numba(
                    job['requested_gpus'], gpu_status, T_die, T_sink,
                    is_thermal_aware,
                    Config.AMBIENT_TEMP_C, PARAMS_TUPLE,
                    PHYSICS_PARAMS['PROJECTION_POWER'], projection_steps, PHYSICS_PARAMS['IDLE_POWER']
                )
                
                if cand_n != -1:
                    job = jobs_queue.popleft() 
                    job['timeStarted'] = time_elapsed
                    if cand_g == 2:
                        job['assignment_temp'] = max(T_die[cand_n, 0], T_die[cand_n, 1])
                    else:
                        job['assignment_temp'] = T_die[cand_n, cand_g]
                    
                    if Config.PRELOAD_TO_RAM:
                        job['_t0'] = GLOBAL_TRACE_CACHE[f"{job['id']}_t0"]
                        job['_t1'] = GLOBAL_TRACE_CACHE[f"{job['id']}_t1"]
                    else:
                        if job['id'] not in active_traces:
                            t0_path = npy_cache_dir / f"{job['id']}_t0.npy"
                            t1_path = npy_cache_dir / f"{job['id']}_t1.npy"
                            active_traces[job['id']] = (
                                np.load(t0_path, mmap_mode='r'),
                                np.load(t1_path, mmap_mode='r')
                            )
                        job['_t0'], job['_t1'] = active_traces[job['id']]

                    if cand_g == 2:
                        gpu_status[cand_n, 0], gpu_status[cand_n, 1] = STATE_ACTIVE, STATE_ACTIVE
                        gpu_jobs[cand_n, 0], gpu_jobs[cand_n, 1] = job, job
                    else:
                        gpu_status[cand_n, cand_g] = STATE_ACTIVE
                        gpu_jobs[cand_n, cand_g] = job
                else:
                    break 
                    
            # --- 2. WORK DEFICIT ---
            P_draw.fill(PHYSICS_PARAMS["IDLE_POWER"])
            
            for n in range(num_nodes):
                p0, p1 = PHYSICS_PARAMS["IDLE_POWER"], PHYSICS_PARAMS["IDLE_POWER"]
                job0, job1 = gpu_jobs[n, 0], gpu_jobs[n, 1]
                
                if job0 is not None and job0 == job1:
                    job = job0
                    t0, t1 = T_die[n, 0], T_die[n, 1]
                    
                    job['tick_count_0'] += 1
                    job['min_temp_0'] = min(job['min_temp_0'], t0)
                    job['max_temp_0'] = max(job['max_temp_0'], t0)
                    d0 = t0 - job['mean_0']
                    job['mean_0'] += d0 / job['tick_count_0']
                    job['M2_0'] += d0 * (t0 - job['mean_0'])

                    job['tick_count_1'] += 1
                    job['min_temp_1'] = min(job['min_temp_1'], t1)
                    job['max_temp_1'] = max(job['max_temp_1'], t1)
                    d1 = t1 - job['mean_1']
                    job['mean_1'] += d1 / job['tick_count_1']
                    job['M2_1'] += d1 * (t1 - job['mean_1'])
                    
                    if t0 >= PHYSICS_PARAMS["SHUTDOWN_TEMP"]:
                        gpu_status[n, 0] = STATE_SHUTDOWN
                        if job['id'] not in failed_job_ids: 
                            failed_job_ids.add(job['id'])
                            jobs_failed += 1
                            pbar.update(1)
                    elif t0 >= PHYSICS_PARAMS["THROTTLE_TEMP"] and gpu_status[n, 0] == STATE_ACTIVE:
                        gpu_status[n, 0] = STATE_THROTTLED
                    elif t0 <= PHYSICS_PARAMS["RECOVERY_TEMP"] and gpu_status[n, 0] == STATE_THROTTLED:
                        gpu_status[n, 0] = STATE_ACTIVE
                    
                    if gpu_status[n, 0] == STATE_THROTTLED: job['throttledSteps_0'] += 1
                        
                    if t1 >= PHYSICS_PARAMS["SHUTDOWN_TEMP"]:
                        gpu_status[n, 1] = STATE_SHUTDOWN
                        if job['id'] not in failed_job_ids: 
                            failed_job_ids.add(job['id'])
                            jobs_failed += 1
                            pbar.update(1)
                    elif t1 >= PHYSICS_PARAMS["THROTTLE_TEMP"] and gpu_status[n, 1] == STATE_ACTIVE:
                        gpu_status[n, 1] = STATE_THROTTLED
                    elif t1 <= PHYSICS_PARAMS["RECOVERY_TEMP"] and gpu_status[n, 1] == STATE_THROTTLED:
                        gpu_status[n, 1] = STATE_ACTIVE
                        
                    if gpu_status[n, 1] == STATE_THROTTLED: job['throttledSteps_1'] += 1

                    if job['workDeficit_0'] <= 0 and job['workDeficit_1'] <= 0 and job['currentIndex'] < job['trace_length_0']:
                        job['workDeficit_0'] = job['_t0'][job['currentIndex']]
                        job['workDeficit_1'] = job['_t1'][job['currentIndex']]

                    if job['workDeficit_0'] > 0:
                        p0 = min(job['workDeficit_0'], PHYSICS_PARAMS["THROTTLE_CAP"]) if gpu_status[n, 0] == STATE_THROTTLED else job['workDeficit_0']
                        job['workDeficit_0'] -= p0
                    if job['workDeficit_1'] > 0:
                        p1 = min(job['workDeficit_1'], PHYSICS_PARAMS["THROTTLE_CAP"]) if gpu_status[n, 1] == STATE_THROTTLED else job['workDeficit_1']
                        job['workDeficit_1'] -= p1
                    if job['workDeficit_0'] <= 0 and job['workDeficit_1'] <= 0:
                        job['currentIndex'] += 1
                else:
                    if job0 is not None:
                        t0 = T_die[n, 0]
                        job0['tick_count_0'] += 1
                        job0['min_temp_0'] = min(job0['min_temp_0'], t0)
                        job0['max_temp_0'] = max(job0['max_temp_0'], t0)
                        d0 = t0 - job0['mean_0']
                        job0['mean_0'] += d0 / job0['tick_count_0']
                        job0['M2_0'] += d0 * (t0 - job0['mean_0'])

                        if t0 >= PHYSICS_PARAMS["SHUTDOWN_TEMP"]:
                            gpu_status[n, 0] = STATE_SHUTDOWN
                            if job0['id'] not in failed_job_ids: 
                                failed_job_ids.add(job0['id'])
                                jobs_failed += 1
                                pbar.update(1)
                        elif t0 >= PHYSICS_PARAMS["THROTTLE_TEMP"] and gpu_status[n, 0] == STATE_ACTIVE:
                            gpu_status[n, 0] = STATE_THROTTLED
                        elif t0 <= PHYSICS_PARAMS["RECOVERY_TEMP"] and gpu_status[n, 0] == STATE_THROTTLED:
                            gpu_status[n, 0] = STATE_ACTIVE
                        if gpu_status[n, 0] == STATE_THROTTLED: job0['throttledSteps_0'] += 1
                        if job0['workDeficit_0'] <= 0 and job0['currentIndex'] < job0['trace_length_0']:
                            job0['workDeficit_0'] = job0['_t0'][job0['currentIndex']]
                        if job0['workDeficit_0'] > 0:
                            p0 = min(job0['workDeficit_0'], PHYSICS_PARAMS["THROTTLE_CAP"]) if gpu_status[n, 0] == STATE_THROTTLED else job0['workDeficit_0']
                            job0['workDeficit_0'] -= p0
                        if job0['workDeficit_0'] <= 0: job0['currentIndex'] += 1
                        
                    if job1 is not None:
                        t1 = T_die[n, 1]
                        job1['tick_count_1'] += 1
                        job1['min_temp_1'] = min(job1['min_temp_1'], t1)
                        job1['max_temp_1'] = max(job1['max_temp_1'], t1)
                        d1 = t1 - job1['mean_1']
                        job1['mean_1'] += d1 / job1['tick_count_1']
                        job1['M2_1'] += d1 * (t1 - job1['mean_1'])

                        if t1 >= PHYSICS_PARAMS["SHUTDOWN_TEMP"]:
                            gpu_status[n, 1] = STATE_SHUTDOWN
                            if job1['id'] not in failed_job_ids: 
                                failed_job_ids.add(job1['id'])
                                jobs_failed += 1
                                pbar.update(1)
                        elif t1 >= PHYSICS_PARAMS["THROTTLE_TEMP"] and gpu_status[n, 1] == STATE_ACTIVE:
                            gpu_status[n, 1] = STATE_THROTTLED
                        elif t1 <= PHYSICS_PARAMS["RECOVERY_TEMP"] and gpu_status[n, 1] == STATE_THROTTLED:
                            gpu_status[n, 1] = STATE_ACTIVE
                        if gpu_status[n, 1] == STATE_THROTTLED: job1['throttledSteps_1'] += 1
                        traceToUse = job1['_t1'] if job1['requested_gpus'] == 2 else job1['_t0']
                        trace_len_use = job1['trace_length_1'] if job1['requested_gpus'] == 2 else job1['trace_length_0']
                        if job1['workDeficit_1'] <= 0 and job1['currentIndex'] < trace_len_use:
                            job1['workDeficit_1'] = traceToUse[job1['currentIndex']]
                        if job1['workDeficit_1'] > 0:
                            p1 = min(job1['workDeficit_1'], PHYSICS_PARAMS["THROTTLE_CAP"]) if gpu_status[n, 1] == STATE_THROTTLED else job1['workDeficit_1']
                            job1['workDeficit_1'] -= p1
                        if job1['workDeficit_1'] <= 0: job1['currentIndex'] += 1
                P_draw[n, 0], P_draw[n, 1] = p0, p1

            # --- 3. STEP PHYSICS ---
            T_die, T_sink = step_physics_numba(T_die, T_sink, P_draw, Config.AMBIENT_TEMP_C, PARAMS_TUPLE)
            
            # --- 4. CHECK COMPLETIONS ---
            for n in range(num_nodes):
                for g_id in [0, 1]:
                    job = gpu_jobs[n, g_id]
                    if not job: continue
                    
                    if gpu_status[n, g_id] == STATE_SHUTDOWN:
                        gpu_jobs[n, g_id] = None
                        gpu_status[n, g_id] = STATE_IDLE
                        continue

                    trace_len = job['trace_length_1'] if (job['requested_gpus'] == 2 and g_id == 1) else job['trace_length_0']
                    
                    if job['currentIndex'] >= trace_len:
                        if g_id == 0:
                            min_t = job['min_temp_0'] if job['min_temp_0'] != float('inf') else 0
                            max_t = job['max_temp_0'] if job['max_temp_0'] != -float('inf') else 0
                            mean_t = job['mean_0']
                            std_t = math.sqrt(job['M2_0'] / job['tick_count_0']) if job['tick_count_0'] > 0 else 0
                            thr = job['throttledSteps_0']
                        else:
                            min_t = job['min_temp_1'] if job['min_temp_1'] != float('inf') else 0
                            max_t = job['max_temp_1'] if job['max_temp_1'] != -float('inf') else 0
                            mean_t = job['mean_1']
                            std_t = math.sqrt(job['M2_1'] / job['tick_count_1']) if job['tick_count_1'] > 0 else 0
                            thr = job['throttledSteps_1']
                            
                        completed_stats.append({
                            'job_id': job['id'], 'node_number': n, 'gpu_index': g_id,
                            'req_gpus': job['requested_gpus'],
                            'wait_time_sec': job['timeStarted'] - job['timeArrived'],
                            'execution_time_sec': time_elapsed - job['timeStarted'],
                            'min_temp_C': min_t, 'max_temp_C': max_t,
                            'mean_temp_C': mean_t, 'temp_std_dev_C': std_t,
                            'was_throttled': thr > 0, 'throttle_time_sec': thr * PHYSICS_PARAMS["DT"],
                            'assignment_temp_C': job.get('assignment_temp', 0.0)
                        })
                        
                        is_job_fully_done = True
                        if job['requested_gpus'] == 2:
                            other_g_id = 1 if g_id == 0 else 0
                            if gpu_jobs[n, other_g_id] is not None and gpu_jobs[n, other_g_id]['id'] == job['id']:
                                is_job_fully_done = False
                        
                        gpu_status[n, g_id] = STATE_IDLE
                        gpu_jobs[n, g_id] = None
                        
                        if is_job_fully_done:
                            if not Config.PRELOAD_TO_RAM:
                                if job['id'] in active_traces:
                                    del active_traces[job['id']]
                            jobs_completed += 1
                            pbar.update(1)

            # --- 5. OPTIONAL TELEMETRY TRACKING ---
            if Config.EXPORT_GRANULAR_TELEMETRY:
                should_log = True if Config.HIGH_RES_TELEMETRY else (tick_counter % 50 == 0)
                if should_log:
                    for n in range(num_nodes):
                        telemetry_buffers[n].append((time_elapsed, T_die[n,0], T_die[n,1], P_draw[n,0], P_draw[n,1]))

                if len(telemetry_buffers[0]) >= 5000:
                    for n in range(num_nodes):
                        with open(telemetry_dir / f"Node_{n}_Telemetry{file_suffix}.csv", "a") as f:
                            lines = [f"{row[0]:.2f},{row[1]:.2f},{row[2]:.2f},{row[3]:.2f},{row[4]:.2f}\n" for row in telemetry_buffers[n]]
                            f.writelines(lines)
                        telemetry_buffers[n].clear()

                if tick_counter % chart_sample_rate == 0:
                    chart_labels.append(round(time_elapsed))
                    for n in range(num_nodes):
                        chart_datasets[n]['t0'].append(float(T_die[n, 0])); chart_datasets[n]['t1'].append(float(T_die[n, 1]))
                        chart_datasets[n]['p0'].append(float(P_draw[n, 0])); chart_datasets[n]['p1'].append(float(P_draw[n, 1]))

            # --- 6. UPDATE PROGRESS BAR ---
            if tick_counter % 1000 == 0:
                active_list = [j for j in gpu_jobs.flatten() if j is not None]
                num_active = len(set(id(j) for j in active_list))
                postfix_data = OrderedDict([
                    ("Submitted", total_jobs), ("Queued", len(jobs_queue)),
                    ("Active", num_active), ("Completed", jobs_completed), ("Failed", jobs_failed)
                ])
                pbar.set_postfix(postfix_data)

            time_elapsed += PHYSICS_PARAMS["DT"]
            tick_counter += 1

        pbar.set_postfix(OrderedDict([
            ("Submitted", total_jobs), ("Queued", 0),
            ("Active", 0), ("Completed", jobs_completed), ("Failed", jobs_failed)
        ]))
        pbar.refresh()

    if Config.EXPORT_GRANULAR_TELEMETRY:
        print(f"\n[*] Flushing final telemetry payloads to disk...\n")
        for n in range(num_nodes):
            if len(telemetry_buffers[n]) > 0:
                with open(telemetry_dir / f"Node_{n}_Telemetry{file_suffix}.csv", "a") as f:
                    lines = [f"{row[0]:.2f},{row[1]:.2f},{row[2]:.2f},{row[3]:.2f},{row[4]:.2f}\n" for row in telemetry_buffers[n]]
                    f.writelines(lines)
                telemetry_buffers[n].clear()
                
        for n in range(num_nodes):
            html_str = generate_html_template(n, chart_labels, chart_datasets[n]['t0'], chart_datasets[n]['t1'], chart_datasets[n]['p0'], chart_datasets[n]['p1'], mode, theme=Config.THEME)
            with open(html_dir / f"Node_{n}_Telemetry.html", 'w', encoding='utf-8') as f: f.write(html_str)
        
    unique_jobs = {}
    for s in completed_stats:
        jid = s['job_id']
        if jid not in unique_jobs:
            unique_jobs[jid] = {
                'node_number': s['node_number'],
                'gpu_index': s['gpu_index'] if s['req_gpus'] == 1 else 'BOTH',
                'wait_time_sec': s['wait_time_sec'],
                'execution_time_sec': s['execution_time_sec'],
                'min_temp_C': s['min_temp_C'],
                'max_temp_C': s['max_temp_C'],
                'mean_temp_C': s['mean_temp_C'],
                'temp_std_dev_C': s['temp_std_dev_C'],
                'was_throttled': s['was_throttled'],
                'throttle_time_sec': s['throttle_time_sec'],
                'assignment_temp_C': s['assignment_temp_C']
            }
        else:
            unique_jobs[jid]['min_temp_C'] = min(unique_jobs[jid]['min_temp_C'], s['min_temp_C'])
            unique_jobs[jid]['max_temp_C'] = max(unique_jobs[jid]['max_temp_C'], s['max_temp_C'])
            unique_jobs[jid]['mean_temp_C'] = (unique_jobs[jid]['mean_temp_C'] + s['mean_temp_C']) / 2.0
            unique_jobs[jid]['temp_std_dev_C'] = (unique_jobs[jid]['temp_std_dev_C'] + s['temp_std_dev_C']) / 2.0
            unique_jobs[jid]['throttle_time_sec'] += s['throttle_time_sec']
            if s['was_throttled']: unique_jobs[jid]['was_throttled'] = True

    n_unique = len(unique_jobs)
    if n_unique > 0:
        tot_wait = sum(j['wait_time_sec'] for j in unique_jobs.values())
        avg_wait = tot_wait / n_unique
        tot_exec = sum(j['execution_time_sec'] for j in unique_jobs.values())
        avg_exec = tot_exec / n_unique
        min_temp = min(j['min_temp_C'] for j in unique_jobs.values())
        avg_min_temp = sum(j['min_temp_C'] for j in unique_jobs.values()) / n_unique
        max_temp = max(j['max_temp_C'] for j in unique_jobs.values())
        avg_max_temp = sum(j['max_temp_C'] for j in unique_jobs.values()) / n_unique
        mean_temp = sum(j['mean_temp_C'] for j in unique_jobs.values()) / n_unique
        avg_mean_temp = mean_temp 
        min_std = min(j['temp_std_dev_C'] for j in unique_jobs.values())
        max_std = max(j['temp_std_dev_C'] for j in unique_jobs.values())
        avg_std = sum(j['temp_std_dev_C'] for j in unique_jobs.values()) / n_unique
        tot_throttle = sum(j['throttle_time_sec'] for j in unique_jobs.values())
        avg_assign_temp = sum(j['assignment_temp_C'] for j in unique_jobs.values()) / n_unique
    else:
        tot_wait = avg_wait = tot_exec = avg_exec = min_temp = avg_min_temp = max_temp = avg_max_temp = mean_temp = avg_mean_temp = min_std = max_std = avg_std = tot_throttle = avg_assign_temp = 0

    agg_stats_dict = {
        "completedCount_UniqueJobs": n_unique,
        "total_wait_time_sec": tot_wait,
        "avg_wait_time_sec": avg_wait,
        "total_execution_time_sec": tot_exec,
        "avg_execution_time_sec": avg_exec,
        "min_temp_C": min_temp,
        "avg_min_temp_C": avg_min_temp,
        "max_temp_C": max_temp,
        "avg_max_temp_C": avg_max_temp,
        "mean_temp_C": mean_temp,
        "avg_mean_temp_C": avg_mean_temp,
        "avg_assignment_temp_C": avg_assign_temp,
        "min_temp_std_dev_C": min_std,
        "max_temp_std_dev_C": max_std,
        "avg_temp_std_dev_C": avg_std,
        "throttle_time_sec": tot_throttle
    }

    local_metadata = {
        "System_Configuration": {
            "simulation_mode": mode,
            "node_count": num_nodes,
            "total_submitted_jobs": total_jobs,
            "ambient_temp_C": Config.AMBIENT_TEMP_C,
            "cooling_efficiency_pct": Config.COOLING_EFFICIENCY_PCT
        },
        "simulated_makespan_sec": time_elapsed,
        "metrics": agg_stats_dict
    }
    local_json_path = output_dir / f"{base_filename}_metadata.json"
    with open(local_json_path, 'w') as f: json.dump(local_metadata, f, indent=2)

    overall_row = f"OVERALL,N/A,N/A,{avg_wait:.1f},{avg_exec:.1f},{min_temp:.1f},{max_temp:.1f},{mean_temp:.1f},{avg_assign_temp:.1f},{avg_std:.2f},{tot_throttle > 0},{tot_throttle:.1f}\n"
    local_csv_path = output_dir / f"{base_filename}_summary.csv"
    
    with open(local_csv_path, 'w', newline='') as f:
        f.write("job_id,node_number,gpu_index,wait_time_sec,execution_time_sec,min_temp_C,max_temp_C,mean_temp_C,assignment_temp_C,temp_std_dev_C,was_throttled,throttle_time_sec\n")
        for jid, s in unique_jobs.items():
            f.write(f"{jid},{s['node_number']},{s['gpu_index']},{s['wait_time_sec']:.1f},{s['execution_time_sec']:.1f},"
                    f"{s['min_temp_C']:.1f},{s['max_temp_C']:.1f},{s['mean_temp_C']:.1f},{s['assignment_temp_C']:.1f},{s['temp_std_dev_C']:.2f},{str(s['was_throttled']).lower()},{s['throttle_time_sec']:.1f}\n")
        f.write(overall_row)

    return unique_jobs, agg_stats_dict, time_elapsed

# =============================================================================
# 6. MAIN EXECUTION
# =============================================================================
def main():
    Config.validate()
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    log_nodes = str(Config.NODES).replace(" ", "")
    
    input_path = Path(Config.INPUT_DIR).resolve()
    npy_cache_dir = input_path / "_npy_cache"
    metadata_cache = npy_cache_dir / "_metadata.pkl"
    if metadata_cache.exists():
        with open(metadata_cache, 'rb') as f:
            total_jobs = len(pickle.load(f))
    else:
        total_jobs = len(list(input_path.glob('*.csv')) + list(input_path.glob('*.parquet')))
    
    folder_name = f"{Config.MODE}_{log_nodes}_{total_jobs}_{Config.AMBIENT_TEMP_C}_{timestamp}"
    Config.OUTPUT_DIR = Path(Config.OUTPUT_DIR) / folder_name
    Config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    final_log_filename = Config.OUTPUT_DIR / f"{Config.MODE}_{log_nodes}_{total_jobs}_{Config.AMBIENT_TEMP_C}_{timestamp}_terminal_logs.txt"
    sys.stdout = TerminalLogger(final_log_filename)
    sys.stderr = sys.stdout
     
    master_queue = load_jobs_from_directory(Config.INPUT_DIR)

    if Config.PRELOAD_TO_RAM:
        print("\n[*] PRELOAD_TO_RAM explicitly enabled. Initializing traces to system memory...")
        for job in tqdm(master_queue, desc="Caching Binary Traces"):
            t0_path = npy_cache_dir / f"{job['id']}_t0.npy"
            t1_path = npy_cache_dir / f"{job['id']}_t1.npy"
            GLOBAL_TRACE_CACHE[f"{job['id']}_t0"] = np.load(t0_path)
            GLOBAL_TRACE_CACHE[f"{job['id']}_t1"] = np.load(t1_path)
        print(f"\n[*] Memory ingest complete: {len(GLOBAL_TRACE_CACHE)} vector structures retained.\n")
    else:
        print("\n[*] PRELOAD_TO_RAM effectively disabled. Bypassing global memory and adopting mmap I/O constraints.\n")
    
    print("=" * 70)
    print(f"--- ThermalODE SIMULATOR INITIALIZATION ---")
    print("=" * 70)

    print("\n[*] DIRECTORY PATHS:")
    print(f"    - INPUT DIRECTORY : \n        {Config.INPUT_DIR}")
    print(f"    - OUTPUT DIRECTORY: \n        {Config.OUTPUT_DIR}")
    
    print("\n[*] SYSTEM CONFIGURATION:")
    print(f"    - MODE                      : {Config.MODE}")
    print(f"    - NODE(S) TO SIMULATE       : {Config.NODES}")
    print(f"    - AMBIENT TEMP              : {Config.AMBIENT_TEMP_C} °C")
    print(f"    - COOLING EFFICIENCY        : {Config.COOLING_EFFICIENCY_PCT}%")
    print(f"    - NUM CORES                 : {Config.NUM_CORES if Config.NUM_CORES is not None else 'Auto (All)'}")
    print(f"    - PRELOAD TO RAM            : {Config.PRELOAD_TO_RAM}")
    print(f"    - EXPORT GRANULAR TELEMETRY : {Config.EXPORT_GRANULAR_TELEMETRY}")
    print(f"    - HIGH RES TELEMETRY        : {Config.HIGH_RES_TELEMETRY}")
    print(f"    - THEME                     : {Config.THEME}")

    print("\n[*] DERIVED PHYSICS PARAMETERS:")
    print(f"    - h_base_0                  : {PHYSICS_PARAMS['h_base_0']:.5f}")
    print(f"    - h_active_0                : {PHYSICS_PARAMS['h_active_0']:.5f}")
    print(f"    - h_base_1                  : {PHYSICS_PARAMS['h_base_1']:.5f}")
    print(f"    - h_active_1                : {PHYSICS_PARAMS['h_active_1']:.5f}")
    print("=" * 70)
    
    if Config.MODE in ['STANDARD', 'THERMAL_AWARE', 'AB_TESTING']:
        print("\n[*] Initializing JIT Compilation sequence parameters...")
        _ = step_physics_numba(np.zeros((1, 2)), np.zeros((1, 2)), np.zeros((1, 2)), 25.0, PARAMS_TUPLE)
        _ = find_best_placement_numba(
            1, np.full((1, 2), STATE_IDLE, dtype=np.int8), 
            np.zeros((1, 2)), np.zeros((1, 2)),
            True,
            25.0, PARAMS_TUPLE, 250.0, 10, 25.0
        )
    
    for current_nodes in Config.NODES:
        sweep_start_time = time.time()
        
        print(f"\n" + "="*60)
        print(f"[*] EXECUTING SIMULATION ROUTINE: {current_nodes} NODES")
        print("="*60)
        
        temp_dir = Config.OUTPUT_DIR / f"_temp_{current_nodes}nodes"
        temp_dir.mkdir(parents=True, exist_ok=True)
        base_filename = f"{Config.MODE}_{current_nodes}_{total_jobs}_{Config.AMBIENT_TEMP_C}_{timestamp}"
        
        if Config.MODE == 'AB_TESTING':
            dir_standard = temp_dir / "Scheduler_STANDARD"; dir_standard.mkdir(parents=True, exist_ok=True)
            std_dict, std_agg, std_makespan = run_simulation('STANDARD', copy.deepcopy(master_queue), npy_cache_dir, dir_standard, base_filename, current_nodes)
            
            dir_thermal = temp_dir / "Scheduler_THERMAL_AWARE"; dir_thermal.mkdir(parents=True, exist_ok=True)
            ta_dict, ta_agg, ta_makespan = run_simulation('THERMAL_AWARE', copy.deepcopy(master_queue), npy_cache_dir, dir_thermal, base_filename, current_nodes)
            
            all_ids = list(set(list(std_dict.keys()) + list(ta_dict.keys())))
            master_csv_path = temp_dir / f"{base_filename}_summary.csv"
            
            with open(master_csv_path, 'w', newline='') as f:
                f.write("STD_job_id,STD_Node,STD_GPU,STD_Wait_s,STD_Exec_s,STD_Min_C,STD_Max_C,STD_Mean_C,STD_Assign_C,STD_StdDev_C,STD_Throttled,STD_ThrottleTime_s,,TA_job_id,TA_Node,TA_GPU,TA_Wait_s,TA_Exec_s,TA_Min_C,TA_Max_C,TA_Mean_C,TA_Assign_C,TA_StdDev_C,TA_Throttled,TA_ThrottleTime_s\n")
                
                for jid in all_ids:
                    s = std_dict.get(jid)
                    t = ta_dict.get(jid)
                    
                    s_cols = f"{jid},{s['node_number']},{s['gpu_index']},{s['wait_time_sec']:.1f},{s['execution_time_sec']:.1f},{s['min_temp_C']:.1f},{s['max_temp_C']:.1f},{s['mean_temp_C']:.1f},{s['assignment_temp_C']:.1f},{s['temp_std_dev_C']:.2f},{str(s['was_throttled']).lower()},{s['throttle_time_sec']:.1f}" if s else "N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A"
                    t_cols = f"{jid},{t['node_number']},{t['gpu_index']},{t['wait_time_sec']:.1f},{t['execution_time_sec']:.1f},{t['min_temp_C']:.1f},{t['max_temp_C']:.1f},{t['mean_temp_C']:.1f},{t['assignment_temp_C']:.1f},{t['temp_std_dev_C']:.2f},{str(t['was_throttled']).lower()},{t['throttle_time_sec']:.1f}" if t else "N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A"
                    
                    f.write(f"{s_cols},,{t_cols}\n")
                
                s_agg_cols = f"OVERALL,N/A,N/A,{std_agg['avg_wait_time_sec']:.1f},{std_agg['avg_execution_time_sec']:.1f},{std_agg['min_temp_C']:.1f},{std_agg['max_temp_C']:.1f},{std_agg['mean_temp_C']:.1f},{std_agg['avg_assignment_temp_C']:.1f},{std_agg['avg_temp_std_dev_C']:.2f},{std_agg['throttle_time_sec'] > 0},{std_agg['throttle_time_sec']:.1f}"
                t_agg_cols = f"OVERALL,N/A,N/A,{ta_agg['avg_wait_time_sec']:.1f},{ta_agg['avg_execution_time_sec']:.1f},{ta_agg['min_temp_C']:.1f},{ta_agg['max_temp_C']:.1f},{ta_agg['mean_temp_C']:.1f},{ta_agg['avg_assignment_temp_C']:.1f},{ta_agg['avg_temp_std_dev_C']:.2f},{ta_agg['throttle_time_sec'] > 0},{ta_agg['throttle_time_sec']:.1f}"
                f.write(f"{s_agg_cols},,{t_agg_cols}\n")
                
            master_json_path = temp_dir / f"{base_filename}_metadata.json"
            master_metadata = {
                "System_Configuration": {
                    "simulation_mode": Config.MODE,
                    "node_count": current_nodes,
                    "total_submitted_jobs": total_jobs,
                    "ambient_temp_C": Config.AMBIENT_TEMP_C,
                    "cooling_efficiency_pct": Config.COOLING_EFFICIENCY_PCT
                },
                "simulated_makespan_STANDARD_sec": std_makespan,
                "simulated_makespan_THERMAL_AWARE_sec": ta_makespan,
                "STANDARD_STATS": std_agg,
                "THERMAL_AWARE_STATS": ta_agg
            }
            with open(master_json_path, 'w') as f: json.dump(master_metadata, f, indent=2)
                
        else:
            run_simulation(Config.MODE, copy.deepcopy(master_queue), npy_cache_dir, temp_dir, base_filename, current_nodes)

        print(f"\n[*] Execution cycle complete. Saving ZIP...")
        
        zip_filename = Path(Config.OUTPUT_DIR) / f"{base_filename}.zip"
        
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    zipf.write(file_path, os.path.relpath(file_path, temp_dir))

        shutil.rmtree(temp_dir)
        print(f"\n[SUCCESS] ZIP archive saved at: {zip_filename.resolve()}")

        sweep_time_taken = time.time() - sweep_start_time
        print(f"[METADATA] Hardware Compute Duration for {current_nodes} Nodes: {sweep_time_taken:.2f} seconds.\n")

if __name__ == "__main__":
    main()