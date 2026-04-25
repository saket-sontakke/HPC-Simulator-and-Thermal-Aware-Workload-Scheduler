import os
import sys
import time
import json
import zipfile
import tempfile
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

# =============================================================================
# 1. SIMULATION CONFIGURATION
# =============================================================================
class Config:
    # --- Infrastructure ---
    NUM_NODES = 256           # Must be an int between 1 and 256
    AMBIENT_TEMP_C = 25       # Must be an int between 15 and 45
    
    # --- Scheduler Policy ---
    MODE = 'AB_TESTING'       # Must be 'STANDARD', 'THERMAL_AWARE', or 'AB_TESTING'
    
    # --- Export Appearance ---
    THEME = 'light'            # Must be 'dark' or 'light'. Controls exported HTML graph colors.
    
    # --- Data Paths ---
    INPUT_DIR = r'C:\Users\Saket Sontakke\Downloads\New folder' 
    OUTPUT_DIR = r'C:\Users\Saket Sontakke\Downloads\New'
    
    # --- Telemetry & Optimization ---
    HIGH_RES_TELEMETRY = False 
    NUM_CORES = None 

    @classmethod
    def validate(cls):
        """Strictly validates configuration parameters before the script runs."""
        valid_modes = ['STANDARD', 'THERMAL_AWARE', 'AB_TESTING']
        if cls.MODE not in valid_modes:
            raise ValueError(f"FATAL: Invalid MODE '{cls.MODE}'. Must be one of: {valid_modes}")

        if not isinstance(cls.NUM_NODES, int) or type(cls.NUM_NODES) is bool:
            raise TypeError(f"FATAL: NUM_NODES must be a whole number (integer).")
        if not (1 <= cls.NUM_NODES <= 256):
            raise ValueError(f"FATAL: NUM_NODES must be between 1 and 256.")

        if not isinstance(cls.AMBIENT_TEMP_C, int) or type(cls.AMBIENT_TEMP_C) is bool:
            raise TypeError(f"FATAL: AMBIENT_TEMP_C must be a whole number (integer).")
        if not (15 <= cls.AMBIENT_TEMP_C <= 45):
            raise ValueError(f"FATAL: AMBIENT_TEMP_C must be between 15 and 45 °C.")
            
        valid_themes = ['dark', 'light']
        if cls.THEME not in valid_themes:
            raise ValueError(f"FATAL: Invalid THEME '{cls.THEME}'.")

if Config.NUM_CORES is not None:
    os.environ["OMP_NUM_THREADS"] = str(Config.NUM_CORES)
    os.environ["OPENBLAS_NUM_THREADS"] = str(Config.NUM_CORES)
    os.environ["MKL_NUM_THREADS"] = str(Config.NUM_CORES)

# =============================================================================
# 2. PHYSICS PARAMETERS & FAST JIT ENGINE
# =============================================================================
PHYSICS_PARAMS = {
    "DT": 0.11,
    "IDLE_POWER": 25.0,
    "PROJECTION_POWER": 200.0,
    "THROTTLE_TEMP": 87.0,
    "RECOVERY_TEMP": 83.0,
    "SHUTDOWN_TEMP": 90.0,
    "THROTTLE_CAP": 100.0,
    "C_die_0": 8.9324045, "C_die_1": 8.8706836,
    "C_sink_0": 4713.588, "C_sink_1": 4831.154,
    "R_paste_0": 0.03658, "R_paste_1": 0.03357,
    "k01": 0.01670, "k10": 0.00283,
    "q0": -8.92023, "q1": -8.94283,
    "h_base_0": 3.97913, "h_base_1": 4.76174,
    "h_active_0": 20.9113, "h_active_1": 19.8178,
    "T_thresh_0": 70.2506, "T_thresh_1": 67.5134,
    "beta_0": 1.66880, "beta_1": 1.33509
}

# Flatten params for ultra-fast Numba execution
PARAMS_TUPLE = (
    PHYSICS_PARAMS["DT"], PHYSICS_PARAMS["C_die_0"], PHYSICS_PARAMS["C_die_1"], 
    PHYSICS_PARAMS["C_sink_0"], PHYSICS_PARAMS["C_sink_1"], PHYSICS_PARAMS["R_paste_0"], 
    PHYSICS_PARAMS["R_paste_1"], PHYSICS_PARAMS["k01"], PHYSICS_PARAMS["k10"], 
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
    (DT, C_die_0, C_die_1, C_sink_0, C_sink_1, R_paste_0, R_paste_1, 
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
        
        dT0_die = (p0 - (t_d0 - t_s0) / R_paste_0) / C_die_0
        dT0_sink = ((t_d0 - t_s0) / R_paste_0 + k01 * p1 - h0_curr * (t_s0 - T_amb) + q0) / C_sink_0
        
        dT1_die = (p1 - (t_d1 - t_s1) / R_paste_1) / C_die_1
        dT1_sink = ((t_d1 - t_s1) / R_paste_1 + k10 * p0 - h1_curr * (t_s1 - T_amb) + q1) / C_sink_1
        
        T_die_new[i, 0] = t_d0 + DT * dT0_die
        T_die_new[i, 1] = t_d1 + DT * dT1_die
        T_sink_new[i, 0] = t_s0 + DT * dT0_sink
        T_sink_new[i, 1] = t_s1 + DT * dT1_sink

    return T_die_new, T_sink_new

@njit(cache=True)
def run_projection_loop_numba(T_die_proj, T_sink_proj, P_draw_proj, T_amb, steps, params_tuple):
    num_cands = T_die_proj.shape[0]
    peak_temps = np.empty(num_cands)
    for i in range(num_cands):
        peak_temps[i] = max(T_die_proj[i, 0], T_die_proj[i, 1])

    for _ in range(steps):
        T_die_proj, T_sink_proj = step_physics_numba(T_die_proj, T_sink_proj, P_draw_proj, T_amb, params_tuple)
        for i in range(num_cands):
            if T_die_proj[i, 0] > peak_temps[i]: peak_temps[i] = T_die_proj[i, 0]
            if T_die_proj[i, 1] > peak_temps[i]: peak_temps[i] = T_die_proj[i, 1]
            
    return peak_temps

def find_best_placement_vectorized(job, gpu_status, T_die, T_sink, mode, T_amb):
    req = job['requested_gpus']
    candidates = []
    
    for n in range(len(gpu_status)):
        if req == 2:
            if gpu_status[n, 0] == 'IDLE' and gpu_status[n, 1] == 'IDLE':
                candidates.append((n, 'BOTH'))
        elif req == 1:
            if gpu_status[n, 0] == 'IDLE': candidates.append((n, 0))
            if gpu_status[n, 1] == 'IDLE': candidates.append((n, 1))
                
    if not candidates:
        return None
        
    if mode == 'STANDARD':
        return candidates[0]
        
    num_cands = len(candidates)
    T_die_proj = np.zeros((num_cands, 2))
    T_sink_proj = np.zeros((num_cands, 2))
    P_draw_proj = np.full((num_cands, 2), PHYSICS_PARAMS['IDLE_POWER'])
    
    for i, (n, gpu_idx) in enumerate(candidates):
        T_die_proj[i] = T_die[n]
        T_sink_proj[i] = T_sink[n]
        if gpu_idx == 'BOTH':
            P_draw_proj[i, 0] = PHYSICS_PARAMS['PROJECTION_POWER']
            P_draw_proj[i, 1] = PHYSICS_PARAMS['PROJECTION_POWER']
        elif gpu_idx == 0:
            P_draw_proj[i, 0] = PHYSICS_PARAMS['PROJECTION_POWER']
            if gpu_status[n, 1] not in ('IDLE', 'SHUTDOWN'):
                P_draw_proj[i, 1] = PHYSICS_PARAMS['PROJECTION_POWER']
        elif gpu_idx == 1:
            P_draw_proj[i, 1] = PHYSICS_PARAMS['PROJECTION_POWER']
            if gpu_status[n, 0] not in ('IDLE', 'SHUTDOWN'):
                P_draw_proj[i, 0] = PHYSICS_PARAMS['PROJECTION_POWER']
            
    PROJECTION_STEPS = int((5 * 60) / PHYSICS_PARAMS['DT']) 
    peak_temps = run_projection_loop_numba(T_die_proj, T_sink_proj, P_draw_proj, T_amb, PROJECTION_STEPS, PARAMS_TUPLE)
        
    best_idx = np.argmin(peak_temps)
    return candidates[best_idx]


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

    # NEW: Calculate badge colors dynamically to match the React UI
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
        
        /* UPDATED: Dynamic Badge CSS */
        .badge {{ 
          background: {badge_bg}; 
          color: {badge_text}; 
          padding: 4px 10px; 
          border-radius: 6px; 
          font-size: 0.875rem; 
          font-weight: bold; 
          text-transform: uppercase; 
          letter-spacing: 0.05em;
        }}
        
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
# 4. JOB INGESTION (With Cache)
# =============================================================================
def load_jobs_from_directory(directory):
    path = Path(directory).resolve()
    print(f"\n[*] Scanning directory: {path}\n")
    
    if not path.exists():
        print(f"[!] FATAL: Input directory does not exist.")
        sys.exit(1)
        
    cache_path = path / "parsed_jobs_cache.pkl"
    
    if cache_path.exists():
        print(f"[*] Found pre-parsed cache! Loading from {cache_path.name}...")
        with open(cache_path, 'rb') as f:
            jobs = pickle.load(f)
        print(f"[*] Loaded {len(jobs)} jobs demanding {sum(j['requested_gpus'] for j in jobs)} GPUs from cache.")
        return jobs
        
    files = list(path.glob('*.csv')) + list(path.glob('*.parquet'))
    files.sort(key=lambda f: int(f.stem.split('-')[0]))
    
    if not files:
        print(f"[!] No valid trace files found in {directory}")
        sys.exit(1)
        
    jobs = []
    for file in tqdm(files, desc="Parsing Traces ", unit="file"):
        job_id = file.stem
        df = pd.read_csv(file) if file.suffix == '.csv' else pd.read_parquet(file)
            
        trace0 = df[df['gpu_index'] == 0]['power_draw_W'].tolist()
        trace1 = df[df['gpu_index'] == 1]['power_draw_W'].tolist()
        
        req_gpus = 2 if (trace0 and trace1) else 1
        t0_final = trace0 if trace0 else trace1
        t1_final = trace1 if (trace0 and trace1) else []
        
        jobs.append({
            'id': job_id, 'requested_gpus': req_gpus,
            'power_trace_0': t0_final, 'power_trace_1': t1_final,
            'timeArrived': 0.0, 'timeStarted': 0.0,
            'currentIndex': 0, 'workDeficit_0': 0.0, 'workDeficit_1': 0.0,
            
            # Welford Memory Diet Replacements
            'tick_count_0': 0, 'min_temp_0': float('inf'), 'max_temp_0': -float('inf'), 'mean_0': 0.0, 'M2_0': 0.0,
            'tick_count_1': 0, 'min_temp_1': float('inf'), 'max_temp_1': -float('inf'), 'mean_1': 0.0, 'M2_1': 0.0,
            
            'throttledSteps_0': 0, 'throttledSteps_1': 0
        })
        
    print(f"\n[*] Saving parsed jobs to cache for faster future runs...")
    with open(cache_path, 'wb') as f:
        pickle.dump(jobs, f)
        
    print(f"[*] Loaded {len(jobs)} jobs demanding {sum(j['requested_gpus'] for j in jobs)} GPUs.")
    return jobs

# =============================================================================
# 5. CORE SIMULATION FUNCTION
# =============================================================================
def run_simulation(mode, jobs_list, output_dir):
    total_jobs = len(jobs_list)
    jobs_queue = deque(jobs_list)
    
    telemetry_dir = output_dir / "Telemetry_Data_CSV"
    html_dir = output_dir / "Interactive_Graphs_HTML"
    telemetry_dir.mkdir(parents=True, exist_ok=True)
    html_dir.mkdir(parents=True, exist_ok=True)
    
    T_die = np.full((Config.NUM_NODES, 2), Config.AMBIENT_TEMP_C, dtype=np.float64)
    T_sink = np.full((Config.NUM_NODES, 2), Config.AMBIENT_TEMP_C, dtype=np.float64)
    P_draw = np.full((Config.NUM_NODES, 2), PHYSICS_PARAMS["IDLE_POWER"], dtype=np.float64)
    
    gpu_status = np.full((Config.NUM_NODES, 2), 'IDLE', dtype=object)
    gpu_jobs = np.full((Config.NUM_NODES, 2), None, dtype=object)
    
    chart_labels = []
    chart_datasets = {i: {'t0':[], 't1':[], 'p0':[], 'p1':[]} for i in range(Config.NUM_NODES)}
    
    file_handles = []
    telemetry_buffers = {i: [] for i in range(Config.NUM_NODES)}
    file_suffix = "" if Config.HIGH_RES_TELEMETRY else "_Sampled"

    for i in range(Config.NUM_NODES):
        f = open(telemetry_dir / f"Node_{i}_Telemetry{file_suffix}.csv", "w")
        f.write("time_sec,gpu0_temp_C,gpu1_temp_C,gpu0_power_W,gpu1_power_W\n")
        file_handles.append(f)

    time_elapsed = 0.0
    tick_counter = 0
    jobs_completed = 0
    jobs_failed = 0
    
    failed_job_ids = set()
    completed_stats = []

    with tqdm(total=total_jobs, desc=f"[{mode}] Simulating", unit="job", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]") as pbar:
        while jobs_queue or np.any((gpu_status == 'ACTIVE') | (gpu_status == 'THROTTLED')):
            
            # --- 1. JOB PLACEMENT ---
            while jobs_queue:
                job = jobs_queue[0]
                cand = find_best_placement_vectorized(job, gpu_status, T_die, T_sink, mode, Config.AMBIENT_TEMP_C)
                
                if cand:
                    job = jobs_queue.popleft() 
                    job['timeStarted'] = time_elapsed
                    n, gpu_idx = cand
                    if gpu_idx == 'BOTH':
                        gpu_status[n, 0], gpu_status[n, 1] = 'ACTIVE', 'ACTIVE'
                        gpu_jobs[n, 0], gpu_jobs[n, 1] = job, job
                    else:
                        gpu_status[n, gpu_idx] = 'ACTIVE'
                        gpu_jobs[n, gpu_idx] = job
                else:
                    break 
                    
            # --- 2. WORK DEFICIT & HYSTERESIS (With Welford's Update) ---
            P_draw.fill(PHYSICS_PARAMS["IDLE_POWER"])
            
            for n in range(Config.NUM_NODES):
                p0, p1 = PHYSICS_PARAMS["IDLE_POWER"], PHYSICS_PARAMS["IDLE_POWER"]
                job0, job1 = gpu_jobs[n, 0], gpu_jobs[n, 1]
                
                if job0 is not None and job0 == job1:
                    job = job0
                    t0, t1 = T_die[n, 0], T_die[n, 1]
                    
                    # Welford's continuous calculation for GPU 0
                    job['tick_count_0'] += 1
                    job['min_temp_0'] = min(job['min_temp_0'], t0)
                    job['max_temp_0'] = max(job['max_temp_0'], t0)
                    d0 = t0 - job['mean_0']
                    job['mean_0'] += d0 / job['tick_count_0']
                    job['M2_0'] += d0 * (t0 - job['mean_0'])

                    # Welford's continuous calculation for GPU 1
                    job['tick_count_1'] += 1
                    job['min_temp_1'] = min(job['min_temp_1'], t1)
                    job['max_temp_1'] = max(job['max_temp_1'], t1)
                    d1 = t1 - job['mean_1']
                    job['mean_1'] += d1 / job['tick_count_1']
                    job['M2_1'] += d1 * (t1 - job['mean_1'])
                    
                    if t0 >= PHYSICS_PARAMS["SHUTDOWN_TEMP"]:
                        gpu_status[n, 0] = 'SHUTDOWN'
                        if job['id'] not in failed_job_ids: 
                            failed_job_ids.add(job['id']); jobs_failed += 1
                    elif t0 >= PHYSICS_PARAMS["THROTTLE_TEMP"] and gpu_status[n, 0] == 'ACTIVE':
                        gpu_status[n, 0] = 'THROTTLED'
                    elif t0 <= PHYSICS_PARAMS["RECOVERY_TEMP"] and gpu_status[n, 0] == 'THROTTLED':
                        gpu_status[n, 0] = 'ACTIVE'
                    
                    if gpu_status[n, 0] == 'THROTTLED': job['throttledSteps_0'] += 1
                        
                    if t1 >= PHYSICS_PARAMS["SHUTDOWN_TEMP"]:
                        gpu_status[n, 1] = 'SHUTDOWN'
                        if job['id'] not in failed_job_ids: 
                            failed_job_ids.add(job['id']); jobs_failed += 1
                    elif t1 >= PHYSICS_PARAMS["THROTTLE_TEMP"] and gpu_status[n, 1] == 'ACTIVE':
                        gpu_status[n, 1] = 'THROTTLED'
                    elif t1 <= PHYSICS_PARAMS["RECOVERY_TEMP"] and gpu_status[n, 1] == 'THROTTLED':
                        gpu_status[n, 1] = 'ACTIVE'
                        
                    if gpu_status[n, 1] == 'THROTTLED': job['throttledSteps_1'] += 1

                    if job['workDeficit_0'] <= 0 and job['workDeficit_1'] <= 0 and job['currentIndex'] < len(job['power_trace_0']):
                        job['workDeficit_0'] = job['power_trace_0'][job['currentIndex']]
                        job['workDeficit_1'] = job['power_trace_1'][job['currentIndex']]

                    if job['workDeficit_0'] > 0:
                        p0 = min(job['workDeficit_0'], PHYSICS_PARAMS["THROTTLE_CAP"]) if gpu_status[n, 0] == 'THROTTLED' else job['workDeficit_0']
                        job['workDeficit_0'] -= p0
                    if job['workDeficit_1'] > 0:
                        p1 = min(job['workDeficit_1'], PHYSICS_PARAMS["THROTTLE_CAP"]) if gpu_status[n, 1] == 'THROTTLED' else job['workDeficit_1']
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
                            gpu_status[n, 0] = 'SHUTDOWN'
                            if job0['id'] not in failed_job_ids: failed_job_ids.add(job0['id']); jobs_failed += 1
                        elif t0 >= PHYSICS_PARAMS["THROTTLE_TEMP"] and gpu_status[n, 0] == 'ACTIVE':
                            gpu_status[n, 0] = 'THROTTLED'
                        elif t0 <= PHYSICS_PARAMS["RECOVERY_TEMP"] and gpu_status[n, 0] == 'THROTTLED':
                            gpu_status[n, 0] = 'ACTIVE'
                        if gpu_status[n, 0] == 'THROTTLED': job0['throttledSteps_0'] += 1
                        if job0['workDeficit_0'] <= 0 and job0['currentIndex'] < len(job0['power_trace_0']):
                            job0['workDeficit_0'] = job0['power_trace_0'][job0['currentIndex']]
                        if job0['workDeficit_0'] > 0:
                            p0 = min(job0['workDeficit_0'], PHYSICS_PARAMS["THROTTLE_CAP"]) if gpu_status[n, 0] == 'THROTTLED' else job0['workDeficit_0']
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
                            gpu_status[n, 1] = 'SHUTDOWN'
                            if job1['id'] not in failed_job_ids: failed_job_ids.add(job1['id']); jobs_failed += 1
                        elif t1 >= PHYSICS_PARAMS["THROTTLE_TEMP"] and gpu_status[n, 1] == 'ACTIVE':
                            gpu_status[n, 1] = 'THROTTLED'
                        elif t1 <= PHYSICS_PARAMS["RECOVERY_TEMP"] and gpu_status[n, 1] == 'THROTTLED':
                            gpu_status[n, 1] = 'ACTIVE'
                        if gpu_status[n, 1] == 'THROTTLED': job1['throttledSteps_1'] += 1
                        traceToUse = job1['power_trace_1'] if job1['requested_gpus'] == 2 else job1['power_trace_0']
                        if job1['workDeficit_1'] <= 0 and job1['currentIndex'] < len(traceToUse):
                            job1['workDeficit_1'] = traceToUse[job1['currentIndex']]
                        if job1['workDeficit_1'] > 0:
                            p1 = min(job1['workDeficit_1'], PHYSICS_PARAMS["THROTTLE_CAP"]) if gpu_status[n, 1] == 'THROTTLED' else job1['workDeficit_1']
                            job1['workDeficit_1'] -= p1
                        if job1['workDeficit_1'] <= 0: job1['currentIndex'] += 1
                P_draw[n, 0], P_draw[n, 1] = p0, p1

            # --- 3. STEP PHYSICS ---
            T_die, T_sink = step_physics_numba(T_die, T_sink, P_draw, Config.AMBIENT_TEMP_C, PARAMS_TUPLE)

            # --- 4. CHECK COMPLETIONS ---
            for n in range(Config.NUM_NODES):
                for g_id in [0, 1]:
                    job = gpu_jobs[n, g_id]
                    if not job or gpu_status[n, g_id] == 'SHUTDOWN': continue
                    trace_len = len(job['power_trace_1']) if (job['requested_gpus'] == 2 and g_id == 1) else len(job['power_trace_0'])
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
                            'wait_time_sec': job['timeStarted'] - job['timeArrived'],
                            'execution_time_sec': time_elapsed - job['timeStarted'],
                            'min_temp_C': min_t, 'max_temp_C': max_t,
                            'mean_temp_C': mean_t, 'temp_std_dev_C': std_t,
                            'was_throttled': thr > 0, 'throttle_time_sec': thr * PHYSICS_PARAMS["DT"]
                        })
                        
                        gpu_status[n, g_id] = 'IDLE'; gpu_jobs[n, g_id] = None
                        if job['requested_gpus'] == 1 or (gpu_status[n, 0] == 'IDLE' and gpu_status[n, 1] == 'IDLE'):
                            jobs_completed += 1; pbar.update(1)

            # --- 5. TELEMETRY ---
            should_log = True if Config.HIGH_RES_TELEMETRY else (tick_counter % 50 == 0)
            if should_log:
                for n in range(Config.NUM_NODES):
                    telemetry_buffers[n].append(f"{time_elapsed:.2f},{T_die[n,0]:.2f},{T_die[n,1]:.2f},{P_draw[n,0]:.2f},{P_draw[n,1]:.2f}\n")
                    
                    if len(telemetry_buffers[n]) >= 10000:
                        file_handles[n].writelines(telemetry_buffers[n])
                        telemetry_buffers[n].clear()

            if tick_counter % 50 == 0:
                chart_labels.append(round(time_elapsed))
                for n in range(Config.NUM_NODES):
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

    for n in range(Config.NUM_NODES):
        if telemetry_buffers[n]:
            file_handles[n].writelines(telemetry_buffers[n])
        file_handles[n].close()

    # --- Write Outputs ---
    n_stats = len(completed_stats)
    avg_wait = sum(s['wait_time_sec'] for s in completed_stats) / n_stats if n_stats > 0 else 0
    avg_exec = sum(s['execution_time_sec'] for s in completed_stats) / n_stats if n_stats > 0 else 0
    
    overall_min = sum(s['min_temp_C'] for s in completed_stats) / n_stats if n_stats > 0 else 0
    overall_max = sum(s['max_temp_C'] for s in completed_stats) / n_stats if n_stats > 0 else 0
    overall_mean = sum(s['mean_temp_C'] for s in completed_stats) / n_stats if n_stats > 0 else 0
    avg_std_dev = sum(s['temp_std_dev_C'] for s in completed_stats) / n_stats if n_stats > 0 else 0
    total_throttled = sum(1 for s in completed_stats if s['was_throttled'])
    total_throttle_time = sum(s['throttle_time_sec'] for s in completed_stats)

    agg_stats_dict = {
        "completedCount": n_stats, "avgWait": avg_wait, "avgExec": avg_exec,
        "overallMin": overall_min, "overallMax": overall_max,
        "overallMean": overall_mean, "avgStdDev": avg_std_dev,
        "totalThrottledJobs": total_throttled, "totalThrottleTime": total_throttle_time
    }

    config_data = {
        "simulation_mode": mode, "ambient_temp_C": Config.AMBIENT_TEMP_C, "node_count": Config.NUM_NODES,
        "time_elapsed_sec": time_elapsed, "total_submitted_jobs": total_jobs, "global_stats": agg_stats_dict
    }
    with open(output_dir / f"Simulation_Config_and_Stats_{mode}.json", 'w') as f: json.dump(config_data, f, indent=2)

    overall_row = f"OVERALL,{n_stats} Jobs,N/A,{avg_wait:.1f},{avg_exec:.1f},{overall_min:.1f},{overall_max:.1f},{overall_mean:.1f},{avg_std_dev:.2f},{total_throttled},{total_throttle_time:.1f}\n"

    with open(output_dir / f"Jobs_Summary_{mode}.csv", 'w', newline='') as f:
        f.write("job_id,node_number,gpu_index,wait_time_sec,execution_time_sec,min_temp_C,max_temp_C,mean_temp_C,temp_std_dev_C,was_throttled,throttle_time_sec\n")
        for s in completed_stats:
            f.write(f"{s['job_id']},{s['node_number']},{s['gpu_index']},{s['wait_time_sec']:.1f},{s['execution_time_sec']:.1f},"
                    f"{s['min_temp_C']:.1f},{s['max_temp_C']:.1f},{s['mean_temp_C']:.1f},{s['temp_std_dev_C']:.2f},{str(s['was_throttled']).lower()},{s['throttle_time_sec']:.1f}\n")
        f.write(overall_row)

    for n in range(Config.NUM_NODES):
        html_str = generate_html_template(n, chart_labels, chart_datasets[n]['t0'], chart_datasets[n]['t1'], chart_datasets[n]['p0'], chart_datasets[n]['p1'], mode, theme=Config.THEME)
        with open(html_dir / f"Node_{n}_Telemetry.html", 'w', encoding='utf-8') as f: f.write(html_str)

    return completed_stats, agg_stats_dict

# =============================================================================
# 6. MAIN EXECUTION
# =============================================================================
# =============================================================================
# 6. MAIN EXECUTION
# =============================================================================
def main():
    Config.validate()
    print("=" * 70)
    print(f"--- ThermalODE SIMULATOR: {Config.MODE} ---")
    print("=" * 70)
    
    master_queue = load_jobs_from_directory(Config.INPUT_DIR)
    
    if Config.MODE in ['STANDARD', 'THERMAL_AWARE', 'AB_TESTING']:
        print("\n[*] JIT Compiling Physics Engine (Warmup)...")
        _ = step_physics_numba(np.zeros((1, 2)), np.zeros((1, 2)), np.zeros((1, 2)), 25.0, PARAMS_TUPLE)
        _ = run_projection_loop_numba(np.zeros((1, 2)), np.zeros((1, 2)), np.zeros((1, 2)), 25.0, 10, PARAMS_TUPLE)
    
    start_time = time.time()
    
    print(f"[*] STARTING SIMULATION: {Config.NUM_NODES} NODES\n")
    
    temp_dir = Path(tempfile.mkdtemp())
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    total_jobs = len(master_queue)
    mode_str = Config.MODE
    
    if Config.MODE == 'AB_TESTING':
        dir_standard = temp_dir / "Scheduler_STANDARD"; dir_standard.mkdir(parents=True, exist_ok=True)
        std_stats, std_agg = run_simulation('STANDARD', copy.deepcopy(master_queue), dir_standard)
        
        dir_thermal = temp_dir / "Scheduler_THERMAL_AWARE"; dir_thermal.mkdir(parents=True, exist_ok=True)
        ta_stats, ta_agg = run_simulation('THERMAL_AWARE', copy.deepcopy(master_queue), dir_thermal)
        
        std_dict = {s['job_id']: s for s in std_stats}
        ta_dict = {s['job_id']: s for s in ta_stats}
        all_ids = list(set(list(std_dict.keys()) + list(ta_dict.keys())))
        
        comparison_path = temp_dir / f"{mode_str}_{Config.NUM_NODES}_{total_jobs}_{Config.AMBIENT_TEMP_C}_{timestamp}.csv"
        
        with open(comparison_path, 'w', newline='') as f:
            f.write("STD_job_id,STD_Node,STD_GPU,STD_Wait_s,STD_Exec_s,STD_Min_C,STD_Max_C,STD_Mean_C,STD_StdDev_C,STD_Throttled,STD_ThrottleTime_s,,TA_job_id,TA_Node,TA_GPU,TA_Wait_s,TA_Exec_s,TA_Min_C,TA_Max_C,TA_Mean_C,TA_StdDev_C,TA_Throttled,TA_ThrottleTime_s\n")
            
            for jid in all_ids:
                s = std_dict.get(jid)
                t = ta_dict.get(jid)
                
                s_cols = f"{jid},{s['node_number']},{s['gpu_index']},{s['wait_time_sec']:.1f},{s['execution_time_sec']:.1f},{s['min_temp_C']:.1f},{s['max_temp_C']:.1f},{s['mean_temp_C']:.1f},{s['temp_std_dev_C']:.2f},{str(s['was_throttled']).lower()},{s['throttle_time_sec']:.1f}" if s else "N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A"
                t_cols = f"{jid},{t['node_number']},{t['gpu_index']},{t['wait_time_sec']:.1f},{t['execution_time_sec']:.1f},{t['min_temp_C']:.1f},{t['max_temp_C']:.1f},{t['mean_temp_C']:.1f},{t['temp_std_dev_C']:.2f},{str(t['was_throttled']).lower()},{t['throttle_time_sec']:.1f}" if t else "N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A"
                
                f.write(f"{s_cols},,{t_cols}\n")
            
            s_agg_cols = f"OVERALL,N/A,N/A,{std_agg['avgWait']:.1f},{std_agg['avgExec']:.1f},{std_agg['overallMin']:.1f},{std_agg['overallMax']:.1f},{std_agg['overallMean']:.1f},{std_agg['avgStdDev']:.2f},{std_agg['totalThrottledJobs']},{std_agg['totalThrottleTime']:.1f}"
            t_agg_cols = f"OVERALL,N/A,N/A,{ta_agg['avgWait']:.1f},{ta_agg['avgExec']:.1f},{ta_agg['overallMin']:.1f},{ta_agg['overallMax']:.1f},{ta_agg['overallMean']:.1f},{ta_agg['avgStdDev']:.2f},{ta_agg['totalThrottledJobs']},{ta_agg['totalThrottleTime']:.1f}"
            
            f.write(f"{s_agg_cols},,{t_agg_cols}\n")
            
    else:
        run_simulation(Config.MODE, copy.deepcopy(master_queue), temp_dir)

    print(f"\n[*] {Config.NUM_NODES}-Node Simulation Complete. Structuring ZIP Archive...")
    Path(Config.OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    zip_filename = Path(Config.OUTPUT_DIR) / f"{mode_str}_{Config.NUM_NODES}_{total_jobs}_{Config.AMBIENT_TEMP_C}_{timestamp}.zip"
    
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, os.path.relpath(file_path, temp_dir))

    import shutil
    shutil.rmtree(temp_dir)
    print(f"\n[SUCCESS] Archive saved to: {zip_filename.resolve()}")

    real_time_taken = time.time() - start_time
    print(f"[METADATA] Total Compute Time: {real_time_taken:.2f} seconds.\n")

if __name__ == "__main__":
    main()