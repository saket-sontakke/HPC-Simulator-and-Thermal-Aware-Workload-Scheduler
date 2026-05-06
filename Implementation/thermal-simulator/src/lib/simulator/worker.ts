import { stepPhysics, PHYSICS_PARAMS, getPrecalculatedParams } from './physics';
import { Job, ServerNode, SimulationState, WorkerDelta } from './types';

const THROTTLE_TEMP = 87.0;
const RECOVERY_TEMP = 83.0;
const SHUTDOWN_TEMP = 90.0;
const THROTTLE_CAP = 100.0;
const IDLE_POWER = 25.0;
const PROJECTION_POWER = 250.0;
const PROJECTION_STEPS = Math.floor(300.0 / PHYSICS_PARAMS.DT);

let state: SimulationState;
let pendingJobs: Job[] = [];
let isRunning = false;
let speedMultiplier = 1;
let intervalId: ReturnType<typeof setInterval> | null = null;
let currentMode: 'STANDARD' | 'THERMAL_AWARE' = 'THERMAL_AWARE';
let tickCounter = 0;
let currentCoolingParams = { h_base_0: 0, h_active_0: 0, h_base_1: 0, h_active_1: 0 };
let totalJobsCount = 0;
let chartSampleRate = 50;

let lastSentCompletedCount = 0;
let lastSentChartLength = 0;

let isExportMode = false;
let useOPFS = false;
let exportCsvBuffers: Record<number, string[]> = {};
let exportLineCaches: Record<number, string[]> = {};

let accessHandles: Record<number, any> = {};
let exportFileHandles: Record<number, any> = {};
let textEncoder = new TextEncoder();

function initSimulation(ambientTemp: number, nodeCount: number, coolingEfficiencyPct: number = 100) {

  currentCoolingParams = getPrecalculatedParams(coolingEfficiencyPct);

  const nodes: ServerNode[] = [];
  const datasets: Record<number, { t0: number[]; t1: number[]; p0: number[]; p1: number[] }> = {};

  if (isExportMode && !useOPFS) {
    exportCsvBuffers = {};
  }

  for (let i = 0; i < nodeCount; i++) {
    nodes.push({
      id: i,
      thermalState: { T_die_0: ambientTemp, T_die_1: ambientTemp, T_sink_0: ambientTemp, T_sink_1: ambientTemp },
      gpu0: { id: 0, status: 'IDLE', currentJob: null },
      gpu1: { id: 1, status: 'IDLE', currentJob: null },
    });
    datasets[i] = { t0: [], t1: [], p0: [], p1: [] };

    if (isExportMode && !useOPFS) {
      exportCsvBuffers[i] = ["time_sec,gpu0_temp_C,gpu1_temp_C,gpu0_power_W,gpu1_power_W\n"];
    }
  }

  tickCounter = 0;
  lastSentCompletedCount = 0;
  lastSentChartLength = 0;
  pendingJobs = [];
  state = {
    time_elapsed_sec: 0, ambient_temp: ambientTemp, cooling_efficiency_pct: coolingEfficiencyPct, nodes, jobs_completed: 0, jobs_failed: 0, completed_stats: [],
    queued_job_ids: [], active_job_ids: [], failed_job_ids: [],
    chart_data: { labels: [], datasets }
  };
}

function findSafestGPU(reqGpus: 1 | 2): { nodeId: number; gpuId: 0 | 1 | 'BOTH' } | null {
  let bestNode = -1, bestGpu: 0 | 1 | 'BOTH' = 0, lowestAvg = Infinity; 
  
  for (const node of state.nodes) {
    if (reqGpus === 2) {
      if (node.gpu0.status === 'IDLE' && node.gpu1.status === 'IDLE') {
        let testState = { ...node.thermalState };
        let sumTemp = 0;
        
        for (let step = 0; step < PROJECTION_STEPS; step++) {
          testState = stepPhysics(testState, PROJECTION_POWER, PROJECTION_POWER, state.ambient_temp, currentCoolingParams);
          sumTemp += Math.max(testState.T_die_0, testState.T_die_1); 
        }
        
        const avgTemp = sumTemp / PROJECTION_STEPS;
        if (avgTemp < lowestAvg) { 
          lowestAvg = avgTemp; 
          bestNode = node.id; 
          bestGpu = 'BOTH'; 
        }
      }
    } else {
      if (node.gpu0.status === 'IDLE') {
        let testState = { ...node.thermalState };
        let sumTemp = 0;
        const p1_est = (node.gpu1.status !== 'IDLE' && node.gpu1.status !== 'SHUTDOWN') ? PROJECTION_POWER : IDLE_POWER;
        
        for (let step = 0; step < PROJECTION_STEPS; step++) {
          testState = stepPhysics(testState, PROJECTION_POWER, p1_est, state.ambient_temp, currentCoolingParams);
          sumTemp += testState.T_die_0;
        }
        
        const avgTemp = sumTemp / PROJECTION_STEPS;
        if (avgTemp < lowestAvg) { 
          lowestAvg = avgTemp; 
          bestNode = node.id; 
          bestGpu = 0; 
        }
      }
      if (node.gpu1.status === 'IDLE') {
        let testState = { ...node.thermalState };
        let sumTemp = 0;
        const p0_est = (node.gpu0.status !== 'IDLE' && node.gpu0.status !== 'SHUTDOWN') ? PROJECTION_POWER : IDLE_POWER;
        
        for (let step = 0; step < PROJECTION_STEPS; step++) {
          testState = stepPhysics(testState, p0_est, PROJECTION_POWER, state.ambient_temp, currentCoolingParams);
          sumTemp += testState.T_die_1;
        }
        
        const avgTemp = sumTemp / PROJECTION_STEPS;
        if (avgTemp < lowestAvg) { 
          lowestAvg = avgTemp; 
          bestNode = node.id; 
          bestGpu = 1; 
        }
      }
    }
  }
  return bestNode !== -1 ? { nodeId: bestNode, gpuId: bestGpu } : null;
}

function findFirstGPU(reqGpus: 1 | 2): { nodeId: number; gpuId: 0 | 1 | 'BOTH' } | null {
  for (const node of state.nodes) {
    if (reqGpus === 2 && node.gpu0.status === 'IDLE' && node.gpu1.status === 'IDLE') return { nodeId: node.id, gpuId: 'BOTH' };
    if (reqGpus === 1) {
      if (node.gpu0.status === 'IDLE') return { nodeId: node.id, gpuId: 0 };
      if (node.gpu1.status === 'IDLE') return { nodeId: node.id, gpuId: 1 };
    }
  }
  return null;
}

function syncJobIds() {
  const active = new Set<string>();
  state.nodes.forEach(n => {
    if (n.gpu0.currentJob) active.add(n.gpu0.currentJob.id);
    if (n.gpu1.currentJob) active.add(n.gpu1.currentJob.id);
  });
  state.active_job_ids = Array.from(active);
  state.queued_job_ids = pendingJobs.map(j => j.id);
}

function tick() {
  while (pendingJobs.length > 0) {
    const jobToPlace = pendingJobs[0];
    const target = currentMode === 'THERMAL_AWARE' ? findSafestGPU(jobToPlace.requested_gpus) : findFirstGPU(jobToPlace.requested_gpus);
    if (!target) break;

    const job = pendingJobs.shift()!;
    job.timeStarted = state.time_elapsed_sec;
    const node = state.nodes.find(n => n.id === target.nodeId)!;

    if (target.gpuId === 'BOTH') {
      job.assignment_temp_C = Math.max(node.thermalState.T_die_0, node.thermalState.T_die_1);
      node.gpu0.status = 'ACTIVE'; node.gpu0.currentJob = job;
      node.gpu1.status = 'ACTIVE'; node.gpu1.currentJob = job;
    } else {
      job.assignment_temp_C = target.gpuId === 0 ? node.thermalState.T_die_0 : node.thermalState.T_die_1;
      const gpu = target.gpuId === 0 ? node.gpu0 : node.gpu1;
      gpu.status = 'ACTIVE'; gpu.currentJob = job;
    }
  }

  const shouldLog = !isExportMode && (tickCounter % chartSampleRate === 0);
  if (shouldLog) {
    state.chart_data.labels.push(Math.round(state.time_elapsed_sec));
  }

  for (const node of state.nodes) {
    let p0 = IDLE_POWER, p1 = IDLE_POWER;

    if (node.gpu0.currentJob && node.gpu0.currentJob === node.gpu1.currentJob) {
      const job = node.gpu0.currentJob;

      const t0 = node.thermalState.T_die_0;
      const t1 = node.thermalState.T_die_1;

      job.tick_count_0++;
      if (t0 < job.min_temp_0) job.min_temp_0 = t0;
      if (t0 > job.max_temp_0) job.max_temp_0 = t0;
      const d0 = t0 - job.mean_0;
      job.mean_0 += d0 / job.tick_count_0;
      job.M2_0 += d0 * (t0 - job.mean_0);

      job.tick_count_1++;
      if (t1 < job.min_temp_1) job.min_temp_1 = t1;
      if (t1 > job.max_temp_1) job.max_temp_1 = t1;
      const d1 = t1 - job.mean_1;
      job.mean_1 += d1 / job.tick_count_1;
      job.M2_1 += d1 * (t1 - job.mean_1);

      if (t0 >= SHUTDOWN_TEMP) {
        node.gpu0.status = 'SHUTDOWN';
        if (!state.failed_job_ids.includes(job.id)) { state.failed_job_ids.push(job.id); state.jobs_failed++; }
      } else if (t0 >= THROTTLE_TEMP && node.gpu0.status === 'ACTIVE') node.gpu0.status = 'THROTTLED';
      else if (t0 <= RECOVERY_TEMP && node.gpu0.status === 'THROTTLED') node.gpu0.status = 'ACTIVE';
      if (node.gpu0.status === 'THROTTLED') job.throttledSteps_0++;

      if (t1 >= SHUTDOWN_TEMP) {
        node.gpu1.status = 'SHUTDOWN';
        if (!state.failed_job_ids.includes(job.id)) { state.failed_job_ids.push(job.id); state.jobs_failed++; }
      } else if (t1 >= THROTTLE_TEMP && node.gpu1.status === 'ACTIVE') node.gpu1.status = 'THROTTLED';
      else if (t1 <= RECOVERY_TEMP && node.gpu1.status === 'THROTTLED') node.gpu1.status = 'ACTIVE';
      if (node.gpu1.status === 'THROTTLED') job.throttledSteps_1++;

      if (job.workDeficit_0 <= 0 && job.workDeficit_1 <= 0 && job.currentIndex < job.power_trace_0.length) {
        job.workDeficit_0 = job.currentIndex < job.power_trace_0.length ? job.power_trace_0[job.currentIndex] : 0;
        job.workDeficit_1 = job.currentIndex < job.power_trace_1.length ? job.power_trace_1[job.currentIndex] : 0;
      }

      if (job.workDeficit_0 > 0) {
        p0 = node.gpu0.status === 'THROTTLED' ? Math.min(job.workDeficit_0, THROTTLE_CAP) : job.workDeficit_0;
        job.workDeficit_0 -= p0;
      } else if (job.currentIndex < job.power_trace_0.length) {
        p0 = IDLE_POWER; 
      }

      if (job.workDeficit_1 > 0) {
        p1 = node.gpu1.status === 'THROTTLED' ? Math.min(job.workDeficit_1, THROTTLE_CAP) : job.workDeficit_1;
        job.workDeficit_1 -= p1;
      } else if (job.currentIndex < job.power_trace_1.length) {
        p1 = IDLE_POWER; 
      }

      if (job.workDeficit_0 <= 0 && job.workDeficit_1 <= 0) {
        job.currentIndex++;
      }

    } else {
      if (node.gpu0.currentJob) {
        const job = node.gpu0.currentJob;
        const t0 = node.thermalState.T_die_0;

        job.tick_count_0++;
        if (t0 < job.min_temp_0) job.min_temp_0 = t0;
        if (t0 > job.max_temp_0) job.max_temp_0 = t0;
        const d0 = t0 - job.mean_0;
        job.mean_0 += d0 / job.tick_count_0;
        job.M2_0 += d0 * (t0 - job.mean_0);

        if (t0 >= SHUTDOWN_TEMP) {
          node.gpu0.status = 'SHUTDOWN';
          if (!state.failed_job_ids.includes(job.id)) { state.failed_job_ids.push(job.id); state.jobs_failed++; }
        } else if (t0 >= THROTTLE_TEMP && node.gpu0.status === 'ACTIVE') node.gpu0.status = 'THROTTLED';
        else if (t0 <= RECOVERY_TEMP && node.gpu0.status === 'THROTTLED') node.gpu0.status = 'ACTIVE';
        if (node.gpu0.status === 'THROTTLED') job.throttledSteps_0++;

        if (job.workDeficit_0 <= 0 && job.currentIndex < job.power_trace_0.length) {
          job.workDeficit_0 = job.power_trace_0[job.currentIndex];
        }

        if (job.workDeficit_0 > 0) {
          p0 = node.gpu0.status === 'THROTTLED' ? Math.min(job.workDeficit_0, THROTTLE_CAP) : job.workDeficit_0;
          job.workDeficit_0 -= p0;
        }

        if (job.workDeficit_0 <= 0) job.currentIndex++;
      }

      if (node.gpu1.currentJob) {
        const job = node.gpu1.currentJob;
        const t1 = node.thermalState.T_die_1;

        job.tick_count_1++;
        if (t1 < job.min_temp_1) job.min_temp_1 = t1;
        if (t1 > job.max_temp_1) job.max_temp_1 = t1;
        const d1 = t1 - job.mean_1;
        job.mean_1 += d1 / job.tick_count_1;
        job.M2_1 += d1 * (t1 - job.mean_1);

        if (t1 >= SHUTDOWN_TEMP) {
          node.gpu1.status = 'SHUTDOWN';
          if (!state.failed_job_ids.includes(job.id)) { state.failed_job_ids.push(job.id); state.jobs_failed++; }
        } else if (t1 >= THROTTLE_TEMP && node.gpu1.status === 'ACTIVE') node.gpu1.status = 'THROTTLED';
        else if (t1 <= RECOVERY_TEMP && node.gpu1.status === 'THROTTLED') node.gpu1.status = 'ACTIVE';
        if (node.gpu1.status === 'THROTTLED') job.throttledSteps_1++;

        let traceToUse = job.requested_gpus === 2 ? job.power_trace_1 : job.power_trace_0;
        if (job.workDeficit_1 <= 0 && job.currentIndex < traceToUse.length) {
          job.workDeficit_1 = traceToUse[job.currentIndex];
        }

        if (job.workDeficit_1 > 0) {
          p1 = node.gpu1.status === 'THROTTLED' ? Math.min(job.workDeficit_1, THROTTLE_CAP) : job.workDeficit_1;
          job.workDeficit_1 -= p1;
        }

        if (job.workDeficit_1 <= 0) job.currentIndex++;
      }
    }

    node.thermalState = stepPhysics(node.thermalState, p0, p1, state.ambient_temp, currentCoolingParams);

    if (shouldLog) {
      state.chart_data.datasets[node.id].t0.push(node.thermalState.T_die_0);
      state.chart_data.datasets[node.id].t1.push(node.thermalState.T_die_1);
      state.chart_data.datasets[node.id].p0.push(p0);
      state.chart_data.datasets[node.id].p1.push(p1);
    }

    if (isExportMode) {
      exportLineCaches[node.id].push(`${state.time_elapsed_sec.toFixed(2)},${node.thermalState.T_die_0.toFixed(2)},${node.thermalState.T_die_1.toFixed(2)},${p0.toFixed(2)},${p1.toFixed(2)}\n`);
      
      if (exportLineCaches[node.id].length >= 5000) {
        if (useOPFS) {
          const text = exportLineCaches[node.id].join('');
          accessHandles[node.id].write(textEncoder.encode(text));
          exportLineCaches[node.id] = [];
        } else {
          exportCsvBuffers[node.id].push(exportLineCaches[node.id].join(''));
          exportLineCaches[node.id] = [];
        }
      }
    }

    const checkFinish = (gpu: 'gpu0' | 'gpu1', idx: number) => {
      const job = node[gpu].currentJob;
      
      if (!job) return;

      if (node[gpu].status === 'SHUTDOWN') {
        node[gpu].status = 'IDLE'; 
        node[gpu].currentJob = null;
        return;
      }

      const traceLen = job.requested_gpus === 2 && gpu === 'gpu1' ? job.power_trace_1.length : job.power_trace_0.length;

      if (job.currentIndex >= traceLen) {
        const tick_count = gpu === 'gpu0' ? job.tick_count_0 : job.tick_count_1;
        const min_t = gpu === 'gpu0' ? job.min_temp_0 : job.min_temp_1;
        const max_t = gpu === 'gpu0' ? job.max_temp_0 : job.max_temp_1;
        const mean_t = gpu === 'gpu0' ? job.mean_0 : job.mean_1;
        const M2 = gpu === 'gpu0' ? job.M2_0 : job.M2_1;
        const thr = gpu === 'gpu0' ? job.throttledSteps_0 : job.throttledSteps_1;
        
        const stdDev = tick_count > 0 ? Math.sqrt(M2 / tick_count) : 0;
        const finalMin = min_t === Infinity ? 0 : min_t;
        const finalMax = max_t === -Infinity ? 0 : max_t;
        
        state.completed_stats.push({
          job_id: job.id, node_number: node.id, gpu_index: idx,
          wait_time_sec: job.timeStarted - job.timeArrived,
          execution_time_sec: state.time_elapsed_sec - job.timeStarted,
          min_temp_C: finalMin, max_temp_C: finalMax, mean_temp_C: mean_t,
          temp_std_dev_C: stdDev, was_throttled: thr > 0, throttle_time_sec: thr * PHYSICS_PARAMS.DT,
          assignment_temp_C: job.assignment_temp_C || 0
        });
        
        node[gpu].status = 'IDLE'; node[gpu].currentJob = null;
        
        const otherGpu = gpu === 'gpu0' ? 'gpu1' : 'gpu0';
        const isJobFullyDone = job.requested_gpus === 1 || (node[otherGpu].currentJob?.id !== job.id);
        
        if (isJobFullyDone) {
          state.jobs_completed++;
        }
      }
    };

    checkFinish('gpu0', 0);
    checkFinish('gpu1', 1);
  }
  
  state.time_elapsed_sec += PHYSICS_PARAMS.DT;
  tickCounter++;
  syncJobIds();
}

function buildDelta(): WorkerDelta {
  const newStats = state.completed_stats.slice(lastSentCompletedCount);
  const chartStart = lastSentChartLength;
  const newLabels = state.chart_data.labels.slice(chartStart);

  let newChartData: Record<number, { t0: number[]; t1: number[]; p0: number[]; p1: number[] }> | null = null;
  if (newLabels.length > 0) {
    newChartData = {};
    for (const node of state.nodes) {
      const d = state.chart_data.datasets[node.id];
      newChartData[node.id] = {
        t0: d.t0.slice(chartStart),
        t1: d.t1.slice(chartStart),
        p0: d.p0.slice(chartStart),
        p1: d.p1.slice(chartStart),
      };
    }
  }

  lastSentCompletedCount = state.completed_stats.length;
  lastSentChartLength = state.chart_data.labels.length;

  return {
    time_elapsed_sec: state.time_elapsed_sec,
    ambient_temp: state.ambient_temp,
    cooling_efficiency_pct: state.cooling_efficiency_pct,
    nodes: state.nodes.map(n => ({
      id: n.id,
      T_die_0: n.thermalState.T_die_0,
      T_die_1: n.thermalState.T_die_1,
      gpu0_status: n.gpu0.status,
      gpu1_status: n.gpu1.status,
      gpu0_jobId: n.gpu0.currentJob?.id || null,
      gpu1_jobId: n.gpu1.currentJob?.id || null,
    })),
    jobs_completed: state.jobs_completed,
    jobs_failed: state.jobs_failed,
    queued_job_ids: state.queued_job_ids,
    active_job_ids: state.active_job_ids,
    failed_job_ids: state.failed_job_ids,
    newCompletedStats: newStats,
    newChartLabels: newLabels,
    newChartData,
  };
}

function buildFullUISnapshot() {
  return {
    time_elapsed_sec: state.time_elapsed_sec,
    ambient_temp: state.ambient_temp,
    cooling_efficiency_pct: state.cooling_efficiency_pct,
    nodes: state.nodes.map(n => ({
      id: n.id,
      T_die_0: n.thermalState.T_die_0,
      T_die_1: n.thermalState.T_die_1,
      gpu0: { id: n.gpu0.id as (0 | 1), status: n.gpu0.status, currentJobId: n.gpu0.currentJob?.id || null },
      gpu1: { id: n.gpu1.id as (0 | 1), status: n.gpu1.status, currentJobId: n.gpu1.currentJob?.id || null },
    })),
    jobs_completed: state.jobs_completed,
    jobs_failed: state.jobs_failed,
    queued_job_ids: state.queued_job_ids,
    active_job_ids: state.active_job_ids,
    failed_job_ids: state.failed_job_ids,
    completed_stats: state.completed_stats,
    chart_data: { labels: state.chart_data.labels, datasets: state.chart_data.datasets }
  };
}

self.onmessage = async (e: MessageEvent) => {
  const { type, payload } = e.data;

  if (type === 'RUN_EXPORT') {
    isExportMode = true;
    currentMode = payload.mode;
    useOPFS = true;

    try {
      const root = await navigator.storage.getDirectory();
      for (let i = 0; i < payload.nodeCount; i++) {
        const handle = await root.getFileHandle(`export_node_${i}.csv`, { create: true });
        exportFileHandles[i] = handle;
        const accessHandle = await (handle as any).createSyncAccessHandle();
        accessHandle.truncate(0); 
        accessHandle.write(textEncoder.encode("time_sec,gpu0_temp_C,gpu1_temp_C,gpu0_power_W,gpu1_power_W\n"));
        accessHandles[i] = accessHandle;
        exportLineCaches[i] = [];
      }
    } catch (err) {
      useOPFS = false; 
    }

    initSimulation(payload.ambientTemp, payload.nodeCount, payload.coolingEfficiencyPct || 100);
    
    const totalJobs = payload.jobs.length;
    const jobsWithTime = payload.jobs.map((j: Job) => ({
      ...j, timeArrived: 0, timeStarted: 0, currentIndex: 0,
      workDeficit_0: 0, workDeficit_1: 0,
      assignment_temp_C: 0,
      tick_count_0: 0, min_temp_0: Infinity, max_temp_0: -Infinity, mean_0: 0, M2_0: 0,
      tick_count_1: 0, min_temp_1: Infinity, max_temp_1: -Infinity, mean_1: 0, M2_1: 0,
      throttledSteps_0: 0, throttledSteps_1: 0
    }));
    pendingJobs.push(...jobsWithTime);
    syncJobIds();

    const processChunk = async () => {
      let steps = 0;
      while (steps < 5000 && (pendingJobs.length > 0 || state.nodes.some(n => n.gpu0.status !== 'IDLE' || n.gpu1.status !== 'IDLE'))) {
        tick();
        steps++;
      }

      if (pendingJobs.length === 0 && !state.nodes.some(n => n.gpu0.status !== 'IDLE' || n.gpu1.status !== 'IDLE')) {
        const blobs: Record<number, Blob | File> = {};
        for (let i = 0; i < payload.nodeCount; i++) {
          if (useOPFS) {
            if (exportLineCaches[i].length > 0) accessHandles[i].write(textEncoder.encode(exportLineCaches[i].join('')));
            accessHandles[i].flush();
            accessHandles[i].close();
            blobs[i] = await exportFileHandles[i].getFile();
          } else {
            if (exportLineCaches[i].length > 0) exportCsvBuffers[i].push(exportLineCaches[i].join(''));
            blobs[i] = new Blob(exportCsvBuffers[i], { type: 'text/csv' });
          }
        }
        self.postMessage({ type: 'EXPORT_COMPLETE', payload: { mode: currentMode, blobs } });
      } else {
        const progress = totalJobs > 0 ? Math.min(99, Math.round(((state.jobs_completed + state.jobs_failed) / totalJobs) * 100)) : 0;
        self.postMessage({ type: 'EXPORT_PROGRESS', payload: { progress } });
        setTimeout(processChunk, 0); 
      }
    };
    processChunk();
    return;
  }

  isExportMode = false;

  if (type === 'INIT') {
    currentMode = payload.mode || 'THERMAL_AWARE';
    initSimulation(payload.ambientTemp, payload.nodeCount, payload.coolingEfficiencyPct || 100);
    self.postMessage({ type: 'STATE_INIT', state: buildFullUISnapshot() });
  }
  else if (type === 'ADD_JOBS') {
    totalJobsCount += payload.jobs.length;
    if (totalJobsCount <= 100) chartSampleRate = 50;
    else if (totalJobsCount <= 500) chartSampleRate = 500;
    else chartSampleRate = 5000;

    const jobsWithTime = payload.jobs.map((j: Job) => ({
      ...j, timeArrived: state.time_elapsed_sec, timeStarted: 0, currentIndex: 0,
      workDeficit_0: 0, workDeficit_1: 0,
      assignment_temp_C: 0,
      tick_count_0: 0, min_temp_0: Infinity, max_temp_0: -Infinity, mean_0: 0, M2_0: 0,
      tick_count_1: 0, min_temp_1: Infinity, max_temp_1: -Infinity, mean_1: 0, M2_1: 0,
      throttledSteps_0: 0, throttledSteps_1: 0
    }));
    pendingJobs.push(...jobsWithTime);
    syncJobIds();
  }
  else if (type === 'START') {
    if (isRunning) return;
    isRunning = true;
    speedMultiplier = payload.speed || 1;
    currentMode = payload.mode;

    if (payload.isLockstep) {
      if (intervalId) clearInterval(intervalId);
      return; 
    }

    const tickRateMs = Math.round(PHYSICS_PARAMS.DT * 1000);

    intervalId = setInterval(() => {
      const allIdle = state.nodes.every(n => n.gpu0.status === 'IDLE' && n.gpu1.status === 'IDLE');
      if (pendingJobs.length === 0 && allIdle) {
        isRunning = false;
        clearInterval(intervalId!);
        state.chart_data.labels.push(Math.round(state.time_elapsed_sec));
        for (const node of state.nodes) {
          state.chart_data.datasets[node.id].t0.push(node.thermalState.T_die_0);
          state.chart_data.datasets[node.id].t1.push(node.thermalState.T_die_1);
          state.chart_data.datasets[node.id].p0.push(IDLE_POWER);
          state.chart_data.datasets[node.id].p1.push(IDLE_POWER);
        }
        lastSentCompletedCount = 0;
        lastSentChartLength = 0;
        self.postMessage({ type: 'SIMULATION_COMPLETE', state: buildFullUISnapshot() });
        return;
      }

      const stepsPerFrame = Math.floor(speedMultiplier);
      for (let i = 0; i < stepsPerFrame; i++) tick();
      self.postMessage({ type: 'STATE_DELTA', delta: buildDelta() });
    }, tickRateMs);
  }
  else if (type === 'PAUSE') {
    isRunning = false;
    if (intervalId) {
      clearInterval(intervalId);
      intervalId = null;
    }
  }
  else if (type === 'TICK_CHUNK') {
    if (!isRunning) return;
    const ticksToRun = payload?.ticks || 1;

    for (let i = 0; i < ticksToRun; i++) {
      tick();
    }

    const allIdle = state.nodes.every(n => n.gpu0.status === 'IDLE' && n.gpu1.status === 'IDLE');
    const isFinished = pendingJobs.length === 0 && allIdle;

    self.postMessage({ type: 'CHUNK_COMPLETE', delta: buildDelta(), isFinished });
  }
  else if (type === 'GET_FULL_STATE') {
    isRunning = false;
    if (state.chart_data.labels.length === 0 || state.chart_data.labels[state.chart_data.labels.length - 1] !== Math.round(state.time_elapsed_sec)) {
      state.chart_data.labels.push(Math.round(state.time_elapsed_sec));
      for (const node of state.nodes) {
        state.chart_data.datasets[node.id].t0.push(node.thermalState.T_die_0);
        state.chart_data.datasets[node.id].t1.push(node.thermalState.T_die_1);
        state.chart_data.datasets[node.id].p0.push(IDLE_POWER);
        state.chart_data.datasets[node.id].p1.push(IDLE_POWER);
      }
    }
    lastSentCompletedCount = 0;
    lastSentChartLength = 0;
    self.postMessage({ type: 'SIMULATION_COMPLETE', state: buildFullUISnapshot() });
  }
  else if (type === 'SKIP_TO_END') {
    isRunning = false;
    currentMode = payload?.mode || currentMode;
    if (intervalId) clearInterval(intervalId);

    const processChunk = () => {
      let steps = 0;
      while (steps < 500 && (pendingJobs.length > 0 || state.nodes.some(n => n.gpu0.status !== 'IDLE' || n.gpu1.status !== 'IDLE'))) {
        tick();
        steps++;
      }

      if (pendingJobs.length === 0 && !state.nodes.some(n => n.gpu0.status !== 'IDLE' || n.gpu1.status !== 'IDLE')) {
        state.chart_data.labels.push(Math.round(state.time_elapsed_sec));
        for (const node of state.nodes) {
          state.chart_data.datasets[node.id].t0.push(node.thermalState.T_die_0);
          state.chart_data.datasets[node.id].t1.push(node.thermalState.T_die_1);
          state.chart_data.datasets[node.id].p0.push(IDLE_POWER);
          state.chart_data.datasets[node.id].p1.push(IDLE_POWER);
        }
        lastSentCompletedCount = 0;
        lastSentChartLength = 0;
        self.postMessage({ type: 'SIMULATION_COMPLETE', state: buildFullUISnapshot() });
      } else {
        self.postMessage({ type: 'SKIP_PROGRESS', payload: { completed: state.jobs_completed + state.jobs_failed } });
        setTimeout(processChunk, 0); 
      }
    };

    processChunk();
  }
};