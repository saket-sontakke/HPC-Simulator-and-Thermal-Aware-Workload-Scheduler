// src/lib/simulator/worker.ts
import { stepPhysics, PHYSICS_PARAMS } from './physics';
import { Job, ServerNode, SimulationState, WorkerDelta, UINodeState, UIGPUState } from './types';

const THROTTLE_TEMP = 87.0;
const RECOVERY_TEMP = 83.0;
const SHUTDOWN_TEMP = 90.0;
const THROTTLE_CAP = 100.0;
const IDLE_POWER = 25.0;
const PROJECTION_POWER = 200.0;
const PROJECTION_STEPS = Math.floor((5 * 60) / PHYSICS_PARAMS.DT);

let state: SimulationState;
let pendingJobs: Job[] = [];
let isRunning = false;
let speedMultiplier = 1;
let intervalId: ReturnType<typeof setInterval> | null = null;
let currentMode: 'STANDARD' | 'THERMAL_AWARE' = 'THERMAL_AWARE';
let tickCounter = 0;

let lastSentCompletedCount = 0;
let lastSentChartLength = 0;

function initSimulation(ambientTemp: number, nodeCount: number) {
  const nodes: ServerNode[] = [];
  const datasets: Record<number, { t0: number[]; t1: number[]; p0: number[]; p1: number[] }> = {};

  for (let i = 0; i < nodeCount; i++) {
    nodes.push({
      id: i,
      thermalState: { T_die_0: ambientTemp, T_die_1: ambientTemp, T_sink_0: ambientTemp, T_sink_1: ambientTemp },
      gpu0: { id: 0, status: 'IDLE', currentJob: null },
      gpu1: { id: 1, status: 'IDLE', currentJob: null },
    });
    datasets[i] = { t0: [], t1: [], p0: [], p1: [] };
  }

  tickCounter = 0;
  lastSentCompletedCount = 0;
  lastSentChartLength = 0;
  pendingJobs = [];
  state = {
    time_elapsed_sec: 0, ambient_temp: ambientTemp, nodes, jobs_completed: 0, jobs_failed: 0, completed_stats: [],
    queued_job_ids: [], active_job_ids: [], failed_job_ids: [],
    chart_data: { labels: [], datasets }
  };
}

function getStats(arr: number[]) {
  if (arr.length === 0) return { min: 0, max: 0, mean: 0, stdDev: 0 };
  let min = arr[0], max = arr[0], sum = 0;
  for (const v of arr) {
    if (v < min) min = v;
    if (v > max) max = v;
    sum += v;
  }
  const mean = sum / arr.length;
  let sqDiff = 0;
  for (const v of arr) sqDiff += Math.pow(v - mean, 2);
  return { min, max, mean, stdDev: Math.sqrt(sqDiff / arr.length) };
}

function findSafestGPU(reqGpus: 1 | 2): { nodeId: number; gpuId: 0 | 1 | 'BOTH' } | null {
  let bestNode = -1, bestGpu: 0 | 1 | 'BOTH' = 0, lowestPeak = Infinity;
  for (const node of state.nodes) {
    if (reqGpus === 2) {
      if (node.gpu0.status === 'IDLE' && node.gpu1.status === 'IDLE') {
        let testState = { ...node.thermalState };
        let peak = 0;
        for (let step = 0; step < PROJECTION_STEPS; step++) {
          testState = stepPhysics(testState, PROJECTION_POWER, PROJECTION_POWER, state.ambient_temp);
          if (testState.T_die_0 > peak) peak = testState.T_die_0;
          if (testState.T_die_1 > peak) peak = testState.T_die_1;
        }
        if (peak < lowestPeak) { lowestPeak = peak; bestNode = node.id; bestGpu = 'BOTH'; }
      }
    } else {
      if (node.gpu0.status === 'IDLE') {
        let testState = { ...node.thermalState };
        let peak = 0;
        const p1_est = (node.gpu1.status !== 'IDLE' && node.gpu1.status !== 'SHUTDOWN') ? PROJECTION_POWER : IDLE_POWER;
        for (let step = 0; step < PROJECTION_STEPS; step++) {
          testState = stepPhysics(testState, PROJECTION_POWER, p1_est, state.ambient_temp);
          if (testState.T_die_0 > peak) peak = testState.T_die_0;
        }
        if (peak < lowestPeak) { lowestPeak = peak; bestNode = node.id; bestGpu = 0; }
      }
      if (node.gpu1.status === 'IDLE') {
        let testState = { ...node.thermalState };
        let peak = 0;
        const p0_est = (node.gpu0.status !== 'IDLE' && node.gpu0.status !== 'SHUTDOWN') ? PROJECTION_POWER : IDLE_POWER;
        for (let step = 0; step < PROJECTION_STEPS; step++) {
          testState = stepPhysics(testState, p0_est, PROJECTION_POWER, state.ambient_temp);
          if (testState.T_die_1 > peak) peak = testState.T_die_1;
        }
        if (peak < lowestPeak) { lowestPeak = peak; bestNode = node.id; bestGpu = 1; }
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
      node.gpu0.status = 'ACTIVE'; node.gpu0.currentJob = job;
      node.gpu1.status = 'ACTIVE'; node.gpu1.currentJob = job;
    } else {
      const gpu = target.gpuId === 0 ? node.gpu0 : node.gpu1;
      gpu.status = 'ACTIVE'; gpu.currentJob = job;
    }
  }

  const shouldLog = tickCounter % 50 === 0;
  if (shouldLog) {
    state.chart_data.labels.push(Math.round(state.time_elapsed_sec));
  }

  for (const node of state.nodes) {
    let p0 = IDLE_POWER, p1 = IDLE_POWER;

    if (node.gpu0.currentJob) {
      const job = node.gpu0.currentJob;
      const t = node.thermalState.T_die_0;
      job.tempHistory_0.push(t);
      if (t >= SHUTDOWN_TEMP) {
        node.gpu0.status = 'SHUTDOWN';
        if (!state.failed_job_ids.includes(job.id)) { state.failed_job_ids.push(job.id); state.jobs_failed++; }
      }
      else if (t >= THROTTLE_TEMP && node.gpu0.status === 'ACTIVE') node.gpu0.status = 'THROTTLED';
      else if (t <= RECOVERY_TEMP && node.gpu0.status === 'THROTTLED') node.gpu0.status = 'ACTIVE';
      if (node.gpu0.status === 'THROTTLED') job.throttledSteps_0++;
      let req = job.currentIndex < job.power_trace_0.length ? job.power_trace_0[job.currentIndex] : 250.0;
      if (node.gpu0.status === 'THROTTLED') {
        p0 = Math.min(req, THROTTLE_CAP);
        if (req - p0 > 0) job.workDeficit_0 += (req - p0);
        else if (job.currentIndex >= job.power_trace_0.length) job.workDeficit_0 -= p0;
      } else {
        p0 = req;
        if (job.currentIndex >= job.power_trace_0.length) job.workDeficit_0 -= p0;
      }
    }

    if (node.gpu1.currentJob) {
      const job = node.gpu1.currentJob;
      const t = node.thermalState.T_die_1;
      job.tempHistory_1.push(t);
      if (t >= SHUTDOWN_TEMP) {
        node.gpu1.status = 'SHUTDOWN';
        if (!state.failed_job_ids.includes(job.id)) { state.failed_job_ids.push(job.id); state.jobs_failed++; }
      }
      else if (t >= THROTTLE_TEMP && node.gpu1.status === 'ACTIVE') node.gpu1.status = 'THROTTLED';
      else if (t <= RECOVERY_TEMP && node.gpu1.status === 'THROTTLED') node.gpu1.status = 'ACTIVE';
      if (node.gpu1.status === 'THROTTLED') job.throttledSteps_1++;
      let traceToUse = job.requested_gpus === 2 ? job.power_trace_1 : job.power_trace_0;
      let req = job.currentIndex < traceToUse.length ? traceToUse[job.currentIndex] : 250.0;
      if (node.gpu1.status === 'THROTTLED') {
        p1 = Math.min(req, THROTTLE_CAP);
        if (req - p1 > 0) job.workDeficit_1 += (req - p1);
        else if (job.currentIndex >= traceToUse.length) job.workDeficit_1 -= p1;
      } else {
        p1 = req;
        if (job.currentIndex >= traceToUse.length) job.workDeficit_1 -= p1;
      }
    }

    if (node.gpu0.currentJob && node.gpu0.currentJob === node.gpu1.currentJob) {
      node.gpu0.currentJob.currentIndex++;
    } else {
      if (node.gpu0.currentJob) node.gpu0.currentJob.currentIndex++;
      if (node.gpu1.currentJob) node.gpu1.currentJob.currentIndex++;
    }

    node.thermalState = stepPhysics(node.thermalState, p0, p1, state.ambient_temp);

    if (shouldLog) {
      state.chart_data.datasets[node.id].t0.push(node.thermalState.T_die_0);
      state.chart_data.datasets[node.id].t1.push(node.thermalState.T_die_1);
      state.chart_data.datasets[node.id].p0.push(p0);
      state.chart_data.datasets[node.id].p1.push(p1);
    }

    const checkFinish = (gpu: 'gpu0' | 'gpu1', idx: number) => {
      const job = node[gpu].currentJob;
      if (!job || node[gpu].status === 'SHUTDOWN') return;
      const traceLen = job.requested_gpus === 2 && gpu === 'gpu1' ? job.power_trace_1.length : job.power_trace_0.length;
      const def = gpu === 'gpu0' ? job.workDeficit_0 : job.workDeficit_1;
      const hist = gpu === 'gpu0' ? job.tempHistory_0 : job.tempHistory_1;
      const thr = gpu === 'gpu0' ? job.throttledSteps_0 : job.throttledSteps_1;

      if (job.currentIndex >= traceLen && def <= 0) {
        const stats = getStats(hist);
        state.completed_stats.push({
          job_id: job.id, node_number: node.id, gpu_index: idx,
          wait_time_sec: job.timeStarted - job.timeArrived,
          execution_time_sec: state.time_elapsed_sec - job.timeStarted,
          min_temp_C: stats.min, max_temp_C: stats.max, mean_temp_C: stats.mean,
          temp_std_dev_C: stats.stdDev, was_throttled: thr > 0, throttle_time_sec: thr * PHYSICS_PARAMS.DT
        });
        node[gpu].status = 'IDLE'; node[gpu].currentJob = null;
        if (job.requested_gpus === 1 || (node.gpu0.status === 'IDLE' && node.gpu1.status === 'IDLE')) state.jobs_completed++;
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
    chart_data: { labels: state.chart_data.labels, datasets: state.chart_data.datasets },
  };
}

self.onmessage = (e: MessageEvent) => {
  const { type, payload } = e.data;

  if (type === 'INIT') {
    currentMode = payload.mode || 'THERMAL_AWARE';
    initSimulation(payload.ambientTemp, payload.nodeCount);
    self.postMessage({ type: 'STATE_INIT', state: buildFullUISnapshot() });
  }
  else if (type === 'ADD_JOBS') {
    const jobsWithTime = payload.jobs.map((j: Job) => ({
      ...j, timeArrived: state.time_elapsed_sec, timeStarted: 0, currentIndex: 0,
      workDeficit_0: 0, workDeficit_1: 0, tempHistory_0: [], tempHistory_1: [],
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
    }, 100);
  }
  else if (type === 'PAUSE') {
    isRunning = false;
    if (intervalId) clearInterval(intervalId);
  }
  else if (type === 'SKIP_TO_END') {
    isRunning = false;
    currentMode = payload?.mode || currentMode;
    if (intervalId) clearInterval(intervalId);
    while (pendingJobs.length > 0 || state.nodes.some(n => n.gpu0.status !== 'IDLE' || n.gpu1.status !== 'IDLE')) tick();

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
  }
};