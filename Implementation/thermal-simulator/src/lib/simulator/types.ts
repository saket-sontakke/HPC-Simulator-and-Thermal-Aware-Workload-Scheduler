import { ThermalState } from './physics';

export interface Job {
  id: string;
  requested_gpus: 1 | 2;
  power_trace_0: number[];
  power_trace_1: number[];
  currentIndex: number;
  workDeficit_0: number;
  workDeficit_1: number;
  timeArrived: number;
  timeStarted: number;
  tempHistory_0: number[];
  tempHistory_1: number[];
  throttledSteps_0: number;
  throttledSteps_1: number;
}

export interface CompletedJobStat {
  job_id: string;
  node_number: number;
  gpu_index: number;
  wait_time_sec: number;
  execution_time_sec: number;
  min_temp_C: number;
  max_temp_C: number;
  mean_temp_C: number;
  temp_std_dev_C: number;
  was_throttled: boolean;
  throttle_time_sec: number;
}

export interface GPUState {
  id: 0 | 1;
  status: 'IDLE' | 'ACTIVE' | 'THROTTLED' | 'SHUTDOWN';
  currentJob: Job | null;
}

export interface ServerNode {
  id: number;
  thermalState: ThermalState;
  gpu0: GPUState;
  gpu1: GPUState;
}

export interface SimulationState {
  time_elapsed_sec: number;
  ambient_temp: number;
  nodes: ServerNode[];
  jobs_completed: number;
  jobs_failed: number;
  queued_job_ids: string[];
  active_job_ids: string[];
  failed_job_ids: string[];
  completed_stats: CompletedJobStat[];
  chart_data: {
    labels: number[];
    datasets: Record<number, { t0: number[]; t1: number[]; p0: number[]; p1: number[] }>;
  };
}

export type SchedulingMode = 'STANDARD' | 'THERMAL_AWARE';

export interface UIGPUState {
  id: 0 | 1;
  status: 'IDLE' | 'ACTIVE' | 'THROTTLED' | 'SHUTDOWN';
  currentJobId: string | null;
}

export interface UINodeState {
  id: number;
  T_die_0: number;
  T_die_1: number;
  gpu0: UIGPUState;
  gpu1: UIGPUState;
}

export interface ChartDataStore {
  labels: number[];
  datasets: Record<number, { t0: number[]; t1: number[]; p0: number[]; p1: number[] }>;
}

export interface UISimulationState {
  time_elapsed_sec: number;
  ambient_temp: number;
  nodes: UINodeState[];
  jobs_completed: number;
  jobs_failed: number;
  queued_job_ids: string[];
  active_job_ids: string[];
  failed_job_ids: string[];
  completed_stats: CompletedJobStat[];
  chart_data: ChartDataStore;
}

export interface DeltaNodeSnapshot {
  id: number;
  T_die_0: number;
  T_die_1: number;
  gpu0_status: string;
  gpu1_status: string;
  gpu0_jobId: string | null;
  gpu1_jobId: string | null;
}

export interface WorkerDelta {
  time_elapsed_sec: number;
  ambient_temp: number;
  nodes: DeltaNodeSnapshot[];
  jobs_completed: number;
  jobs_failed: number;
  queued_job_ids: string[];
  active_job_ids: string[];
  failed_job_ids: string[];
  newCompletedStats: CompletedJobStat[];
  newChartLabels: number[];
  newChartData: Record<number, { t0: number[]; t1: number[]; p0: number[]; p1: number[] }> | null;
}