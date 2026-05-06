import Papa from 'papaparse';
import { Job } from './types';

export function parseMITTrace(file: File): Promise<Job[]> {
  return new Promise((resolve, reject) => {
    Papa.parse(file, {
      header: true,
      dynamicTyping: true,
      skipEmptyLines: true,
      worker: true,
      complete: function(results) {
        const trace0: number[] = [];
        const trace1: number[] = [];
        
        for (let i = 0; i < results.data.length; i++) {
          const data = results.data[i] as any;
          const power = data.power_draw_W;
          const gpuIndex = data.gpu_index;

          if (power === undefined || power === null || gpuIndex === undefined || gpuIndex === null) continue;

          if (gpuIndex === 0) trace0.push(power);
          else if (gpuIndex === 1) trace1.push(power);
        }

        const jobs: Job[] = [];
        const jobId = file.name.replace('.csv', '').replace('.parquet', '');

        const hasGpu0 = trace0.length > 0;
        const hasGpu1 = trace1.length > 0;

        if (hasGpu0 && hasGpu1) {
          jobs.push({
            id: jobId, requested_gpus: 2,
            power_trace_0: trace0, power_trace_1: trace1,
            currentIndex: 0, workDeficit_0: 0, workDeficit_1: 0,
            timeArrived: 0, timeStarted: 0,
            tick_count_0: 0, min_temp_0: Infinity, max_temp_0: -Infinity, mean_0: 0, M2_0: 0,
            tick_count_1: 0, min_temp_1: Infinity, max_temp_1: -Infinity, mean_1: 0, M2_1: 0,
            throttledSteps_0: 0, throttledSteps_1: 0
          });
        } else if (hasGpu0) {
          jobs.push({
            id: jobId, requested_gpus: 1,
            power_trace_0: trace0, power_trace_1: [],
            currentIndex: 0, workDeficit_0: 0, workDeficit_1: 0,
            timeArrived: 0, timeStarted: 0,
            tick_count_0: 0, min_temp_0: Infinity, max_temp_0: -Infinity, mean_0: 0, M2_0: 0,
            tick_count_1: 0, min_temp_1: Infinity, max_temp_1: -Infinity, mean_1: 0, M2_1: 0,
            throttledSteps_0: 0, throttledSteps_1: 0
          });
        } else if (hasGpu1) {
          jobs.push({
            id: jobId, requested_gpus: 1,
            power_trace_0: trace1, power_trace_1: [],
            currentIndex: 0, workDeficit_0: 0, workDeficit_1: 0,
            timeArrived: 0, timeStarted: 0,
            tick_count_0: 0, min_temp_0: Infinity, max_temp_0: -Infinity, mean_0: 0, M2_0: 0,
            tick_count_1: 0, min_temp_1: Infinity, max_temp_1: -Infinity, mean_1: 0, M2_1: 0,
            throttledSteps_0: 0, throttledSteps_1: 0
          });
        }

        console.log(`[Parser] Extracted ${jobs.length} job(s) from ${file.name}. (Requested GPUs: ${jobs[0]?.requested_gpus || 0})`);
        resolve(jobs);
      },
      error: function(error) { reject(error); }
    });
  });
}