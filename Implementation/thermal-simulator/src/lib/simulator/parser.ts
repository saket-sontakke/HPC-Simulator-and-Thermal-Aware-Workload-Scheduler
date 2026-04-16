// src/lib/simulator/parser.ts
import Papa from 'papaparse';
import { Job } from './types';

export function parseMITTrace(file: File): Promise<Job[]> {
  return new Promise((resolve, reject) => {
    const trace0: number[] = [];
    const trace1: number[] = [];

    Papa.parse(file, {
      header: true,
      dynamicTyping: true,
      skipEmptyLines: true,
      
      step: function(row) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const data = row.data as any;
        const power = data.power_draw_W;
        const gpuIndex = data.gpu_index;

        if (power === undefined || power === null || gpuIndex === undefined || gpuIndex === null) return;

        if (gpuIndex === 0) trace0.push(power);
        else if (gpuIndex === 1) trace1.push(power);
      },
      
      complete: function() {
        const jobs: Job[] = [];
        // Clean ID strictly based on filename
        const jobId = file.name.replace('.csv', '').replace('.parquet', '');

        const hasGpu0 = trace0.length > 0;
        const hasGpu1 = trace1.length > 0;

        // Bundle into a single job depending on what we found
        if (hasGpu0 && hasGpu1) {
          jobs.push({
            id: jobId, requested_gpus: 2,
            power_trace_0: trace0, power_trace_1: trace1,
            currentIndex: 0, workDeficit_0: 0, workDeficit_1: 0,
            timeArrived: 0, timeStarted: 0,
            tempHistory_0: [], tempHistory_1: [],
            throttledSteps_0: 0, throttledSteps_1: 0
          });
        } else if (hasGpu0) {
          jobs.push({
            id: jobId, requested_gpus: 1,
            power_trace_0: trace0, power_trace_1: [],
            currentIndex: 0, workDeficit_0: 0, workDeficit_1: 0,
            timeArrived: 0, timeStarted: 0,
            tempHistory_0: [], tempHistory_1: [],
            throttledSteps_0: 0, throttledSteps_1: 0
          });
        } else if (hasGpu1) {
          // If a file ONLY has GPU 1, we still treat it as a 1-GPU job mapped to trace 0 for simplicity in the scheduler
          jobs.push({
            id: jobId, requested_gpus: 1,
            power_trace_0: trace1, power_trace_1: [],
            currentIndex: 0, workDeficit_0: 0, workDeficit_1: 0,
            timeArrived: 0, timeStarted: 0,
            tempHistory_0: [], tempHistory_1: [],
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