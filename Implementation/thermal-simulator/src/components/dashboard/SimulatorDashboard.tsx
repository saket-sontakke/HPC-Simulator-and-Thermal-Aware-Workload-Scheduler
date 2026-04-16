'use client';

import React, { useState, useEffect, useRef, useCallback } from 'react';
import { parseMITTrace } from '../../lib/simulator/parser';
import { UISimulationState, UINodeState, SchedulingMode, Job, CompletedJobStat, WorkerDelta } from '../../lib/simulator/types';
import Modal from '../ui/Modal';
import HomeView from '../home/HomeView';
import ConfigView from './ConfigView';
import DashboardView from './DashboardView';
import PhysicsView from '../home/PhysicsView';
import TrainingView from '../home/TrainingView';
import DatasetView from '../home/DatasetView';
import PreprocessingView from '../home/PreprocessingView';

type AppView = 'HOME' | 'CONFIG' | 'DASHBOARD' | 'PHYSICS' | 'TRAINING' | 'DATASET';

export default function SimulatorDashboard() {
  const [theme, setTheme] = useState<'dark' | 'light'>('dark');
  const [currentView, setCurrentView] = useState<AppView>('HOME');
  
  const [pendingView, setPendingView] = useState<AppView | null>(null);
  const [isProcessing, setIsProcessing] = useState(false);

  const [rawJobs, setRawJobs] = useState<Job[]>([]);
  const [ambientTemp, setAmbientTemp] = useState<number | ''>(25);
  const [nodeCount, setNodeCount] = useState<number | ''>(4);
  const [mode, setMode] = useState<SchedulingMode>('THERMAL_AWARE');

  const [uiState, setUiState] = useState<UISimulationState | null>(null);
  const [isRunning, setIsRunning] = useState(false);
  const [isComplete, setIsComplete] = useState(false);
  const [simSpeed, setSimSpeed] = useState<number>(1);
  const [chartVersion, setChartVersion] = useState(0);

  const [isUploading, setIsUploading] = useState(false);
  const [uploadStats, setUploadStats] = useState({ current: 0, total: 0 });
  const [alertModal, setAlertModal] = useState({ isOpen: false, message: '' });
  const [confirmHomeModal, setConfirmHomeModal] = useState(false);
  const [duplicateModal, setDuplicateModal] = useState<{ isOpen: boolean; newJobs: Job[]; count: number }>({ isOpen: false, newJobs: [], count: 0 });

  const workerRef = useRef<Worker | null>(null);

  const chartLabelsRef = useRef<number[]>([]);
  const chartDatasetsRef = useRef<Record<number, { t0: number[]; t1: number[]; p0: number[]; p1: number[] }>>({});
  const completedStatsRef = useRef<CompletedJobStat[]>([]);
  const nodesRef = useRef<UINodeState[]>([]);
  const scalarsRef = useRef({
    time_elapsed_sec: 0,
    ambient_temp: 25,
    jobs_completed: 0,
    jobs_failed: 0,
    queued_job_ids: [] as string[],
    active_job_ids: [] as string[],
    failed_job_ids: [] as string[],
  });
  const rafIdRef = useRef(0);
  const chartVersionRef = useRef(0);

  useEffect(() => {
    if (theme === 'dark') document.documentElement.classList.add('dark');
    else document.documentElement.classList.remove('dark');
  }, [theme]);

  const flushToState = useCallback(() => {
    const s = scalarsRef.current;
    setUiState({
      time_elapsed_sec: s.time_elapsed_sec,
      ambient_temp: s.ambient_temp,
      nodes: nodesRef.current,
      jobs_completed: s.jobs_completed,
      jobs_failed: s.jobs_failed,
      queued_job_ids: s.queued_job_ids,
      active_job_ids: s.active_job_ids,
      failed_job_ids: s.failed_job_ids,
      completed_stats: completedStatsRef.current,
      chart_data: {
        labels: chartLabelsRef.current,
        datasets: chartDatasetsRef.current,
      },
    });
    setChartVersion(chartVersionRef.current);
    rafIdRef.current = 0;
  }, []);

  const applyDelta = useCallback((delta: WorkerDelta) => {
    const s = scalarsRef.current;
    s.time_elapsed_sec = delta.time_elapsed_sec;
    s.ambient_temp = delta.ambient_temp;
    s.jobs_completed = delta.jobs_completed;
    s.jobs_failed = delta.jobs_failed;
    s.queued_job_ids = delta.queued_job_ids;
    s.active_job_ids = delta.active_job_ids;
    s.failed_job_ids = delta.failed_job_ids;

    nodesRef.current = delta.nodes.map(n => ({
      id: n.id,
      T_die_0: n.T_die_0,
      T_die_1: n.T_die_1,
      gpu0: { id: 0 as const, status: n.gpu0_status as UINodeState['gpu0']['status'], currentJobId: n.gpu0_jobId },
      gpu1: { id: 1 as const, status: n.gpu1_status as UINodeState['gpu1']['status'], currentJobId: n.gpu1_jobId },
    }));

    if (delta.newCompletedStats.length > 0) {
      completedStatsRef.current = [...completedStatsRef.current, ...delta.newCompletedStats];
    }

    if (delta.newChartLabels.length > 0) {
      chartLabelsRef.current.push(...delta.newChartLabels);
      if (delta.newChartData) {
        for (const nodeIdStr of Object.keys(delta.newChartData)) {
          const nid = Number(nodeIdStr);
          const src = delta.newChartData[nid];
          const dst = chartDatasetsRef.current[nid];
          if (dst && src) {
            dst.t0.push(...src.t0);
            dst.t1.push(...src.t1);
            dst.p0.push(...src.p0);
            dst.p1.push(...src.p1);
          }
        }
      }
      chartVersionRef.current++;
    }

    if (!rafIdRef.current) {
      rafIdRef.current = requestAnimationFrame(flushToState);
    }
  }, [flushToState]);

  const applyFullState = useCallback((full: UISimulationState) => {
    chartLabelsRef.current = full.chart_data.labels;
    chartDatasetsRef.current = full.chart_data.datasets;
    completedStatsRef.current = full.completed_stats;
    nodesRef.current = full.nodes;
    scalarsRef.current = {
      time_elapsed_sec: full.time_elapsed_sec,
      ambient_temp: full.ambient_temp,
      jobs_completed: full.jobs_completed,
      jobs_failed: full.jobs_failed,
      queued_job_ids: full.queued_job_ids,
      active_job_ids: full.active_job_ids,
      failed_job_ids: full.failed_job_ids,
    };
    chartVersionRef.current++;

    if (rafIdRef.current) cancelAnimationFrame(rafIdRef.current);
    rafIdRef.current = 0;
    flushToState();
  }, [flushToState]);

  useEffect(() => {
    workerRef.current = new Worker(new URL('../../lib/simulator/worker.ts', import.meta.url));
    workerRef.current.onmessage = (e) => {
      const { type } = e.data;
      if (type === 'STATE_INIT') {
        applyFullState(e.data.state);
      } else if (type === 'STATE_DELTA') {
        applyDelta(e.data.delta);
      } else if (type === 'SIMULATION_COMPLETE') {
        applyFullState(e.data.state);
        setIsRunning(false);
        setIsComplete(true);
        setIsProcessing(false); // <--- Add this
      }
    };
    return () => {
      if (rafIdRef.current) cancelAnimationFrame(rafIdRef.current);
      workerRef.current?.terminate();
    };
  }, [applyDelta, applyFullState]);

  const handleNodeChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const val = e.target.value;
    if (val === '') { setNodeCount(''); return; }
    let num = parseInt(val);
    if (isNaN(num)) return;
    if (num > 250) num = 250;
    setNodeCount(num);
  };
  const handleNodeBlur = () => { if (nodeCount === '' || nodeCount < 1) setNodeCount(1); };

  const handleTempChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const val = e.target.value;
    if (val === '') { setAmbientTemp(''); return; }
    let num = parseInt(val);
    if (isNaN(num)) return;
    if (num > 45) num = 45;
    setAmbientTemp(num);
  };
  const handleTempBlur = () => { if (ambientTemp === '' || ambientTemp < 15) setAmbientTemp(15); };

  const yieldToMain = () => new Promise(resolve => {
    const channel = new MessageChannel();
    channel.port1.onmessage = resolve;
    channel.port2.postMessage(null);
  });

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    if (files.length === 0) return;

    setIsUploading(true);
    setUploadStats({ current: 0, total: files.length });

    try {
      let allNewJobs: Job[] = [];
      for (let i = 0; i < files.length; i++) {
        const jobs = await parseMITTrace(files[i]);
        allNewJobs = [...allNewJobs, ...jobs];
        
        setUploadStats({ current: i + 1, total: files.length });

        if (document.visibilityState === 'visible') {
          await new Promise(r => setTimeout(r, 10));
        } else {
          await yieldToMain();
        }
      }

      const existingIds = new Set(rawJobs.map(j => j.id));
      const duplicates = allNewJobs.filter(j => existingIds.has(j.id));

      if (duplicates.length > 0) {
        setDuplicateModal({ isOpen: true, newJobs: allNewJobs, count: duplicates.length });
      } else {
        setRawJobs(prev => [...prev, ...allNewJobs]);
      }
    } catch (err) { console.error(err); }

    setIsUploading(false);
    e.target.value = '';
  };

  const handleDuplicateResolve = (choice: 'DISCARD' | 'RENAME') => {
    const allKnownIds = new Set(rawJobs.map(j => j.id));
    let finalJobs = [...duplicateModal.newJobs];

    if (choice === 'DISCARD') {
      finalJobs = finalJobs.filter(j => !allKnownIds.has(j.id));
    } else if (choice === 'RENAME') {
      finalJobs = finalJobs.map(j => {
        if (allKnownIds.has(j.id)) {
          let counter = 1;
          let newId = `${j.id}-copy-${counter}`;
          
          while (allKnownIds.has(newId)) {
            counter++;
            newId = `${j.id}-copy-${counter}`;
          }
          
          allKnownIds.add(newId);
          return { ...j, id: newId };
        } else {
          allKnownIds.add(j.id);
          return j;
        }
      });
    }

    setRawJobs(prev => [...prev, ...finalJobs]);
    setDuplicateModal({ isOpen: false, newJobs: [], count: 0 });
  };

  const resetRefs = useCallback((nc: number) => {
    chartLabelsRef.current = [];
    chartDatasetsRef.current = {};
    for (let i = 0; i < nc; i++) {
      chartDatasetsRef.current[i] = { t0: [], t1: [], p0: [], p1: [] };
    }
    completedStatsRef.current = [];
    nodesRef.current = [];
    chartVersionRef.current = 0;
  }, []);

  const handleLaunchSimulation = () => {
    if (rawJobs.length === 0) return setAlertModal({ isOpen: true, message: "Please upload at least one Job Trace to the Workload Queue first." });
    setCurrentView('DASHBOARD');
    const safeNodes = typeof nodeCount === 'number' ? nodeCount : 4;
    const safeTemp = typeof ambientTemp === 'number' ? ambientTemp : 25;
    resetRefs(safeNodes);
    workerRef.current?.postMessage({ type: 'INIT', payload: { ambientTemp: safeTemp, nodeCount: safeNodes, mode } });
    workerRef.current?.postMessage({ type: 'ADD_JOBS', payload: { jobs: rawJobs } });
  };

  const handleStart = () => { setIsRunning(true); setIsComplete(false); workerRef.current?.postMessage({ type: 'START', payload: { speed: simSpeed, mode } }); };
  const handlePause = () => { setIsRunning(false); workerRef.current?.postMessage({ type: 'PAUSE' }); };
  const handleSkipToEnd = () => { 
    setIsRunning(false); 
    setIsProcessing(true);
    workerRef.current?.postMessage({ type: 'SKIP_TO_END', payload: { mode } }); 
  };
  const handleResetSim = () => {
    setIsRunning(false); setIsComplete(false); setIsProcessing(false);
    const safeNodes = typeof nodeCount === 'number' ? nodeCount : 4;
    resetRefs(safeNodes);
    workerRef.current?.postMessage({ type: 'INIT', payload: { ambientTemp: ambientTemp || 25, nodeCount: safeNodes, mode } });
    workerRef.current?.postMessage({ type: 'ADD_JOBS', payload: { jobs: rawJobs } });
  };

  const confirmHome = () => {
    setCurrentView(pendingView || 'HOME');
    setPendingView(null);
    setRawJobs([]); setUiState(null); setIsRunning(false); setIsComplete(false); setIsProcessing(false);
    setUploadStats({ current: 0, total: 0 }); setConfirmHomeModal(false);
  };

  const handleNavigate = (view: AppView) => {
    if ((currentView === 'CONFIG' && rawJobs.length > 0) || currentView === 'DASHBOARD') {
      if (isRunning) handlePause();
      setPendingView(view);
      setConfirmHomeModal(true);
    } else {
      setCurrentView(view);
    }
  };

  return (
    <div className={`${theme} min-h-screen bg-gray-50 dark:bg-slate-950 text-gray-900 dark:text-slate-200 font-sans flex flex-col transition-colors duration-300`}>

      <Modal isOpen={alertModal.isOpen} title="Attention Required" onClose={() => setAlertModal({ isOpen: false, message: '' })} actions={<button onClick={() => setAlertModal({ isOpen: false, message: '' })} className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-bold">Close</button>}>
        <p>{alertModal.message}</p>
      </Modal>

      <Modal isOpen={confirmHomeModal} title="Confirm Navigation" onClose={() => { setConfirmHomeModal(false); setPendingView(null); }}
        actions={<>
          <button onClick={() => { setConfirmHomeModal(false); setPendingView(null); }} className="px-4 py-2 bg-gray-200 dark:bg-slate-700 text-gray-800 dark:text-slate-200 rounded-md text-sm font-bold">Cancel</button>
          <button onClick={confirmHome} className="px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded-md text-sm font-bold transition-colors">Discard Data & Leave</button>
        </>}>
        <p>Are you sure you want to leave the simulation dashboard? All current simulation data, settings, and queued jobs will be permanently discarded.</p>
      </Modal>

      <Modal isOpen={duplicateModal.isOpen} title="Duplicate Jobs Detected"
        actions={<>
          <button onClick={() => handleDuplicateResolve('DISCARD')} className="px-4 py-2 bg-red-100 text-red-700 hover:bg-red-200 dark:bg-red-900/30 dark:text-red-400 rounded-md text-sm font-bold">Discard Duplicates</button>
          <button onClick={() => handleDuplicateResolve('RENAME')} className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md text-sm font-bold transition-colors">Keep & Rename</button>
        </>}>
        <p>We detected <strong>{duplicateModal.count}</strong> duplicate Job IDs in your upload.</p>
        <p className="mt-2 text-sm text-gray-500 dark:text-slate-400">Was this intentional? You can discard them, or we can append a unique suffix to keep them distinct in the logs.</p>
      </Modal>

      {currentView === 'HOME' && (
        <HomeView 
          theme={theme} 
          onToggleTheme={() => setTheme(t => t === 'dark' ? 'light' : 'dark')} 
          onNavigate={handleNavigate} 
        />
      )}

      {currentView === 'CONFIG' && (
        <ConfigView
          theme={theme} 
          onToggleTheme={() => setTheme(t => t === 'dark' ? 'light' : 'dark')}
          onGoHome={() => handleNavigate('HOME')}
          nodeCount={nodeCount} onNodeChange={handleNodeChange} onNodeBlur={handleNodeBlur}
          ambientTemp={ambientTemp} onTempChange={handleTempChange} onTempBlur={handleTempBlur}
          mode={mode} onModeChange={setMode}
          isUploading={isUploading} uploadStats={uploadStats} jobCount={rawJobs.length}
          onFileUpload={handleFileUpload} onLaunch={handleLaunchSimulation}
        />
      )}

      {currentView === 'DASHBOARD' && (
        <DashboardView
          state={uiState}
          theme={theme} onToggleTheme={() => setTheme(t => t === 'dark' ? 'light' : 'dark')}
          mode={mode} isRunning={isRunning} isComplete={isComplete}
          isProcessing={isProcessing}
          simSpeed={simSpeed} onSpeedChange={setSimSpeed}
          totalSubmittedJobs={rawJobs.length} rawJobIds={rawJobs.map(j => j.id)}
          chartVersion={chartVersion}
          onStart={handleStart} onPause={handlePause} onSkipToEnd={handleSkipToEnd}
          onReset={handleResetSim} 
          onGoHome={() => handleNavigate('HOME')}
        />
      )}

      {currentView === 'PHYSICS' && <PhysicsView theme={theme} onToggleTheme={() => setTheme(t => t === 'dark' ? 'light' : 'dark')} onGoHome={() => handleNavigate('HOME')} />}

      {currentView === 'PREPROCESSING' && <PreprocessingView theme={theme} onToggleTheme={() => setTheme(t => t === 'dark' ? 'light' : 'dark')} onGoHome={() => handleNavigate('HOME')} />}

      {currentView === 'TRAINING' && <TrainingView theme={theme} onToggleTheme={() => setTheme(t => t === 'dark' ? 'light' : 'dark')} onGoHome={() => handleNavigate('HOME')} />}
            
      {currentView === 'DATASET' && <DatasetView theme={theme} onToggleTheme={() => setTheme(t => t === 'dark' ? 'light' : 'dark')} onGoHome={() => handleNavigate('HOME')} />}
      
    </div>
  );
}