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
import { Play, Pause, FastForward, RefreshCw, Sun, Moon, Home } from 'lucide-react';

type AppView = 'HOME' | 'CONFIG' | 'PHYSICS' | 'PREPROCESSING' | 'TRAINING' | 'DATASET';

export default function SimulatorDashboard() {
  const [theme, setTheme] = useState<'dark' | 'light'>('dark');
  const [currentView, setCurrentView] = useState<AppView>('HOME');
  
  const [pendingView, setPendingView] = useState<AppView | null>(null);
  const [isProcessing, setIsProcessing] = useState(false);

  const [rawJobs, setRawJobs] = useState<Job[]>([]);
  const [ambientTemp, setAmbientTemp] = useState<number | ''>(25);
  const [nodeCount, setNodeCount] = useState<number | ''>(4);
  const [mode, setMode] = useState<SchedulingMode>('THERMAL_AWARE');
  
  // A/B Test State
  const [isABTest, setIsABTest] = useState(false);

  const [uiStateA, setUiStateA] = useState<UISimulationState | null>(null);
  const [uiStateB, setUiStateB] = useState<UISimulationState | null>(null);
  
  const [isRunning, setIsRunning] = useState(false);
  const [isComplete, setIsComplete] = useState(false);
  const [simSpeed, setSimSpeed] = useState<number>(1);
  
  const [chartVersionA, setChartVersionA] = useState(0);
  const [chartVersionB, setChartVersionB] = useState(0);

  const [isUploading, setIsUploading] = useState(false);
  const [uploadStats, setUploadStats] = useState({ current: 0, total: 0 });
  const [alertModal, setAlertModal] = useState({ isOpen: false, message: '' });
  const [confirmHomeModal, setConfirmHomeModal] = useState(false);
  const [duplicateModal, setDuplicateModal] = useState<{ isOpen: boolean; newJobs: Job[]; count: number }>({ isOpen: false, newJobs: [], count: 0 });

  const workerRefA = useRef<Worker | null>(null);
  const workerRefB = useRef<Worker | null>(null);

  // -- Refs for Worker A (Standard / Default) --
  const chartLabelsRefA = useRef<number[]>([]);
  const chartDatasetsRefA = useRef<Record<number, { t0: number[]; t1: number[]; p0: number[]; p1: number[] }>>({});
  const completedStatsRefA = useRef<CompletedJobStat[]>([]);
  const nodesRefA = useRef<UINodeState[]>([]);
  const scalarsRefA = useRef({
    time_elapsed_sec: 0, ambient_temp: 25, jobs_completed: 0, jobs_failed: 0,
    queued_job_ids: [] as string[], active_job_ids: [] as string[], failed_job_ids: [] as string[],
  });
  const rafIdRefA = useRef(0);
  const chartVersionRefA = useRef(0);

  // -- Refs for Worker B (Thermal Aware for A/B Test) --
  const chartLabelsRefB = useRef<number[]>([]);
  const chartDatasetsRefB = useRef<Record<number, { t0: number[]; t1: number[]; p0: number[]; p1: number[] }>>({});
  const completedStatsRefB = useRef<CompletedJobStat[]>([]);
  const nodesRefB = useRef<UINodeState[]>([]);
  const scalarsRefB = useRef({
    time_elapsed_sec: 0, ambient_temp: 25, jobs_completed: 0, jobs_failed: 0,
    queued_job_ids: [] as string[], active_job_ids: [] as string[], failed_job_ids: [] as string[],
  });
  const rafIdRefB = useRef(0);
  const chartVersionRefB = useRef(0);

  useEffect(() => {
    if (theme === 'dark') document.documentElement.classList.add('dark');
    else document.documentElement.classList.remove('dark');
  }, [theme]);

  // --- Update Handlers ---
  const flushToStateA = useCallback(() => {
    const s = scalarsRefA.current;
    setUiStateA({
      time_elapsed_sec: s.time_elapsed_sec, ambient_temp: s.ambient_temp, nodes: nodesRefA.current,
      jobs_completed: s.jobs_completed, jobs_failed: s.jobs_failed,
      queued_job_ids: s.queued_job_ids, active_job_ids: s.active_job_ids, failed_job_ids: s.failed_job_ids,
      completed_stats: completedStatsRefA.current,
      chart_data: { labels: chartLabelsRefA.current, datasets: chartDatasetsRefA.current },
    });
    setChartVersionA(chartVersionRefA.current);
    rafIdRefA.current = 0;
  }, []);

  const flushToStateB = useCallback(() => {
    const s = scalarsRefB.current;
    setUiStateB({
      time_elapsed_sec: s.time_elapsed_sec, ambient_temp: s.ambient_temp, nodes: nodesRefB.current,
      jobs_completed: s.jobs_completed, jobs_failed: s.jobs_failed,
      queued_job_ids: s.queued_job_ids, active_job_ids: s.active_job_ids, failed_job_ids: s.failed_job_ids,
      completed_stats: completedStatsRefB.current,
      chart_data: { labels: chartLabelsRefB.current, datasets: chartDatasetsRefB.current },
    });
    setChartVersionB(chartVersionRefB.current);
    rafIdRefB.current = 0;
  }, []);

  const applyDelta = useCallback((delta: WorkerDelta, target: 'A' | 'B') => {
    const s = target === 'A' ? scalarsRefA.current : scalarsRefB.current;
    const nodesRef = target === 'A' ? nodesRefA : nodesRefB;
    const completedStatsRef = target === 'A' ? completedStatsRefA : completedStatsRefB;
    const chartLabelsRef = target === 'A' ? chartLabelsRefA : chartLabelsRefB;
    const chartDatasetsRef = target === 'A' ? chartDatasetsRefA : chartDatasetsRefB;
    const chartVersionRef = target === 'A' ? chartVersionRefA : chartVersionRefB;
    const rafIdRef = target === 'A' ? rafIdRefA : rafIdRefB;
    const flushFunc = target === 'A' ? flushToStateA : flushToStateB;

    s.time_elapsed_sec = delta.time_elapsed_sec;
    s.ambient_temp = delta.ambient_temp;
    s.jobs_completed = delta.jobs_completed;
    s.jobs_failed = delta.jobs_failed;
    s.queued_job_ids = delta.queued_job_ids;
    s.active_job_ids = delta.active_job_ids;
    s.failed_job_ids = delta.failed_job_ids;

    nodesRef.current = delta.nodes.map(n => ({
      id: n.id, T_die_0: n.T_die_0, T_die_1: n.T_die_1,
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
            dst.t0.push(...src.t0); dst.t1.push(...src.t1);
            dst.p0.push(...src.p0); dst.p1.push(...src.p1);
          }
        }
      }
      chartVersionRef.current++;
    }

    if (!rafIdRef.current) rafIdRef.current = requestAnimationFrame(flushFunc);
  }, [flushToStateA, flushToStateB]);

  const applyFullState = useCallback((full: UISimulationState, target: 'A' | 'B') => {
    const s = target === 'A' ? scalarsRefA : scalarsRefB;
    const nodesRef = target === 'A' ? nodesRefA : nodesRefB;
    const completedStatsRef = target === 'A' ? completedStatsRefA : completedStatsRefB;
    const chartLabelsRef = target === 'A' ? chartLabelsRefA : chartLabelsRefB;
    const chartDatasetsRef = target === 'A' ? chartDatasetsRefA : chartDatasetsRefB;
    const chartVersionRef = target === 'A' ? chartVersionRefA : chartVersionRefB;
    const rafIdRef = target === 'A' ? rafIdRefA : rafIdRefB;
    const flushFunc = target === 'A' ? flushToStateA : flushToStateB;

    chartLabelsRef.current = full.chart_data.labels;
    chartDatasetsRef.current = full.chart_data.datasets;
    completedStatsRef.current = full.completed_stats;
    nodesRef.current = full.nodes;
    s.current = {
      time_elapsed_sec: full.time_elapsed_sec, ambient_temp: full.ambient_temp,
      jobs_completed: full.jobs_completed, jobs_failed: full.jobs_failed,
      queued_job_ids: full.queued_job_ids, active_job_ids: full.active_job_ids, failed_job_ids: full.failed_job_ids,
    };
    chartVersionRef.current++;

    if (rafIdRef.current) cancelAnimationFrame(rafIdRef.current);
    rafIdRef.current = 0;
    flushFunc();
  }, [flushToStateA, flushToStateB]);

  useEffect(() => {
    workerRefA.current = new Worker(new URL('../../lib/simulator/worker.ts', import.meta.url));
    workerRefA.current.onmessage = (e) => {
      const { type } = e.data;
      if (type === 'STATE_INIT') applyFullState(e.data.state, 'A');
      else if (type === 'STATE_DELTA') applyDelta(e.data.delta, 'A');
      else if (type === 'SIMULATION_COMPLETE') {
        applyFullState(e.data.state, 'A');
        setIsRunning(false);
        setIsComplete(true);
        setIsProcessing(false);
      }
    };

    workerRefB.current = new Worker(new URL('../../lib/simulator/worker.ts', import.meta.url));
    workerRefB.current.onmessage = (e) => {
      const { type } = e.data;
      if (type === 'STATE_INIT') applyFullState(e.data.state, 'B');
      else if (type === 'STATE_DELTA') applyDelta(e.data.delta, 'B');
      else if (type === 'SIMULATION_COMPLETE') {
        applyFullState(e.data.state, 'B');
        // Let worker A control the main UI flags for simplicity
      }
    };

    return () => {
      if (rafIdRefA.current) cancelAnimationFrame(rafIdRefA.current);
      if (rafIdRefB.current) cancelAnimationFrame(rafIdRefB.current);
      workerRefA.current?.terminate();
      workerRefB.current?.terminate();
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
        if (document.visibilityState === 'visible') await new Promise(r => setTimeout(r, 10));
        else await yieldToMain();
      }

      const existingIds = new Set(rawJobs.map(j => j.id));
      const duplicates = allNewJobs.filter(j => existingIds.has(j.id));

      if (duplicates.length > 0) setDuplicateModal({ isOpen: true, newJobs: allNewJobs, count: duplicates.length });
      else setRawJobs(prev => [...prev, ...allNewJobs]);
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
          while (allKnownIds.has(newId)) { counter++; newId = `${j.id}-copy-${counter}`; }
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

  const resetRefs = useCallback((nc: number, target: 'A' | 'B') => {
    const chartLabelsRef = target === 'A' ? chartLabelsRefA : chartLabelsRefB;
    const chartDatasetsRef = target === 'A' ? chartDatasetsRefA : chartDatasetsRefB;
    const completedStatsRef = target === 'A' ? completedStatsRefA : completedStatsRefB;
    const nodesRef = target === 'A' ? nodesRefA : nodesRefB;
    const chartVersionRef = target === 'A' ? chartVersionRefA : chartVersionRefB;

    chartLabelsRef.current = [];
    chartDatasetsRef.current = {};
    for (let i = 0; i < nc; i++) chartDatasetsRef.current[i] = { t0: [], t1: [], p0: [], p1: [] };
    completedStatsRef.current = [];
    nodesRef.current = [];
    chartVersionRef.current = 0;
  }, []);

  const handleLaunchSimulation = () => {
    if (rawJobs.length === 0) return setAlertModal({ isOpen: true, message: "Please upload at least one Job Trace to the Workload Queue first." });
    setCurrentView('DASHBOARD');
    const safeNodes = typeof nodeCount === 'number' ? nodeCount : 4;
    const safeTemp = typeof ambientTemp === 'number' ? ambientTemp : 25;
    
    // Launch Worker A (Either Standard or Selected Mode)
    resetRefs(safeNodes, 'A');
    workerRefA.current?.postMessage({ type: 'INIT', payload: { ambientTemp: safeTemp, nodeCount: safeNodes, mode: isABTest ? 'STANDARD' : mode } });
    workerRefA.current?.postMessage({ type: 'ADD_JOBS', payload: { jobs: rawJobs } });

    // Launch Worker B (Thermal Aware) if A/B Testing
    if (isABTest) {
      resetRefs(safeNodes, 'B');
      workerRefB.current?.postMessage({ type: 'INIT', payload: { ambientTemp: safeTemp, nodeCount: safeNodes, mode: 'THERMAL_AWARE' } });
      workerRefB.current?.postMessage({ type: 'ADD_JOBS', payload: { jobs: rawJobs } });
    }
  };

  const handleStart = () => { 
    setIsRunning(true); setIsComplete(false); 
    workerRefA.current?.postMessage({ type: 'START', payload: { speed: simSpeed, mode: isABTest ? 'STANDARD' : mode } }); 
    if (isABTest) workerRefB.current?.postMessage({ type: 'START', payload: { speed: simSpeed, mode: 'THERMAL_AWARE' } });
  };
  const handlePause = () => { 
    setIsRunning(false); 
    workerRefA.current?.postMessage({ type: 'PAUSE' }); 
    if (isABTest) workerRefB.current?.postMessage({ type: 'PAUSE' }); 
  };
  const handleSkipToEnd = () => { 
    setIsRunning(false); setIsProcessing(true);
    workerRefA.current?.postMessage({ type: 'SKIP_TO_END', payload: { mode: isABTest ? 'STANDARD' : mode } }); 
    if (isABTest) workerRefB.current?.postMessage({ type: 'SKIP_TO_END', payload: { mode: 'THERMAL_AWARE' } }); 
  };
  const handleResetSim = () => {
    setIsRunning(false); setIsComplete(false); setIsProcessing(false);
    const safeNodes = typeof nodeCount === 'number' ? nodeCount : 4;
    resetRefs(safeNodes, 'A');
    workerRefA.current?.postMessage({ type: 'INIT', payload: { ambientTemp: ambientTemp || 25, nodeCount: safeNodes, mode: isABTest ? 'STANDARD' : mode } });
    workerRefA.current?.postMessage({ type: 'ADD_JOBS', payload: { jobs: rawJobs } });
    if (isABTest) {
      resetRefs(safeNodes, 'B');
      workerRefB.current?.postMessage({ type: 'INIT', payload: { ambientTemp: ambientTemp || 25, nodeCount: safeNodes, mode: 'THERMAL_AWARE' } });
      workerRefB.current?.postMessage({ type: 'ADD_JOBS', payload: { jobs: rawJobs } });
    }
  };

  const confirmHome = () => {
    setCurrentView(pendingView || 'HOME');
    setPendingView(null);
    setRawJobs([]); setUiStateA(null); setUiStateB(null); setIsRunning(false); setIsComplete(false); setIsProcessing(false);
    setUploadStats({ current: 0, total: 0 }); setConfirmHomeModal(false);
  };

  const handleNavigate = (view: AppView) => {
    if ((currentView === 'CONFIG' && rawJobs.length > 0) || currentView === 'DASHBOARD') {
      if (isRunning) handlePause();
      setPendingView(view);
      setConfirmHomeModal(true);
    } else setCurrentView(view);
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

      {currentView === 'HOME' && <HomeView theme={theme} onToggleTheme={() => setTheme(t => t === 'dark' ? 'light' : 'dark')} onNavigate={handleNavigate} />}

      {currentView === 'CONFIG' && (
        <ConfigView
          theme={theme} onToggleTheme={() => setTheme(t => t === 'dark' ? 'light' : 'dark')} onGoHome={() => handleNavigate('HOME')}
          nodeCount={nodeCount} onNodeChange={handleNodeChange} onNodeBlur={handleNodeBlur}
          ambientTemp={ambientTemp} onTempChange={handleTempChange} onTempBlur={handleTempBlur}
          mode={mode} onModeChange={setMode}
          isABTest={isABTest} onABTestChange={setIsABTest}
          isUploading={isUploading} uploadStats={uploadStats} jobCount={rawJobs.length}
          onFileUpload={handleFileUpload} onLaunch={handleLaunchSimulation}
        />
      )}

      {currentView === 'DASHBOARD' && (
        <div className="flex-1 flex flex-col w-full h-full relative">
          
          {/* Shared Control Bar for A/B Testing Mode */}
          {isABTest && (
            <div className="bg-white dark:bg-slate-900 border-b border-gray-200 dark:border-slate-800 sticky top-0 z-[60] px-6 py-3 flex justify-between items-center shadow-sm">
              <div className="flex items-center gap-4">
                <button onClick={() => handleNavigate('HOME')} className="p-2 hover:bg-gray-100 dark:hover:bg-slate-800 rounded-lg text-gray-600 dark:text-slate-400 transition-colors"><Home className="w-5 h-5"/></button>
                <div className="h-6 w-px bg-gray-300 dark:bg-slate-700"></div>
                <div>
                  <div className="flex items-center gap-2">
                    {/* FIX 3: Changed the navigation heading to match requested format */}
                    <h1 className="font-bold text-lg leading-none">Simulation Dashboard</h1>
                    <span className="px-2 py-0.5 text-[10px] font-bold rounded uppercase tracking-wide bg-blue-100 text-blue-800 dark:bg-blue-900/50 dark:text-blue-400">A/B TESTING</span>
                  </div>
                  <p className="text-xs text-gray-500 dark:text-slate-500 mt-0.5">Hardware: NVIDIA V100 (MIT TX-Gaia Cluster)</p>
                </div>
              </div>

              <div className="flex items-center gap-3">
                <div className="flex items-center gap-2 bg-gray-100 dark:bg-slate-800 p-1.5 rounded-lg border border-gray-200 dark:border-slate-700">
                  <select value={simSpeed} onChange={(e) => setSimSpeed(Number(e.target.value))} disabled={isRunning} className="bg-white dark:bg-slate-700 text-xs font-bold px-2 py-1.5 rounded outline-none text-gray-700 dark:text-white border border-gray-200 dark:border-slate-600 disabled:opacity-50">
                    <option value={1}>1x Speed</option><option value={10}>10x Speed</option><option value={20}>20x Speed</option><option value={50}>50x Speed</option><option value={100}>100x Speed</option>
                  </select>
                  <div className="w-px h-4 bg-gray-300 dark:bg-slate-600 mx-1"></div>
                  {!isRunning && !isComplete ? (
                    <button onClick={handleStart} className="flex items-center gap-1 bg-emerald-600 hover:bg-emerald-500 text-white px-3 py-1.5 rounded shadow-sm text-sm font-medium transition-colors"><Play className="w-4 h-4" /> Start</button>
                  ) : (
                    <button onClick={handlePause} disabled={isComplete} className="flex items-center gap-1 bg-amber-500 hover:bg-amber-400 text-white disabled:opacity-50 px-3 py-1.5 rounded shadow-sm text-sm font-medium transition-colors"><Pause className="w-4 h-4" /> Pause</button>
                  )}
                  <button onClick={handleSkipToEnd} disabled={isComplete || rawJobs.length === 0} className="flex items-center gap-1 bg-gray-200 dark:bg-slate-700 hover:bg-gray-300 dark:hover:bg-slate-600 disabled:opacity-50 px-3 py-1.5 rounded text-sm font-medium"><FastForward className="w-4 h-4" /> Skip to End</button>
                  <button onClick={handleResetSim} disabled={isRunning} className="flex items-center gap-1 bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400 disabled:opacity-30 px-3 py-1.5 rounded text-sm font-medium"><RefreshCw className="w-4 h-4" /> Reset</button>
                </div>
                <button onClick={() => setTheme(t => t === 'dark' ? 'light' : 'dark')} className="p-2 bg-gray-200 dark:bg-slate-800 rounded-lg text-gray-700 dark:text-slate-300">
                  {theme === 'dark' ? <Sun className="w-4 h-4"/> : <Moon className="w-4 h-4"/>}
                </button>
              </div>
            </div>
          )}

          <div className={isABTest ? "flex flex-row w-full flex-1 overflow-hidden" : "flex flex-col w-full flex-1"}>
            
            {/* Left/Main Side: Default / Standard */}
            <div className={isABTest ? "w-1/2 flex flex-col border-r-2 border-gray-300 dark:border-slate-700 overflow-y-auto custom-scrollbar pr-2" : "w-full flex flex-col flex-1"}>
              {isABTest && <h2 className="font-bold text-center py-2 bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-500 m-4 rounded shadow-sm uppercase tracking-wide">Standard Scheduler</h2>}
              <DashboardView
                state={uiStateA} theme={theme} onToggleTheme={() => setTheme(t => t === 'dark' ? 'light' : 'dark')}
                mode={isABTest ? 'STANDARD' : mode} isRunning={isRunning} isComplete={isComplete} isProcessing={isProcessing}
                simSpeed={simSpeed} onSpeedChange={setSimSpeed} totalSubmittedJobs={rawJobs.length} rawJobIds={rawJobs.map(j => j.id)}
                chartVersion={chartVersionA}
                onStart={handleStart} onPause={handlePause} onSkipToEnd={handleSkipToEnd} onReset={handleResetSim} onGoHome={() => handleNavigate('HOME')}
                hideControlBar={isABTest}
                isABTest={isABTest}
              />
            </div>

            {/* Right Side: Thermal Aware (Only shown in A/B Test) */}
            {isABTest && (
              <div className="w-1/2 flex flex-col overflow-y-auto custom-scrollbar pl-2">
                <h2 className="font-bold text-center py-2 bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-500 m-4 rounded shadow-sm uppercase tracking-wide">Thermal-Aware Scheduler (ODE)</h2>
                <DashboardView
                  state={uiStateB} theme={theme}
                  mode="THERMAL_AWARE" isRunning={isRunning} isComplete={isComplete} isProcessing={isProcessing}
                  totalSubmittedJobs={rawJobs.length} rawJobIds={rawJobs.map(j => j.id)}
                  chartVersion={chartVersionB}
                  hideControlBar={true}
                  isABTest={true}
                />
              </div>
            )}

          </div>
        </div>
      )}

      {currentView === 'PHYSICS' && <PhysicsView theme={theme} onToggleTheme={() => setTheme(t => t === 'dark' ? 'light' : 'dark')} onGoHome={() => handleNavigate('HOME')} />}
      {currentView === 'PREPROCESSING' && <PreprocessingView theme={theme} onToggleTheme={() => setTheme(t => t === 'dark' ? 'light' : 'dark')} onGoHome={() => handleNavigate('HOME')} />}
      {currentView === 'TRAINING' && <TrainingView theme={theme} onToggleTheme={() => setTheme(t => t === 'dark' ? 'light' : 'dark')} onGoHome={() => handleNavigate('HOME')} />}
      {currentView === 'DATASET' && <DatasetView theme={theme} onToggleTheme={() => setTheme(t => t === 'dark' ? 'light' : 'dark')} onGoHome={() => handleNavigate('HOME')} />}
    </div>
  );
}