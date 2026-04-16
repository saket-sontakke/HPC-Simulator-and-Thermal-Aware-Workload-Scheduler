"use client";

import React, { useState, useRef, useEffect, useMemo, useCallback, memo } from 'react';
import { Play, Pause, FastForward, Activity, Download, Sun, Moon, Home, RefreshCw, Maximize, Minimize, RotateCcw, ChevronUp, ChevronDown, Search, Info } from 'lucide-react';
import { UISimulationState, CompletedJobStat, SchedulingMode, UINodeState, UIGPUState } from '../../lib/simulator/types';

import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend, Filler } from 'chart.js';
import { Line } from 'react-chartjs-2';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend, Filler);

const VIRTUAL_ROW_HEIGHT = 44;
const VIRTUAL_OVERSCAN = 8;

const getGPUColor = (status: string, temp: number) => {
  if (status === 'SHUTDOWN') return '#dc2626';
  if (status === 'THROTTLED') return '#c026d3';
  const t = Math.max(15, Math.min(90, temp));
  const ratio = (t - 15) / (90 - 15);
  let r = Math.min(255, 2 * ratio * 255);
  let g = Math.min(255, 2 * (1 - ratio) * 255);
  return `rgb(${Math.round(r)}, ${Math.round(g)}, 0)`;
};

interface NodeCardProps {
  node: UINodeState;
  isSelected: boolean;
  onSelect: (id: number) => void;
}

const NodeCard = memo(function NodeCard({ node, isSelected, onSelect }: NodeCardProps) {
  const gpus: [UIGPUState, number][] = [[node.gpu0, 0], [node.gpu1, 1]];
  return (
    <div
      onClick={() => onSelect(node.id)}
      className={`cursor-pointer p-4 rounded-xl border-2 transition-all duration-300 ${
        isSelected
          ? 'bg-blue-50 dark:bg-slate-800 border-blue-500 shadow-[0_0_20px_rgba(59,130,246,0.25)] dark:shadow-[0_0_20px_rgba(59,130,246,0.15)] z-10'
          : 'bg-white dark:bg-slate-950 border-gray-200/80 dark:border-slate-800 hover:border-blue-300 dark:hover:border-slate-700 hover:shadow-md'
      }`}
    >
      <div className="text-sm font-bold border-b border-gray-200 dark:border-slate-800 pb-2 mb-3">Node {node.id}</div>
      <div className="space-y-3">
        {gpus.map(([gpu, i]) => {
          const temp = i === 0 ? node.T_die_0 : node.T_die_1;
          const color = getGPUColor(gpu.status, temp);
          return (
            <div key={i} className="flex justify-between items-center">
              <div className="flex flex-col">
                <span className="text-xs font-medium text-gray-500 dark:text-slate-400">
                  GPU {i}
                </span>
                <span className={`text-[10px] text-gray-500 dark:text-slate-400 opacity-70 ${gpu.status === 'ACTIVE' ? 'font-bold' : ''}`}>
                  ({gpu.status})
                </span>
              </div>
              <div className="flex items-center gap-2">
                <span className="font-mono text-xs font-bold text-gray-700 dark:text-slate-300">
                  {temp.toFixed(1)}°C
                </span>
                <div className="relative flex h-3 w-3">
                  {gpu.status === 'ACTIVE' && <span className="animate-ping absolute inline-flex h-full w-full rounded-full opacity-75" style={{ backgroundColor: color }}></span>}
                  <span className="relative inline-flex rounded-full h-3 w-3 border border-black/20" style={{ backgroundColor: color }}></span>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}, (prev, next) => {
  const pn = prev.node, nn = next.node;
  return prev.isSelected === next.isSelected
    && pn.T_die_0 === nn.T_die_0 && pn.T_die_1 === nn.T_die_1
    && pn.gpu0.status === nn.gpu0.status && pn.gpu1.status === nn.gpu1.status
    && pn.gpu0.currentJobId === nn.gpu0.currentJobId && pn.gpu1.currentJobId === nn.gpu1.currentJobId;
});

interface DashboardViewProps {
  state: UISimulationState | null;
  theme: 'dark' | 'light';
  onToggleTheme?: () => void;
  mode: SchedulingMode;
  isRunning: boolean;
  isComplete: boolean;
  isProcessing: boolean;
  simSpeed?: number;
  onSpeedChange?: (speed: number) => void;
  totalSubmittedJobs: number;
  rawJobIds: string[];
  chartVersion: number;
  onStart?: () => void;
  onPause?: () => void;
  onSkipToEnd?: () => void;
  onReset?: () => void;
  onGoHome?: () => void;
  hideControlBar?: boolean;
  isABTest?: boolean;
}

export default function DashboardView(props: DashboardViewProps) {
  const { state, theme, mode, isRunning, isComplete, isProcessing, simSpeed, chartVersion, hideControlBar, isABTest } = props;

  useEffect(() => {
    import('chartjs-plugin-zoom').then((plugin) => ChartJS.register(plugin.default));
  }, []);

  const [selectedNode, setSelectedNode] = useState<number>(0);
  const [isGraphFullScreen, setIsGraphFullScreen] = useState(false);
  const [activeDropdown, setActiveDropdown] = useState<'SUBMITTED' | 'QUEUED' | 'ACTIVE' | 'COMPLETED' | 'FAILED' | null>(null);
  const [dropdownSearchQuery, setDropdownSearchQuery] = useState('');
  const [tableSearch, setTableSearch] = useState('');
  const [sortConfig, setSortConfig] = useState<{ key: keyof CompletedJobStat; direction: 'asc' | 'desc' } | null>(null);
  const [tableScrollTop, setTableScrollTop] = useState(0);

  const chartRef = useRef<any>(null);
  const nodeRefs = useRef<Record<number, HTMLDivElement | null>>({});
  const tableContainerRef = useRef<HTMLDivElement>(null);

  const toggleDropdown = (type: any) => {
    if (activeDropdown === type) setActiveDropdown(null);
    else { setActiveDropdown(type); setDropdownSearchQuery(''); }
  };

  const resetZoom = () => { if (chartRef.current) chartRef.current.resetZoom(); };

  const handleNodeSelect = useCallback((id: number) => setSelectedNode(id), []);

  const getActiveJobInfo = useCallback((jobId: string) => {
    for (const n of state?.nodes || []) {
      if (n.gpu0.currentJobId === jobId) return `Node ${n.id} (GPU 0)`;
      if (n.gpu1.currentJobId === jobId) return `Node ${n.id} (GPU 1)`;
    }
    return '';
  }, [state?.nodes]);

  const handleActiveJobClick = useCallback((jobId: string) => {
    let foundNodeId = -1;
    for (const n of state?.nodes || []) {
      if (n.gpu0.currentJobId === jobId || n.gpu1.currentJobId === jobId) {
        foundNodeId = n.id; break;
      }
    }
    if (foundNodeId !== -1) {
      setSelectedNode(foundNodeId);
      setActiveDropdown(null);
      setTimeout(() => {
        nodeRefs.current[foundNodeId]?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }, 50);
    }
  }, [state?.nodes]);

  const formatTimeElapsed = useMemo(() => {
    if (!state) return null;
    const totalSeconds = state.time_elapsed_sec;
    const d = Math.floor(totalSeconds / 86400);
    const h = Math.floor((totalSeconds % 86400) / 3600);
    const m = Math.floor((totalSeconds % 3600) / 60);
    const s = Math.floor(totalSeconds % 60);
    return (
      <span className="font-mono text-xl font-bold flex items-baseline">
        {String(d).padStart(2, '0')}<span className="text-sm text-gray-400 dark:text-slate-500 font-medium ml-0.5 mr-1.5">d</span>
        {String(h).padStart(2, '0')}<span className="text-sm text-gray-400 dark:text-slate-500 font-medium ml-0.5 mr-1.5">h</span>
        {String(m).padStart(2, '0')}<span className="text-sm text-gray-400 dark:text-slate-500 font-medium ml-0.5 mr-1.5">m</span>
        {String(s).padStart(2, '0')}<span className="text-sm text-gray-400 dark:text-slate-500 font-medium ml-0.5">s</span>
      </span>
    );
  }, [state ? Math.floor(state.time_elapsed_sec) : null]);

  const handleSort = (key: keyof CompletedJobStat) => {
    if (!sortConfig || sortConfig.key !== key) setSortConfig({ key, direction: 'asc' });
    else if (sortConfig.direction === 'asc') setSortConfig({ key, direction: 'desc' });
    else setSortConfig(null);
  };

  const sortedAndFilteredStats = useMemo(() => {
    if (!state) return [];
    let result = [...state.completed_stats];
    if (tableSearch) result = result.filter(j => j.job_id.toLowerCase().includes(tableSearch.toLowerCase()));
    if (sortConfig) {
      result.sort((a, b) => {
        if (a[sortConfig.key] < b[sortConfig.key]) return sortConfig.direction === 'asc' ? -1 : 1;
        if (a[sortConfig.key] > b[sortConfig.key]) return sortConfig.direction === 'asc' ? 1 : -1;
        return 0;
      });
    }
    return result;
  }, [state?.completed_stats, state?.completed_stats?.length, tableSearch, sortConfig]);

  const aggregateStats = useMemo(() => {
    if (!state || state.completed_stats.length === 0) {
      return { completedCount: 0, avgWait: 0, avgExec: 0, overallMin: 0, overallMax: 0, overallMean: 0, avgStdDev: 0, totalThrottledJobs: 0, totalThrottleTime: 0 };
    }
    const stats = state.completed_stats;
    const n = stats.length;
    let sumWait = 0, sumExec = 0, sumMin = 0, sumMax = 0, sumMean = 0, sumStd = 0, throttled = 0, sumThrottleTime = 0;
    for (let i = 0; i < n; i++) {
      const j = stats[i];
      sumWait += j.wait_time_sec;
      sumExec += j.execution_time_sec;
      sumMin += j.min_temp_C;
      sumMax += j.max_temp_C;
      sumMean += j.mean_temp_C;
      sumStd += j.temp_std_dev_C;
      if (j.was_throttled) throttled++;
      sumThrottleTime += j.throttle_time_sec;
    }
    return {
      completedCount: n, avgWait: sumWait / n, avgExec: sumExec / n,
      overallMin: sumMin / n, overallMax: sumMax / n, overallMean: sumMean / n,
      avgStdDev: sumStd / n, totalThrottledJobs: throttled, totalThrottleTime: sumThrottleTime,
    };
  }, [state?.completed_stats?.length]);

  const uniqueCompletedIds = useMemo(() => {
    if (!state) return [];
    return Array.from(new Set(state.completed_stats.map(s => s.job_id)));
  }, [state?.completed_stats?.length]);

  const legendMargin = useMemo(() => ({
    id: 'legendMargin',
    beforeInit(chart: any) {
      const originalFit = chart.legend.fit;
      chart.legend.fit = function fit() {
        originalFit.bind(chart.legend)();
        this.height += 20;
      };
    }
  }), []);

  const chartOptions = useMemo(() => ({
    responsive: true, maintainAspectRatio: false, animation: false as const,
    interaction: { mode: 'index' as const, intersect: false },
    scales: {
      x: { 
        type: 'linear' as const, 
        grid: { display: false }, 
        ticks: { 
          color: theme === 'dark' ? '#94a3b8' : '#64748b', 
          maxTicksLimit: 12, 
          maxRotation: 0, 
          callback: function(val: any) { 
            return `${Math.round(val as number)}s`; 
          } 
        } 
      },
      y: { type: 'linear' as const, display: true, position: 'left' as const, min: 20, max: 100, title: { display: true, text: 'Temperature (°C)' }, grid: { color: theme === 'dark' ? '#334155' : '#e2e8f0' } },
      y1: { type: 'linear' as const, display: true, position: 'right' as const, min: 0, max: 300, title: { display: true, text: 'Power Draw (W)' }, grid: { drawOnChartArea: false } },
    },
    plugins: {
      legend: { labels: { padding: 16, color: theme === 'dark' ? '#cbd5e1' : '#475569', usePointStyle: true, boxWidth: 20 } },
      zoom: {
        limits: { x: { min: 0 } },
        zoom: { wheel: { enabled: !isRunning }, pinch: { enabled: !isRunning }, mode: 'x' as const, speed: 0.05 },
        pan: { enabled: !isRunning, mode: 'x' as const }
      }
    }
  }), [theme, isRunning]);

  const initialChartData = useMemo(() => ({
    datasets: [
      { label: ' GPU 0 Temp (°C)', data: [] as number[], borderColor: '#ef4444', yAxisID: 'y', tension: 0.2, pointRadius: 0, borderWidth: 2 },
      { label: ' GPU 1 Temp (°C)', data: [] as number[], borderColor: '#f97316', yAxisID: 'y', tension: 0.2, pointRadius: 0, borderWidth: 2, borderDash: [5, 5] },
      { label: ' GPU 0 Power (W)', data: [] as number[], borderColor: '#3b82f6', yAxisID: 'y1', tension: 0.1, pointRadius: 0, borderWidth: 1, fill: true, backgroundColor: 'rgba(59, 130, 246, 0.1)' },
      { label: ' GPU 1 Power (W)', data: [] as number[], borderColor: '#8b5cf6', yAxisID: 'y1', tension: 0.1, pointRadius: 0, borderWidth: 1, fill: true, backgroundColor: 'rgba(139, 92, 246, 0.1)' }
    ]
  }), []);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || !state) return;

    const nodeData = state.chart_data.datasets[selectedNode];
    const labels = state.chart_data.labels;
    
    const t0 = [], t1 = [], p0 = [], p1 = [];
    for (let i = 0; i < labels.length; i++) {
      const x = labels[i];
      t0.push({ x, y: nodeData?.t0[i] });
      t1.push({ x, y: nodeData?.t1[i] });
      p0.push({ x, y: nodeData?.p0[i] });
      p1.push({ x, y: nodeData?.p1[i] });
    }

    chart.data.datasets[0].data = t0;
    chart.data.datasets[1].data = t1;
    chart.data.datasets[2].data = p0;
    chart.data.datasets[3].data = p1;
    if (chart.options.plugins.zoom) {
      chart.options.plugins.zoom.limits.x.max = state.time_elapsed_sec > 0 
        ? state.time_elapsed_sec + 5 
        : undefined; 
    }

    chart.update('none');
  }, [chartVersion, selectedNode, state?.chart_data]);
  
  const handleTableScroll = useCallback((e: React.UIEvent<HTMLDivElement>) => {
    setTableScrollTop(e.currentTarget.scrollTop);
  }, []);

  const virtualTable = useMemo(() => {
    const totalRows = sortedAndFilteredStats.length;
    const totalHeight = totalRows * VIRTUAL_ROW_HEIGHT;
    const containerHeight = 400;
    const startIdx = Math.max(0, Math.floor(tableScrollTop / VIRTUAL_ROW_HEIGHT) - VIRTUAL_OVERSCAN);
    const endIdx = Math.min(totalRows, Math.ceil((tableScrollTop + containerHeight) / VIRTUAL_ROW_HEIGHT) + VIRTUAL_OVERSCAN);
    const topPad = startIdx * VIRTUAL_ROW_HEIGHT;
    const bottomPad = (totalRows - endIdx) * VIRTUAL_ROW_HEIGHT;
    const visibleRows = sortedAndFilteredStats.slice(startIdx, endIdx);
    return { totalHeight, topPad, bottomPad, visibleRows, startIdx };
  }, [sortedAndFilteredStats, tableScrollTop]);

  const downloadReport = useCallback(() => {
    if (!state) return;
    const { completedCount: cc, avgWait, avgExec, overallMin, overallMax, overallMean, avgStdDev, totalThrottledJobs, totalThrottleTime } = aggregateStats;
    const headers = "job_id,node_number,gpu_index,wait_time_sec,execution_time_sec,min_temp_C,max_temp_C,mean_temp_C,temp_std_dev_C,was_throttled,throttle_time_sec\n";
    const rows = state.completed_stats.map(j =>
      `${j.job_id},${j.node_number},${j.gpu_index},${j.wait_time_sec.toFixed(1)},${j.execution_time_sec.toFixed(1)},${j.min_temp_C.toFixed(1)},${j.max_temp_C.toFixed(1)},${j.mean_temp_C.toFixed(1)},${j.temp_std_dev_C.toFixed(2)},${j.was_throttled},${j.throttle_time_sec.toFixed(1)}`
    ).join("\n");
    const overallRow = `\nOVERALL,${cc} Jobs,N/A,${avgWait.toFixed(1)},${avgExec.toFixed(1)},${overallMin.toFixed(1)},${overallMax.toFixed(1)},${overallMean.toFixed(1)},${avgStdDev.toFixed(2)},${totalThrottledJobs},${totalThrottleTime.toFixed(1)}\n`;
    const blob = new Blob([headers + rows + overallRow], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a'); a.href = url; a.download = `Simulation_Report_${mode}.csv`;
    a.click(); window.URL.revokeObjectURL(url);
  }, [state?.completed_stats, aggregateStats, mode]);

  const renderHeader = (label1: string, label2: string | null, sortKey: keyof CompletedJobStat | null, widthClass: string) => {
    const isSortable = sortKey !== null;
    const isActiveSort = sortConfig?.key === sortKey;
    return (
      <div
        className={`${widthClass} flex items-center justify-center text-center px-1 ${isSortable ? 'cursor-pointer hover:bg-gray-200 dark:hover:bg-slate-700' : ''} rounded py-1 transition-colors select-none`}
        onClick={() => isSortable && sortKey && handleSort(sortKey)}
      >
        <div className="flex flex-col items-center leading-tight">
          <span>{label1}</span>
          {label2 && <span>{label2}</span>}
        </div>
        {isSortable && (
          <div className="flex flex-col ml-1 opacity-60">
            <ChevronUp className={`w-3 h-3 ${isActiveSort && sortConfig.direction === 'asc' ? 'text-blue-500 opacity-100 stroke-[3px]' : 'opacity-30'}`} />
            <ChevronDown className={`w-3 h-3 -mt-1 ${isActiveSort && sortConfig.direction === 'desc' ? 'text-blue-500 opacity-100 stroke-[3px]' : 'opacity-30'}`} />
          </div>
        )}
      </div>
    );
  };

  if (!state) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center h-[80vh] bg-gray-50 dark:bg-slate-950">
        <div className="animate-spin rounded-full h-12 w-12 border-t-4 border-b-4 border-blue-600 mb-4"></div>
        <p className="text-gray-500 dark:text-slate-400 font-bold tracking-wide">Initializing ODE Engine & Telemetry...</p>
      </div>
    );
  }

  const { completedCount, avgWait, avgExec, overallMin, overallMax, overallMean, avgStdDev, totalThrottledJobs, totalThrottleTime } = aggregateStats;

  return (
    <div className="flex-1 flex flex-col relative">

      {isProcessing && (
        <div className="fixed inset-0 z-[200] flex flex-col items-center justify-center bg-white/70 dark:bg-slate-950/80 backdrop-blur-sm animate-in fade-in duration-200">
          <div className="bg-white dark:bg-slate-900 p-8 rounded-3xl shadow-2xl border border-gray-200 dark:border-slate-800 flex flex-col items-center text-center max-w-sm">
            <div className="relative flex justify-center items-center mb-6">
               <div className="animate-spin rounded-full h-16 w-16 border-t-4 border-b-4 border-blue-600"></div>
               <Activity className="absolute w-6 h-6 text-blue-500 animate-pulse" />
            </div>
            <h2 className="text-xl font-bold text-gray-900 dark:text-white mb-2">Fast-Forwarding</h2>
            <p className="text-sm text-gray-500 dark:text-slate-400">
              Calculating ODE mathematics for the remaining workloads. Please wait...
            </p>
          </div>
        </div>
      )}

      {activeDropdown && <div className="fixed inset-0 z-40" onClick={() => setActiveDropdown(null)}></div>}

      {!hideControlBar && (
        <div className="bg-white dark:bg-slate-900 border-b border-gray-200 dark:border-slate-800 sticky top-0 z-[60] px-6 py-3 flex justify-between items-center shadow-sm">
          <div className="flex items-center gap-4">
            <button onClick={props.onGoHome} className="p-2 hover:bg-gray-100 dark:hover:bg-slate-800 rounded-lg text-gray-600 dark:text-slate-400 transition-colors"><Home className="w-5 h-5"/></button>
            <div className="h-6 w-px bg-gray-300 dark:bg-slate-700"></div>
            <div>
              <div className="flex items-center gap-2">
                <h1 className="font-bold text-lg leading-none">Simulation Dashboard</h1>
                <span className={`px-2 py-0.5 text-[10px] font-bold rounded uppercase tracking-wide ${mode === 'THERMAL_AWARE' ? 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-400' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-500'}`}>
                  {mode}
                </span>
              </div>
              <p className="text-xs text-gray-500 dark:text-slate-500 mt-0.5">Hardware: NVIDIA V100 (MIT TX-Gaia Cluster)</p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2 bg-gray-100 dark:bg-slate-800 p-1.5 rounded-lg border border-gray-200 dark:border-slate-700">
              <select value={simSpeed} onChange={(e) => props.onSpeedChange && props.onSpeedChange(Number(e.target.value))} disabled={isRunning} className="bg-white dark:bg-slate-700 text-xs font-bold px-2 py-1.5 rounded outline-none text-gray-700 dark:text-white border border-gray-200 dark:border-slate-600 disabled:opacity-50">
                <option value={1}>1x Speed</option>
                <option value={10}>10x Speed</option>
                <option value={20}>20x Speed</option>
                <option value={50}>50x Speed</option>
                <option value={100}>100x Speed</option>
              </select>
              <div className="w-px h-4 bg-gray-300 dark:bg-slate-600 mx-1"></div>
              {!isRunning && !isComplete ? (
                <button onClick={props.onStart} className="flex items-center gap-1 bg-emerald-600 hover:bg-emerald-500 text-white px-3 py-1.5 rounded shadow-sm text-sm font-medium transition-colors"><Play className="w-4 h-4" /> Start</button>
              ) : (
                <button onClick={props.onPause} disabled={isComplete} className="flex items-center gap-1 bg-amber-500 hover:bg-amber-400 text-white disabled:opacity-50 px-3 py-1.5 rounded shadow-sm text-sm font-medium transition-colors"><Pause className="w-4 h-4" /> Pause</button>
              )}
              <button onClick={props.onSkipToEnd} disabled={isComplete || props.totalSubmittedJobs === 0} className="flex items-center gap-1 bg-gray-200 dark:bg-slate-700 hover:bg-gray-300 dark:hover:bg-slate-600 disabled:opacity-50 px-3 py-1.5 rounded text-sm font-medium"><FastForward className="w-4 h-4" /> Skip to End</button>
              <button onClick={props.onReset} disabled={isRunning} className="flex items-center gap-1 bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400 disabled:opacity-30 px-3 py-1.5 rounded text-sm font-medium"><RefreshCw className="w-4 h-4" /> Reset</button>
            </div>
            {props.onToggleTheme && (
              <button onClick={props.onToggleTheme} className="p-2 bg-gray-200 dark:bg-slate-800 rounded-lg text-gray-700 dark:text-slate-300">
                {theme === 'dark' ? <Sun className="w-4 h-4"/> : <Moon className="w-4 h-4"/>}
              </button>
            )}
          </div>
        </div>
      )}

      <div className="px-2 py-3 flex flex-col gap-3 w-full flex-1">
        
        {/* CONDITIONALLY RENDERED STATS BAR */}
        <div className={`bg-white dark:bg-slate-900 border-gray-200 dark:border-slate-800 shadow-sm relative z-40 ${
          isABTest 
            ? 'p-5 rounded-2xl border flex flex-col gap-5' 
            : 'p-4 rounded-2xl border flex flex-wrap items-center justify-between px-8 gap-4'
        }`}>
          
          <div className={isABTest ? "flex items-center justify-between w-full px-2" : "flex items-center gap-8"}>
            <div>
              <span className="block text-xs text-gray-500 dark:text-slate-400 uppercase tracking-wider font-bold mb-1">Ambient Temp</span>
              <span className="font-mono text-xl font-bold">{state.ambient_temp}°C</span>
            </div>
            
            {/* Divider for single line format */}
            {!isABTest && <div className="w-px h-8 bg-gray-200 dark:bg-slate-700"></div>}
            
            <div className={isABTest ? "text-right" : ""}>
              <span className="block text-xs text-gray-500 dark:text-slate-400 uppercase tracking-wider font-bold mb-1">Time Elapsed</span>
              <div className={`flex items-baseline gap-2 ${isABTest ? 'justify-end' : ''}`}>
                {formatTimeElapsed}
                <span className="text-sm font-mono text-gray-400 dark:text-slate-500 font-medium">
                  ({state.time_elapsed_sec.toFixed(1)}s)
                </span>
              </div>
            </div>
          </div>

          {/* Divider for 2-line format */}
          {isABTest && <div className="w-full h-px bg-gray-100 dark:bg-slate-800"></div>}

          <div className={`flex items-center ${isABTest ? 'justify-center gap-6 w-full' : 'gap-4'}`}>
            <div className="relative">
              <div onClick={() => toggleDropdown('SUBMITTED')} className={`cursor-pointer hover:bg-purple-50 dark:hover:bg-slate-800 p-2 px-4 rounded-lg transition-colors text-center border ${activeDropdown==='SUBMITTED'?'bg-purple-50 dark:bg-slate-800 border-purple-200 dark:border-slate-600':'border-transparent hover:border-purple-100 dark:hover:border-slate-700'}`}>
                <span className="block text-xs text-gray-500 dark:text-slate-400 uppercase tracking-wider font-bold mb-1">Jobs Submitted</span>
                <span className="font-mono text-xl font-bold">{props.totalSubmittedJobs}</span>
              </div>
              {activeDropdown === 'SUBMITTED' && (
                <div className={`absolute top-full mt-2 w-64 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 rounded-xl shadow-xl overflow-hidden flex flex-col ${isABTest ? 'left-1/2 -translate-x-1/2' : 'right-0'}`}>
                  <div className="p-2 border-b border-gray-100 dark:border-slate-700 flex items-center gap-2 bg-gray-50 dark:bg-slate-900/50">
                    <Search className="w-4 h-4 text-gray-400 ml-1" />
                    <input type="text" placeholder="Search Job ID..." value={dropdownSearchQuery} onChange={(e) => setDropdownSearchQuery(e.target.value)} className="w-full bg-transparent text-sm outline-none" />
                  </div>
                  <div className="max-h-60 overflow-y-auto custom-scrollbar p-1">
                    {props.rawJobIds.filter(id => id.includes(dropdownSearchQuery)).map((id, idx) => (
                      <div key={`${id}-${idx}`} className="text-xs font-mono p-2 hover:bg-gray-100 dark:hover:bg-slate-700 rounded cursor-default truncate" title={id}>{id}</div>
                    ))}
                    {props.rawJobIds.length === 0 && <div className="p-3 text-xs text-center text-gray-500 italic">No jobs submitted.</div>}
                  </div>
                </div>
              )}
            </div>

            <div className="relative">
              <div onClick={() => toggleDropdown('QUEUED')} className={`cursor-pointer hover:bg-orange-50 dark:hover:bg-slate-800 p-2 px-4 rounded-lg transition-colors text-center border ${activeDropdown==='QUEUED'?'bg-orange-50 dark:bg-slate-800 border-orange-200 dark:border-slate-600':'border-transparent hover:border-orange-100 dark:hover:border-slate-700'}`}>
                <span className="block text-xs text-gray-500 dark:text-slate-400 uppercase tracking-wider font-bold mb-1">Jobs Queued</span>
                <span className="font-mono text-xl font-bold">{state.queued_job_ids.length}</span>
              </div>
              {activeDropdown === 'QUEUED' && (
                <div className={`absolute top-full mt-2 w-64 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 rounded-xl shadow-xl overflow-hidden flex flex-col ${isABTest ? 'left-1/2 -translate-x-1/2' : 'right-0'}`}>
                  <div className="p-2 border-b border-gray-100 dark:border-slate-700 flex items-center gap-2 bg-gray-50 dark:bg-slate-900/50">
                    <Search className="w-4 h-4 text-gray-400 ml-1" />
                    <input type="text" placeholder="Search Job ID..." value={dropdownSearchQuery} onChange={(e) => setDropdownSearchQuery(e.target.value)} className="w-full bg-transparent text-sm outline-none" />
                  </div>
                  <div className="max-h-60 overflow-y-auto custom-scrollbar p-1">
                    {state.queued_job_ids.filter(id => id.includes(dropdownSearchQuery)).map((id, idx) => (
                      <div key={`${id}-${idx}`} className="text-xs font-mono p-2 hover:bg-gray-100 dark:hover:bg-slate-700 rounded cursor-default truncate" title={id}>{id}</div>
                    ))}
                    {state.queued_job_ids.length === 0 && <div className="p-3 text-xs text-center text-gray-500 italic">No jobs queued.</div>}
                  </div>
                </div>
              )}
            </div>

            <div className="relative">
              <div onClick={() => toggleDropdown('ACTIVE')} className={`cursor-pointer hover:bg-blue-50 dark:hover:bg-slate-800 p-2 px-4 rounded-lg transition-colors text-center border ${activeDropdown==='ACTIVE'?'bg-blue-50 dark:bg-slate-800 border-blue-200 dark:border-slate-600':'border-transparent hover:border-blue-100 dark:hover:border-slate-700'}`}>
                <span className="block text-xs text-gray-500 dark:text-slate-400 uppercase tracking-wider font-bold mb-1">Jobs Active</span>
                <span className="font-mono text-xl font-bold">{state.active_job_ids.length}</span>
              </div>
              {activeDropdown === 'ACTIVE' && (
                <div className={`absolute top-full mt-2 w-72 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 rounded-xl shadow-xl overflow-hidden flex flex-col ${isABTest ? 'left-1/2 -translate-x-1/2' : 'right-0'}`}>
                  <div className="p-2 border-b border-gray-100 dark:border-slate-700 flex items-center gap-2 bg-gray-50 dark:bg-slate-900/50">
                    <Search className="w-4 h-4 text-gray-400 ml-1" />
                    <input type="text" placeholder="Search ID or Node..." value={dropdownSearchQuery} onChange={(e) => setDropdownSearchQuery(e.target.value)} className="w-full bg-transparent text-sm outline-none" />
                  </div>
                  <div className="max-h-60 overflow-y-auto custom-scrollbar p-1">
                    {state.active_job_ids.filter(id => id.includes(dropdownSearchQuery) || getActiveJobInfo(id).toLowerCase().includes(dropdownSearchQuery.toLowerCase())).map((id, idx) => (
                      <div key={`${id}-${idx}`} onClick={() => handleActiveJobClick(id)} className="text-xs flex justify-between items-center p-2 hover:bg-gray-100 dark:hover:bg-slate-700 rounded cursor-pointer transition-colors">
                        <span className="font-mono truncate mr-2" title={id}>{id}</span>
                        <span className="bg-blue-100 dark:bg-blue-900/50 text-blue-700 dark:text-blue-300 px-1.5 py-0.5 rounded font-bold shrink-0">{getActiveJobInfo(id)}</span>
                      </div>
                    ))}
                    {state.active_job_ids.length === 0 && <div className="p-3 text-xs text-center text-gray-500 italic">No active jobs.</div>}
                  </div>
                </div>
              )}
            </div>

            <div className="relative">
              <div onClick={() => toggleDropdown('COMPLETED')} className={`cursor-pointer hover:bg-emerald-50 dark:hover:bg-slate-800 p-2 px-4 rounded-lg transition-colors text-center border ${activeDropdown==='COMPLETED'?'bg-emerald-50 dark:bg-slate-800 border-emerald-200 dark:border-slate-600':'border-transparent hover:border-emerald-100 dark:hover:border-slate-700'}`}>
                <span className="block text-xs text-emerald-600 dark:text-emerald-500 uppercase tracking-wider font-bold mb-1">Jobs Completed</span>
                <span className="font-mono text-xl font-bold text-emerald-600 dark:text-emerald-500">{uniqueCompletedIds.length}</span>
              </div>
              {activeDropdown === 'COMPLETED' && (
                <div className={`absolute top-full mt-2 w-64 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 rounded-xl shadow-xl overflow-hidden flex flex-col ${isABTest ? 'left-1/2 -translate-x-1/2' : 'right-0'}`}>
                  <div className="p-2 border-b border-gray-100 dark:border-slate-700 flex items-center gap-2 bg-gray-50 dark:bg-slate-900/50">
                    <Search className="w-4 h-4 text-gray-400 ml-1" />
                    <input type="text" placeholder="Search Job ID..." value={dropdownSearchQuery} onChange={(e) => setDropdownSearchQuery(e.target.value)} className="w-full bg-transparent text-sm outline-none" />
                  </div>
                  <div className="max-h-60 overflow-y-auto custom-scrollbar p-1">
                    {uniqueCompletedIds.filter(id => id.includes(dropdownSearchQuery)).map((id, idx) => (
                      <div key={`${id}-${idx}`} className="text-xs font-mono p-2 hover:bg-gray-100 dark:hover:bg-slate-700 rounded cursor-default truncate" title={id}>{id}</div>
                    ))}
                    {state.completed_stats.length === 0 && <div className="p-3 text-xs text-center text-gray-500 italic">No completed jobs yet.</div>}
                  </div>
                </div>
              )}
            </div>

            <div className="relative">
              <div onClick={() => toggleDropdown('FAILED')} className={`cursor-pointer hover:bg-red-50 dark:hover:bg-slate-800 p-2 px-4 rounded-lg transition-colors text-center border ${activeDropdown==='FAILED'?'bg-red-50 dark:bg-slate-800 border-red-200 dark:border-slate-600':'border-transparent hover:border-red-100 dark:hover:border-slate-700'}`}>
                <span className="block text-xs text-red-600 dark:text-red-500 uppercase tracking-wider font-bold mb-1">Jobs Failed</span>
                <span className="font-mono text-xl font-bold text-red-600 dark:text-red-500">{state.failed_job_ids.length}</span>
              </div>
              {activeDropdown === 'FAILED' && (
                <div className={`absolute top-full mt-2 w-64 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 rounded-xl shadow-xl overflow-hidden flex flex-col ${isABTest ? 'left-1/2 -translate-x-1/2' : 'right-0'}`}>
                  <div className="p-2 border-b border-gray-100 dark:border-slate-700 flex items-center gap-2 bg-gray-50 dark:bg-slate-900/50">
                    <Search className="w-4 h-4 text-gray-400 ml-1" />
                    <input type="text" placeholder="Search Job ID..." value={dropdownSearchQuery} onChange={(e) => setDropdownSearchQuery(e.target.value)} className="w-full bg-transparent text-sm outline-none" />
                  </div>
                  <div className="max-h-60 overflow-y-auto custom-scrollbar p-1">
                    {state.failed_job_ids.filter(id => id.includes(dropdownSearchQuery)).map((id, idx) => (
                      <div key={`${id}-${idx}`} className="text-xs font-mono p-2 hover:bg-gray-100 dark:hover:bg-slate-700 rounded cursor-default text-red-600 dark:text-red-400 truncate" title={id}>{id}</div>
                    ))}
                    {state.failed_job_ids.length === 0 && <div className="p-3 text-xs text-center text-gray-500 italic">No failed jobs.</div>}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>

        <div className={`grid ${isABTest ? 'grid-cols-1 gap-4' : 'grid-cols-1 lg:grid-cols-2 gap-3 h-[430px]'}`}>
          <div className={`bg-white dark:bg-slate-900 p-5 rounded-2xl border border-gray-200 dark:border-slate-800 shadow-sm flex flex-col ${isABTest ? 'h-[440px]' : 'h-full min-h-0'}`}>
            <div className="flex items-center gap-2 mb-4 shrink-0">
              <h2 className="text-base font-bold">Node Thermal Overview</h2>
              <div className="group relative flex items-center">
                <Info className="w-4 h-4 text-gray-400 cursor-help" />
                <div className="absolute left-6 w-64 p-2 bg-gray-800 text-white text-xs rounded opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-10">
                  Click on any node to stream its telemetry directly into the real-time graph.
                </div>
              </div>
            </div>

            <div className={`grid grid-cols-2 lg:grid-cols-3 gap-4 flex-1 overflow-y-auto min-h-0 pr-2 custom-scrollbar content-start`}>
              {state.nodes.map((node) => (
                <div key={node.id} ref={(el) => { nodeRefs.current[node.id] = el; }}>
                  <NodeCard node={node} isSelected={selectedNode === node.id} onSelect={handleNodeSelect} />
                </div>
              ))}
            </div>
          </div>

          <div className={isGraphFullScreen ? "fixed inset-0 z-[100] bg-white dark:bg-slate-950 p-8 flex flex-col" : `bg-white dark:bg-slate-900 p-5 rounded-2xl border border-gray-200 dark:border-slate-800 shadow-sm flex flex-col ${isABTest ? 'h-[350px]' : 'h-full min-h-0'}`}>
            <div className="flex items-center justify-between mb-1 shrink-0">
              <div className="flex items-center gap-2">
                <Activity className="w-5 h-5 text-blue-500" />
                <h3 className="text-base font-bold">Real-time Telemetry (Node {selectedNode})</h3>
              </div>
              <div className="flex items-center gap-2">
                {(!isRunning && state.time_elapsed_sec > 0) && <span className="text-xs text-blue-600 dark:text-blue-400 font-medium bg-blue-50 dark:bg-blue-900/30 px-2 py-1 rounded">Scroll to zoom, drag to pan</span>}
                <button onClick={resetZoom} className="p-1.5 bg-gray-100 hover:bg-gray-200 dark:bg-slate-800 dark:hover:bg-slate-700 rounded text-gray-600 dark:text-slate-400 transition-colors" title="Reset Zoom Scale">
                  <RotateCcw className="w-4 h-4" />
                </button>
                <button onClick={() => setIsGraphFullScreen(!isGraphFullScreen)} className="p-1.5 bg-gray-100 hover:bg-gray-200 dark:bg-slate-800 dark:hover:bg-slate-700 rounded text-gray-600 dark:text-slate-400 transition-colors" title={isGraphFullScreen ? "Minimize" : "Full Screen"}>
                  {isGraphFullScreen ? <Minimize className="w-4 h-4" /> : <Maximize className="w-4 h-4" />}
                </button>
              </div>
            </div>
            <div className="w-full relative flex-1 min-h-0">
              <Line
                ref={chartRef}
                plugins={[legendMargin]}
                data={initialChartData}
                options={chartOptions}
              />
            </div>
          </div>
        </div>

        {state.completed_stats.length > 0 && (
          <div className="w-full pb-8">
            <div className="bg-white dark:bg-slate-900 rounded-2xl border border-gray-200 dark:border-slate-800 shadow-sm overflow-hidden flex flex-col">
              <div className="p-4 border-b border-gray-200 dark:border-slate-800 flex justify-between items-center bg-gray-50 dark:bg-slate-900">
                <h2 className="text-base font-bold">Summary Table</h2>
                <div className="flex items-center gap-3">
                  <div className="flex items-center gap-2 bg-white dark:bg-slate-800 border border-gray-300 dark:border-slate-600 rounded px-3 py-1.5 shadow-inner">
                    <Search className="w-4 h-4 text-gray-400" />
                    <input
                      type="text"
                      placeholder="Search Job ID..."
                      value={tableSearch}
                      onChange={(e) => setTableSearch(e.target.value)}
                      className="bg-transparent outline-none text-sm w-48 text-gray-700 dark:text-gray-200 placeholder-gray-400"
                    />
                  </div>
                  <button onClick={downloadReport} className="flex items-center gap-1 text-sm bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded shadow transition-colors">
                    <Download className="w-4 h-4" /> Export CSV
                  </button>
                </div>
              </div>

              {/* Wrapped in overflow-x-auto and enforced a min-width of 1050px to prevent header/cell cramming */}
              <div className="overflow-x-auto w-full custom-scrollbar">
                <div className="flex flex-col w-full text-sm min-w-[1050px]">
                  <div className="flex bg-gray-100 dark:bg-slate-800 text-gray-700 dark:text-slate-300 font-bold py-2 border-b border-gray-200 dark:border-slate-700 items-center">
                    {renderHeader("Job ID", null, null, "w-[24%]")}
                    {renderHeader("Node", null, null, "w-[5%]")}
                    {renderHeader("GPU", null, null, "w-[5%]")}
                    {renderHeader("Waiting", "Time (s)", "wait_time_sec", "w-[8%]")}
                    {renderHeader("Execution", "Time (s)", "execution_time_sec", "w-[8%]")}
                    {renderHeader("Minimum", "Temp (°C)", "min_temp_C", "w-[8%]")}
                    {renderHeader("Maximum", "Temp (°C)", "max_temp_C", "w-[8%]")}
                    {renderHeader("Mean", "Temp (°C)", "mean_temp_C", "w-[8%]")}
                    {renderHeader("Std", "Dev", "temp_std_dev_C", "w-[7%]")}
                    {renderHeader("Throttled", null, null, "w-[8%]")}
                    {renderHeader("Throttle", "Time (s)", "throttle_time_sec", "w-[11%]")}
                    <div className="w-[8px] shrink-0"></div>
                  </div>

                  <div
                    ref={tableContainerRef}
                    className="max-h-[400px] overflow-y-auto custom-scrollbar"
                    onScroll={handleTableScroll}
                  >
                    {sortedAndFilteredStats.length === 0 ? (
                      <div className="p-8 text-center text-gray-500 italic">No matching completed jobs found.</div>
                    ) : (
                      <div style={{ height: virtualTable.totalHeight, position: 'relative' }}>
                        <div style={{ position: 'absolute', top: virtualTable.topPad, left: 0, right: 0 }}>
                          {virtualTable.visibleRows.map((j, vi) => {
                            const realIdx = virtualTable.startIdx + vi;
                            return (
                              <div key={realIdx} className="flex hover:bg-gray-50 dark:hover:bg-slate-800/50 transition-colors items-center" style={{ height: VIRTUAL_ROW_HEIGHT }}>
                                <div className="w-[24%] flex items-center px-4 font-mono text-[11px] truncate" title={j.job_id}>{j.job_id}</div>
                                <div className="w-[5%] flex items-center justify-center text-center px-1 font-mono">{j.node_number}</div>
                                <div className="w-[5%] flex items-center justify-center text-center px-1 font-mono">{j.gpu_index}</div>
                                <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono">{j.wait_time_sec.toFixed(0)}</div>
                                <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono">{j.execution_time_sec.toFixed(0)}</div>
                                <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono font-bold" style={{ color: getGPUColor('ACTIVE', j.min_temp_C) }}>{j.min_temp_C.toFixed(1)}</div>
                                <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono font-bold" style={{ color: getGPUColor('ACTIVE', j.max_temp_C) }}>{j.max_temp_C.toFixed(1)}</div>
                                <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono font-bold" style={{ color: getGPUColor('ACTIVE', j.mean_temp_C) }}>{j.mean_temp_C.toFixed(1)}</div>
                                <div className="w-[7%] flex items-center justify-center text-center px-1 font-mono">{j.temp_std_dev_C.toFixed(1)}</div>
                                <div className="w-[8%] flex items-center justify-center text-center px-1">{j.was_throttled ? <span className="text-red-500 font-bold">YES</span> : <span className="text-gray-400">NO</span>}</div>
                                <div className="w-[11%] flex items-center justify-center text-center px-1 font-mono">{j.throttle_time_sec.toFixed(0)}</div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    )}
                  </div>

                  <div className="flex bg-blue-50 dark:bg-slate-800 text-blue-900 dark:text-blue-100 font-bold py-3 border-t border-blue-200 dark:border-slate-700 items-center">
                    <div className="w-[24%] flex items-center px-4 uppercase tracking-wider text-xs">Overall</div>
                    <div className="w-[10%] flex items-center justify-center text-center px-1 text-[11px]">{completedCount} Jobs</div>
                    <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono text-xs">{avgWait.toFixed(0)} <span className="text-[10px] ml-1 opacity-70">(avg)</span></div>
                    <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono text-xs">{avgExec.toFixed(0)} <span className="text-[10px] ml-1 opacity-70">(avg)</span></div>
                    <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono text-xs" style={{ color: getGPUColor('ACTIVE', overallMin) }}>{overallMin.toFixed(1)} <span className="text-[10px] ml-1 opacity-70">(avg)</span></div>
                    <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono text-xs" style={{ color: getGPUColor('ACTIVE', overallMax) }}>{overallMax.toFixed(1)} <span className="text-[10px] ml-1 opacity-70">(avg)</span></div>
                    <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono text-xs" style={{ color: getGPUColor('ACTIVE', overallMean) }}>{overallMean.toFixed(1)} <span className="text-[10px] ml-1 opacity-70">(avg)</span></div>
                    <div className="w-[7%] flex items-center justify-center text-center px-1 font-mono text-xs">{avgStdDev.toFixed(1)} <span className="text-[10px] ml-1 opacity-70">(avg)</span></div>
                    <div className="w-[8%] flex flex-col items-center justify-center text-center px-1 text-xs">
                      <span>{totalThrottledJobs}</span>
                      <span className="text-[10px] opacity-70">({((totalThrottledJobs/completedCount)*100 || 0).toFixed(0)}%)</span>
                    </div>
                    <div className="w-[11%] flex items-center justify-center text-center px-1 font-mono text-xs">{totalThrottleTime.toFixed(0)} <span className="text-[10px] ml-1 opacity-70">(total)</span></div>
                    <div className="w-[8px] shrink-0"></div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}