"use client";

import React, { useState, useRef, useEffect, useMemo, useCallback, memo } from 'react';
import { Play, Pause, FastForward, Activity, Download, Sun, Moon, Home, RefreshCw, Maximize, Minimize, RotateCcw, ChevronUp, ChevronDown, Search, Info } from 'lucide-react';
import { UISimulationState, CompletedJobStat, SchedulingMode, UINodeState, UIGPUState } from '../../lib/simulator/types';

import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend, Filler } from 'chart.js';
import zoomPlugin from 'chartjs-plugin-zoom';
import { Line } from 'react-chartjs-2';
import JSZip from 'jszip';
import { saveAs } from 'file-saver';
import { FaFileArchive } from "react-icons/fa";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend, Filler, zoomPlugin);

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

// --- EXPORTABLE HELPER FUNCTIONS --- //

export const calculateAggregateStats = (stats: CompletedJobStat[]) => {
  if (!stats || stats.length === 0) {
    return { completedCount: 0, avgWait: 0, avgExec: 0, overallMin: 0, overallMax: 0, overallMean: 0, avgStdDev: 0, totalThrottledJobs: 0, totalThrottleTime: 0 };
  }
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
};

export const generateHTMLTemplate = (nodeId: number, labels: number[], nodeData: any, currentTheme: string, currentMode: string) => {
  const isDark = currentTheme === 'dark';
  const bgColor = isDark ? '#0f172a' : '#f8fafc';
  const cardColor = isDark ? '#1e293b' : '#ffffff';
  const textColor = isDark ? '#f8fafc' : '#0f172a';
  const gridColor = isDark ? '#334155' : '#e2e8f0';
  const hintColor = isDark ? '#94a3b8' : '#64748b';
  const btnBg = isDark ? '#334155' : '#e2e8f0';
  const btnHover = isDark ? '#475569' : '#cbd5e1';
  const maxLabel = labels.length > 0 ? labels[labels.length - 1] : 100;

  return `
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Node ${nodeId} Telemetry (${currentMode})</title>
      <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
      <script src="https://cdn.jsdelivr.net/npm/hammerjs@2.0.8"></script>
      <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom"></script>
      <style>
        body { font-family: system-ui, -apple-system, sans-serif; background: ${bgColor}; color: ${textColor}; margin: 0; padding: 20px; }
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
        .header h1 { margin: 0; font-size: 1.5rem; }
        .badge { background: #3b82f6; color: #ffffff; padding: 4px 10px; border-radius: 6px; font-size: 0.875rem; font-weight: bold; }
        .chart-container { position: relative; height: 85vh; width: 100%; background: ${cardColor}; border-radius: 12px; padding: 20px; box-sizing: border-box; border: 1px solid ${gridColor}; }
        .hint { font-size: 0.875rem; color: ${hintColor}; }
        .btn-reset { background: ${btnBg}; color: ${textColor}; border: none; padding: 6px 12px; border-radius: 6px; cursor: pointer; font-size: 0.875rem; font-weight: bold; transition: background 0.2s; }
        .btn-reset:hover { background: ${btnHover}; }
        .controls { display: flex; gap: 12px; align-items: center; }
      </style>
    </head>
    <body>
      <div class="header">
        <h1>Node ${nodeId} Telemetry</h1>
        <div class="controls">
          <span class="hint">Scroll to zoom, drag to pan</span>
          <button id="resetZoomBtn" class="btn-reset">Reset Zoom</button>
          <span class="badge">${currentMode}</span>
        </div>
      </div>
      <div class="chart-container">
        <canvas id="telemetryChart"></canvas>
      </div>
      <script>
        const ctx = document.getElementById('telemetryChart').getContext('2d');
        const labels = ${JSON.stringify(labels)};
        const t0 = ${JSON.stringify(nodeData.t0)};
        const t1 = ${JSON.stringify(nodeData.t1)};
        const p0 = ${JSON.stringify(nodeData.p0)};
        const p1 = ${JSON.stringify(nodeData.p1)};
        const mapData = (dataArr) => dataArr.map((y, i) => ({ x: labels[i], y }));
        const chartInstance = new Chart(ctx, {
          type: 'line',
          data: {
            datasets: [
              { label: ' GPU 0 Temp (°C)', data: mapData(t0), borderColor: '#ef4444', yAxisID: 'y', tension: 0.2, pointRadius: 0, borderWidth: 2 },
              { label: ' GPU 1 Temp (°C)', data: mapData(t1), borderColor: '#f97316', yAxisID: 'y', tension: 0.2, pointRadius: 0, borderWidth: 2, borderDash: [5, 5] },
              { label: ' GPU 0 Power (W)', data: mapData(p0), borderColor: '#3b82f6', yAxisID: 'y1', tension: 0.1, pointRadius: 0, borderWidth: 1, fill: true, backgroundColor: 'rgba(59, 130, 246, 0.1)' },
              { label: ' GPU 1 Power (W)', data: mapData(p1), borderColor: '#8b5cf6', yAxisID: 'y1', tension: 0.1, pointRadius: 0, borderWidth: 1, fill: true, backgroundColor: 'rgba(139, 92, 246, 0.1)' }
            ]
          },
          options: {
            responsive: true, maintainAspectRatio: false, animation: false,
            interaction: { mode: 'index', intersect: false },
            scales: {
              x: { type: 'linear', grid: { display: false }, ticks: { color: '${hintColor}', callback: val => Math.round(val) + 's' } },
              y: { type: 'linear', position: 'left', min: 20, max: 100, title: { display: true, text: 'Temperature (°C)', color: '${hintColor}' }, grid: { color: '${gridColor}' }, ticks: { color: '${hintColor}' } },
              y1: { type: 'linear', position: 'right', min: 0, max: 300, title: { display: true, text: 'Power Draw (W)', color: '${hintColor}' }, grid: { drawOnChartArea: false }, ticks: { color: '${hintColor}' } },
            },
            plugins: {
              legend: { labels: { color: '${textColor}', usePointStyle: true, boxWidth: 20 } },
              zoom: {
                limits: { x: { min: 0, max: ${maxLabel} + 5 } },
                zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'x', speed: 0.05 },
                pan: { enabled: true, mode: 'x' }
              }
            }
          }
        });
        document.getElementById('resetZoomBtn').addEventListener('click', () => chartInstance.resetZoom());
      </script>
    </body>
    </html>
  `;
};

export const downloadSimulationZip = async (
  simulations: { state: UISimulationState; mode: string }[],
  theme: string,
  totalSubmittedJobs: number
) => {
  if (simulations.length === 0) return;
  const zip = new JSZip();

  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, '0');
  const dd = String(now.getDate()).padStart(2, '0');
  const hh = String(now.getHours()).padStart(2, '0');
  const min = String(now.getMinutes()).padStart(2, '0');
  const ss = String(now.getSeconds()).padStart(2, '0');
  const timestamp = `${yyyy}${mm}${dd}${hh}${min}${ss}`;

  for (const sim of simulations) {
    const { state, mode } = sim;
    const aggregateStats = calculateAggregateStats(state.completed_stats);
    
    // Create subfolder for A/B Testing mode separation
    const targetZip = simulations.length > 1 ? zip.folder(`Scheduler_${mode}`)! : zip;

    // 1. Config & Stats
    const configData = {
      simulation_mode: mode,
      ambient_temp_C: state.ambient_temp,
      node_count: state.nodes.length,
      time_elapsed_sec: state.time_elapsed_sec,
      total_submitted_jobs: totalSubmittedJobs,
      global_stats: aggregateStats
    };
    targetZip.file(`Simulation_Config_and_Stats_${mode}.json`, JSON.stringify(configData, null, 2));

    // 2. Summary Table
    const summaryFolder = targetZip.folder("Summary_Data");
    const { completedCount: cc, avgWait, avgExec, overallMin, overallMax, overallMean, avgStdDev, totalThrottledJobs, totalThrottleTime } = aggregateStats;
    const headers = "job_id,node_number,gpu_index,wait_time_sec,execution_time_sec,min_temp_C,max_temp_C,mean_temp_C,temp_std_dev_C,was_throttled,throttle_time_sec\n";
    const rows = state.completed_stats.map(j =>
      `${j.job_id},${j.node_number},${j.gpu_index},${j.wait_time_sec.toFixed(1)},${j.execution_time_sec.toFixed(1)},${j.min_temp_C.toFixed(1)},${j.max_temp_C.toFixed(1)},${j.mean_temp_C.toFixed(1)},${j.temp_std_dev_C.toFixed(2)},${j.was_throttled},${j.throttle_time_sec.toFixed(1)}`
    ).join("\n");
    const overallRow = `\nOVERALL,${cc} Jobs,N/A,${avgWait.toFixed(1)},${avgExec.toFixed(1)},${overallMin.toFixed(1)},${overallMax.toFixed(1)},${overallMean.toFixed(1)},${avgStdDev.toFixed(2)},${totalThrottledJobs},${totalThrottleTime.toFixed(1)}\n`;
    summaryFolder?.file(`Jobs_Summary_${mode}.csv`, headers + rows + overallRow);

    // 3. Telemetry
    const htmlFolder = targetZip.folder("Interactive_Graphs_HTML");
    const csvFolder = targetZip.folder("Telemetry_Data_CSV");

    state.nodes.forEach((node) => {
      const nodeData = state.chart_data.datasets[node.id];
      const labels = state.chart_data.labels;
      if (!nodeData || labels.length === 0) return;

      let csvContent = "time_sec,gpu0_temp_C,gpu1_temp_C,gpu0_power_W,gpu1_power_W\n";
      for (let i = 0; i < labels.length; i++) {
        csvContent += `${labels[i]},${nodeData.t0[i].toFixed(2)},${nodeData.t1[i].toFixed(2)},${nodeData.p0[i].toFixed(2)},${nodeData.p1[i].toFixed(2)}\n`;
      }
      csvFolder?.file(`Node_${node.id}_Telemetry.csv`, csvContent);

      const htmlContent = generateHTMLTemplate(node.id, labels, nodeData, theme, mode);
      htmlFolder?.file(`Node_${node.id}_Telemetry.html`, htmlContent);
    });
  }

  const modeStr = simulations.length > 1 ? "AB_TESTING" : simulations[0].mode;
  const zipBlob = await zip.generateAsync({ type: "blob" });
  saveAs(zipBlob, `Simulation_Comprehensive_Export_${modeStr}_${timestamp}.zip`);
};

// --- END EXPORTABLE HELPER FUNCTIONS --- //

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
      className={`cursor-pointer p-3 sm:p-4 rounded-xl border-2 transition-all duration-300 ${
        isSelected
          ? 'bg-blue-50 dark:bg-slate-800 border-blue-500 shadow-[0_0_20px_rgba(59,130,246,0.25)] dark:shadow-[0_0_20px_rgba(59,130,246,0.15)] z-10'
          : 'bg-white dark:bg-slate-950 border-gray-200/80 dark:border-slate-800 hover:border-blue-300 dark:hover:border-slate-700 hover:shadow-md'
      }`}
    >
      <div className="text-xs sm:text-sm font-bold border-b border-gray-200 dark:border-slate-800 pb-2 mb-3">Node {node.id}</div>
      <div className="space-y-3">
        {gpus.map(([gpu, i]) => {
          const temp = i === 0 ? node.T_die_0 : node.T_die_1;
          const color = getGPUColor(gpu.status, temp);
          return (
            <div key={i} className="flex justify-between items-center">
              <div className="flex flex-col">
                <span className="text-[10px] sm:text-xs font-medium text-gray-500 dark:text-slate-400">
                  GPU {i}
                </span>
                <span className={`text-[9px] sm:text-[10px] text-gray-500 dark:text-slate-400 opacity-70 ${gpu.status === 'ACTIVE' ? 'font-bold' : ''}`}>
                  ({gpu.status})
                </span>
              </div>
              <div className="flex items-center gap-1.5 sm:gap-2">
                <span className="font-mono text-[10px] sm:text-xs font-bold text-gray-700 dark:text-slate-300">
                  {temp.toFixed(1)}°C
                </span>
                <div className="relative flex h-2.5 w-2.5 sm:h-3 sm:w-3">
                  {gpu.status === 'ACTIVE' && <span className="animate-ping absolute inline-flex h-full w-full rounded-full opacity-75" style={{ backgroundColor: color }}></span>}
                  <span className="relative inline-flex rounded-full h-2.5 w-2.5 sm:h-3 sm:w-3 border border-black/20" style={{ backgroundColor: color }}></span>
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
  skipProgressCount?: number;
}

export default function DashboardView(props: DashboardViewProps) {
  const { state, theme, mode, isRunning, isComplete, isProcessing, simSpeed, chartVersion, hideControlBar, isABTest, skipProgressCount = 0 } = props;

  const safeTotalJobs = props.totalSubmittedJobs > 0 ? props.totalSubmittedJobs : 1;
  const skipPercentage = Math.min(100, Math.round((skipProgressCount / safeTotalJobs) * 100));

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
      <span className="font-mono text-lg sm:text-xl font-bold flex items-baseline">
        {String(d).padStart(2, '0')}<span className="text-xs sm:text-sm text-gray-400 dark:text-slate-500 font-medium ml-0.5 mr-1.5">d</span>
        {String(h).padStart(2, '0')}<span className="text-xs sm:text-sm text-gray-400 dark:text-slate-500 font-medium ml-0.5 mr-1.5">h</span>
        {String(m).padStart(2, '0')}<span className="text-xs sm:text-sm text-gray-400 dark:text-slate-500 font-medium ml-0.5 mr-1.5">m</span>
        {String(s).padStart(2, '0')}<span className="text-xs sm:text-sm text-gray-400 dark:text-slate-500 font-medium ml-0.5">s</span>
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

  const aggregateStats = useMemo(() => calculateAggregateStats(state?.completed_stats || []), [state?.completed_stats?.length]);

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
        limits: { x: { min: 0 } }, // Max is set dynamically in useEffect to prevent infinite right scroll
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
      const maxLabel = labels.length > 0 ? labels[labels.length - 1] : 0;
      chart.options.plugins.zoom.limits.x.max = maxLabel > 0 ? maxLabel + 5 : undefined; 
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

  const downloadNodeGraphAsHTML = useCallback(() => {
    if (!state || !state.chart_data) return;
    const nodeData = state.chart_data.datasets[selectedNode];
    const labels = state.chart_data.labels;
    
    if (!nodeData || labels.length === 0) {
      alert("No telemetry data available to download yet.");
      return;
    }

    const htmlContent = generateHTMLTemplate(selectedNode, labels, nodeData, theme, mode);
    const blob = new Blob([htmlContent], { type: 'text/html;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `Node_${selectedNode}_Telemetry_${mode}.html`;
    a.click();
    URL.revokeObjectURL(url);
  }, [state?.chart_data, selectedNode, theme, mode]);

  // Use the exported helper for the single Dashboard export
  const handleComprehensiveExport = useCallback(() => {
    if (!state) return;
    downloadSimulationZip([{ state, mode }], theme, props.totalSubmittedJobs);
  }, [state, mode, theme, props.totalSubmittedJobs]);

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
        <div className="animate-spin rounded-full h-10 w-10 sm:h-12 sm:w-12 border-t-4 border-b-4 border-blue-600 mb-4"></div>
        <p className="text-gray-500 dark:text-slate-400 font-bold tracking-wide text-sm sm:text-base">Initializing ODE Engine & Telemetry...</p>
      </div>
    );
  }

  const { completedCount, avgWait, avgExec, overallMin, overallMax, overallMean, avgStdDev, totalThrottledJobs, totalThrottleTime } = aggregateStats;

  return (
    <div className="flex-1 flex flex-col relative w-full overflow-x-hidden">

      {isProcessing && (
        <div className="fixed inset-0 z-[200] flex flex-col items-center justify-center bg-white/40 dark:bg-slate-950/40 backdrop-blur-sm animate-in fade-in duration-200 p-4">
          <div className="bg-white dark:bg-slate-900 p-6 sm:p-8 rounded-3xl shadow-2xl border border-gray-200 dark:border-slate-800 flex flex-col items-center text-center max-w-sm w-full">
            <div className="relative flex justify-center items-center mb-6">
               <div className="animate-spin rounded-full h-14 w-14 sm:h-16 sm:w-16 border-t-4 border-b-4 border-blue-600"></div>
               <Activity className="absolute w-5 h-5 sm:w-6 sm:h-6 text-blue-500 animate-pulse" />
            </div>
            <h2 className="text-lg sm:text-xl font-bold text-gray-900 dark:text-white mb-2">Fast-Forwarding</h2>
            <p className="text-xs sm:text-sm text-gray-500 dark:text-slate-400 mb-5">
              Calculating ODE mathematics for the remaining workloads. Please wait...
            </p>

            <div className="w-full flex flex-col gap-1.5">
              <div className="flex justify-between items-center text-[10px] sm:text-xs font-bold text-gray-500 dark:text-slate-400">
                <span>{skipProgressCount} / {props.totalSubmittedJobs} Jobs Complete</span>
                <span className="text-blue-600 dark:text-blue-400">{skipPercentage}%</span>
              </div>
              <div className="w-full bg-gray-100 dark:bg-slate-800 rounded-full h-2 overflow-hidden border border-gray-200 dark:border-slate-700 shadow-inner">
                <div
                  className="bg-blue-600 h-full rounded-full transition-all duration-300 ease-out"
                  style={{ width: `${skipPercentage}%` }}
                ></div>
              </div>
            </div>

          </div>
        </div>
      )}

      {activeDropdown && <div className="fixed inset-0 z-40" onClick={() => setActiveDropdown(null)}></div>}

      {!hideControlBar && (
        <div className="bg-white dark:bg-slate-900 border-b border-gray-200 dark:border-slate-800 sticky top-0 z-[60] px-4 sm:px-6 py-3 flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4 shadow-sm">
          <div className="flex items-center gap-3 sm:gap-4 w-full sm:w-auto">
            <button onClick={props.onGoHome} className="p-2 hover:bg-gray-100 dark:hover:bg-slate-800 rounded-lg text-gray-600 dark:text-slate-400 transition-colors"><Home className="w-5 h-5"/></button>
            <div className="h-6 w-px bg-gray-300 dark:bg-slate-700 hidden sm:block"></div>
            <div>
              <div className="flex items-center gap-2 flex-wrap">
                <h1 className="font-bold text-base sm:text-lg leading-none">Simulation Dashboard</h1>
                <span className={`px-2 py-0.5 text-[10px] font-bold rounded uppercase tracking-wide ${mode === 'THERMAL_AWARE' ? 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-400' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-500'}`}>
                  {mode}
                </span>
              </div>
              <p className="text-[10px] sm:text-xs text-gray-500 dark:text-slate-500 mt-0.5">Hardware: NVIDIA V100 (MIT TX-Gaia Cluster)</p>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2 sm:gap-3 w-full sm:w-auto justify-between sm:justify-end">
            
            {/* INDEPENDENT EXPORT BUTTON (LEFT OF PLAYBACK BOX) */}
            <button 
              onClick={handleComprehensiveExport} 
              disabled={!isComplete}
              className={`flex items-center justify-center gap-1.5 px-3 py-2 rounded-lg shadow-sm transition-colors ${
                isComplete 
                  ? 'bg-blue-600 hover:bg-blue-700 text-white' 
                  : 'bg-gray-200 dark:bg-slate-800 text-gray-400 dark:text-slate-500 cursor-not-allowed opacity-70'
              }`} 
              title={isComplete ? "Export All Data (.zip)" : "Simulation must finish before data can be exported."}
            >
              Download ZIP<FaFileArchive className="w-4 h-4" />
            </button>

            <div className="flex flex-wrap items-center gap-2 bg-gray-100 dark:bg-slate-800 p-1.5 rounded-lg border border-gray-200 dark:border-slate-700 w-full sm:w-auto justify-center">
              <select value={simSpeed} onChange={(e) => props.onSpeedChange && props.onSpeedChange(Number(e.target.value))} disabled={isRunning} className="bg-white dark:bg-slate-700 text-xs font-bold px-2 py-1.5 rounded outline-none text-gray-700 dark:text-white border border-gray-200 dark:border-slate-600 disabled:opacity-50">
                <option value={1}>1x Speed</option>
                <option value={10}>10x Speed</option>
                <option value={20}>20x Speed</option>
                <option value={50}>50x Speed</option>
                <option value={100}>100x Speed</option>
              </select>
              <div className="w-px h-4 bg-gray-300 dark:bg-slate-600 mx-1 hidden sm:block"></div>
              {!isRunning && !isComplete ? (
                <button onClick={props.onStart} className="flex flex-1 sm:flex-none justify-center items-center gap-1 bg-emerald-600 hover:bg-emerald-500 text-white px-3 py-1.5 rounded shadow-sm text-sm font-medium transition-colors"><Play className="w-4 h-4" /> Start</button>
              ) : (
                <button onClick={props.onPause} disabled={isComplete} className="flex flex-1 sm:flex-none justify-center items-center gap-1 bg-amber-500 hover:bg-amber-400 text-white disabled:opacity-50 px-3 py-1.5 rounded shadow-sm text-sm font-medium transition-colors"><Pause className="w-4 h-4" /> Pause</button>
              )}
              <button onClick={props.onSkipToEnd} disabled={isComplete || props.totalSubmittedJobs === 0} className="flex flex-1 sm:flex-none justify-center items-center gap-1 bg-gray-200 dark:bg-slate-700 hover:bg-gray-300 dark:hover:bg-slate-600 disabled:opacity-50 px-3 py-1.5 rounded text-sm font-medium"><FastForward className="w-4 h-4" /> Skip to End</button>
              <button onClick={props.onReset} disabled={isRunning} className="flex flex-1 sm:flex-none justify-center items-center gap-1 bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400 disabled:opacity-30 px-3 py-1.5 rounded text-sm font-medium"><RefreshCw className="w-4 h-4" /> Reset</button>
            </div>
            
            {props.onToggleTheme && (
              <button onClick={props.onToggleTheme} className="p-2 bg-gray-200 dark:bg-slate-800 rounded-lg text-gray-700 dark:text-slate-300 hidden sm:block">
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
            ? 'p-4 sm:p-5 rounded-2xl border flex flex-col gap-4 sm:gap-5' 
            : 'p-3 sm:p-4 rounded-2xl border flex flex-col sm:flex-row flex-wrap items-start sm:items-center justify-between px-4 sm:px-8 gap-4'
        }`}>
          
          <div className={isABTest ? "flex flex-col sm:flex-row items-start sm:items-center justify-between w-full px-2 gap-4" : "flex flex-col sm:flex-row items-start sm:items-center gap-4 sm:gap-8 w-full sm:w-auto"}>
            <div>
              <span className="block text-[10px] sm:text-xs text-gray-500 dark:text-slate-400 uppercase tracking-wider font-bold mb-1">Ambient Temp</span>
              <span className="font-mono text-lg sm:text-xl font-bold">{state.ambient_temp}°C</span>
            </div>
            
            {/* Divider for single line format */}
            {!isABTest && <div className="w-px h-8 bg-gray-200 dark:bg-slate-700 hidden sm:block"></div>}
            
            <div className={isABTest ? "sm:text-right" : ""}>
              <span className="block text-[10px] sm:text-xs text-gray-500 dark:text-slate-400 uppercase tracking-wider font-bold mb-1">Time Elapsed</span>
              <div className={`flex items-baseline gap-2 ${isABTest ? 'sm:justify-end' : ''}`}>
                {formatTimeElapsed}
                <span className="text-xs sm:text-sm font-mono text-gray-400 dark:text-slate-500 font-medium">
                  ({state.time_elapsed_sec.toFixed(1)}s)
                </span>
              </div>
            </div>
          </div>

          {/* Divider for 2-line format */}
          {isABTest && <div className="w-full h-px bg-gray-100 dark:bg-slate-800"></div>}

          <div className={`flex flex-wrap items-center ${isABTest ? 'justify-start sm:justify-center gap-3 sm:gap-6 w-full' : 'gap-2 sm:gap-4 w-full sm:w-auto'}`}>
            <div className="relative flex-1 sm:flex-none">
              <div onClick={() => toggleDropdown('SUBMITTED')} className={`cursor-pointer hover:bg-purple-50 dark:hover:bg-slate-800 p-2 px-2 sm:px-4 rounded-lg transition-colors text-center border ${activeDropdown==='SUBMITTED'?'bg-purple-50 dark:bg-slate-800 border-purple-200 dark:border-slate-600':'border-transparent hover:border-purple-100 dark:hover:border-slate-700'}`}>
                <span className="block text-[10px] sm:text-xs text-gray-500 dark:text-slate-400 uppercase tracking-wider font-bold mb-1 truncate">Submitted</span>
                <span className="font-mono text-base sm:text-xl font-bold">{props.totalSubmittedJobs}</span>
              </div>
              {activeDropdown === 'SUBMITTED' && (
                <div className={`absolute top-full mt-2 w-56 sm:w-64 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 rounded-xl shadow-xl overflow-hidden flex flex-col left-0 ${!isABTest ? 'sm:left-auto sm:right-0' : ''}`}>
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

            <div className="relative flex-1 sm:flex-none">
              <div onClick={() => toggleDropdown('QUEUED')} className={`cursor-pointer hover:bg-orange-50 dark:hover:bg-slate-800 p-2 px-2 sm:px-4 rounded-lg transition-colors text-center border ${activeDropdown==='QUEUED'?'bg-orange-50 dark:bg-slate-800 border-orange-200 dark:border-slate-600':'border-transparent hover:border-orange-100 dark:hover:border-slate-700'}`}>
                <span className="block text-[10px] sm:text-xs text-gray-500 dark:text-slate-400 uppercase tracking-wider font-bold mb-1 truncate">Queued</span>
                <span className="font-mono text-base sm:text-xl font-bold">{state.queued_job_ids.length}</span>
              </div>
              {activeDropdown === 'QUEUED' && (
                <div className={`absolute top-full mt-2 w-56 sm:w-64 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 rounded-xl shadow-xl overflow-hidden flex flex-col left-0 sm:left-auto ${isABTest ? 'sm:left-1/2 sm:-translate-x-1/2' : 'sm:right-0'}`}>
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

            <div className="relative flex-1 sm:flex-none">
              <div onClick={() => toggleDropdown('ACTIVE')} className={`cursor-pointer hover:bg-blue-50 dark:hover:bg-slate-800 p-2 px-2 sm:px-4 rounded-lg transition-colors text-center border ${activeDropdown==='ACTIVE'?'bg-blue-50 dark:bg-slate-800 border-blue-200 dark:border-slate-600':'border-transparent hover:border-blue-100 dark:hover:border-slate-700'}`}>
                <span className="block text-[10px] sm:text-xs text-gray-500 dark:text-slate-400 uppercase tracking-wider font-bold mb-1 truncate">Active</span>
                <span className="font-mono text-base sm:text-xl font-bold">{state.active_job_ids.length}</span>
              </div>
              {activeDropdown === 'ACTIVE' && (
                <div className={`absolute top-full mt-2 w-64 sm:w-72 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 rounded-xl shadow-xl overflow-hidden flex flex-col left-0 sm:left-auto ${isABTest ? 'sm:left-1/2 sm:-translate-x-1/2' : 'sm:right-0'}`}>
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

            <div className="relative flex-1 sm:flex-none">
              <div onClick={() => toggleDropdown('COMPLETED')} className={`cursor-pointer hover:bg-emerald-50 dark:hover:bg-slate-800 p-2 px-2 sm:px-4 rounded-lg transition-colors text-center border ${activeDropdown==='COMPLETED'?'bg-emerald-50 dark:bg-slate-800 border-emerald-200 dark:border-slate-600':'border-transparent hover:border-emerald-100 dark:hover:border-slate-700'}`}>
                <span className="block text-[10px] sm:text-xs text-emerald-600 dark:text-emerald-500 uppercase tracking-wider font-bold mb-1 truncate">Completed</span>
                <span className="font-mono text-base sm:text-xl font-bold text-emerald-600 dark:text-emerald-500">{uniqueCompletedIds.length}</span>
              </div>
              {activeDropdown === 'COMPLETED' && (
                <div className="absolute top-full mt-2 w-56 sm:w-64 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 rounded-xl shadow-xl overflow-hidden flex flex-col right-0">
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

            <div className="relative flex-1 sm:flex-none">
              <div onClick={() => toggleDropdown('FAILED')} className={`cursor-pointer hover:bg-red-50 dark:hover:bg-slate-800 p-2 px-2 sm:px-4 rounded-lg transition-colors text-center border ${activeDropdown==='FAILED'?'bg-red-50 dark:bg-slate-800 border-red-200 dark:border-slate-600':'border-transparent hover:border-red-100 dark:hover:border-slate-700'}`}>
                <span className="block text-[10px] sm:text-xs text-red-600 dark:text-red-500 uppercase tracking-wider font-bold mb-1 truncate">Failed</span>
                <span className="font-mono text-base sm:text-xl font-bold text-red-600 dark:text-red-500">{state.failed_job_ids.length}</span>
              </div>
              {activeDropdown === 'FAILED' && (
                <div className="absolute top-full mt-2 w-56 sm:w-64 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 rounded-xl shadow-xl overflow-hidden flex flex-col right-0">
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

        <div className={`grid ${isABTest ? 'grid-cols-1 gap-4' : 'grid-cols-1 lg:grid-cols-2 gap-3 sm:gap-4'}`}>
          <div className={`bg-white dark:bg-slate-900 p-4 sm:p-5 rounded-2xl border border-gray-200 dark:border-slate-800 shadow-sm flex flex-col ${isABTest ? 'h-[375px]' : 'h-[375px] sm:h-[425px]'}`}>
            
            <div className="flex items-center justify-between mb-4 shrink-0 flex-wrap gap-2">
              <div className="flex items-center gap-2">
                <h2 className="text-sm sm:text-base font-bold">Node Thermal Overview (Nodes: {state.nodes.length})</h2>
                <div className="group relative flex items-center">
                  <Info className="w-4 h-4 text-gray-400 cursor-help" />
                  <div className="absolute left-6 w-56 sm:w-64 p-2 bg-gray-800 text-white text-[10px] sm:text-xs rounded opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-10">
                    Click on any node to stream its telemetry directly into the real-time graph.
                  </div>
                </div>
              </div>
            </div>

            <div className={`grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-3 sm:gap-4 flex-1 overflow-y-auto min-h-0 pr-2 custom-scrollbar content-start`}>
              {state.nodes.map((node) => (
                <div key={node.id} ref={(el) => { nodeRefs.current[node.id] = el; }}>
                  <NodeCard node={node} isSelected={selectedNode === node.id} onSelect={handleNodeSelect} />
                </div>
              ))}
            </div>
          </div>

          <div className={isGraphFullScreen ? "fixed inset-0 z-[100] bg-white dark:bg-slate-950 p-4 sm:p-8 flex flex-col" : `bg-white dark:bg-slate-900 p-4 sm:p-5 rounded-2xl border border-gray-200 dark:border-slate-800 shadow-sm flex flex-col ${isABTest ? 'h-[375px]' : 'h-[375px] sm:h-[425px]'}`}>
            <div className="flex items-center justify-between mb-1 shrink-0 flex-wrap gap-2">
              <div className="flex items-center gap-2">
                <Activity className="w-4 h-4 sm:w-5 sm:h-5 text-blue-500 shrink-0" />
                <h3 className="text-sm sm:text-base font-bold truncate">Real-time Telemetry (Node {selectedNode})</h3>
              </div>
              <div className="flex items-center gap-2">
                {(!isRunning && state.time_elapsed_sec > 0) && <span className="hidden sm:inline-block text-[10px] sm:text-xs text-blue-600 dark:text-blue-400 font-medium bg-blue-50 dark:bg-blue-900/30 px-2 py-1 rounded">Scroll to zoom, drag to pan</span>}
                
                <button onClick={downloadNodeGraphAsHTML} className="p-1.5 bg-gray-100 hover:bg-gray-200 dark:bg-slate-800 dark:hover:bg-slate-700 rounded text-gray-600 dark:text-slate-400 transition-colors" title="Download Interactive Graph (.html)">
                  <Download className="w-3.5 h-3.5 sm:w-4 sm:h-4" />
                </button>

                <button onClick={resetZoom} className="p-1.5 bg-gray-100 hover:bg-gray-200 dark:bg-slate-800 dark:hover:bg-slate-700 rounded text-gray-600 dark:text-slate-400 transition-colors" title="Reset Zoom Scale">
                  <RotateCcw className="w-3.5 h-3.5 sm:w-4 sm:h-4" />
                </button>
                <button onClick={() => setIsGraphFullScreen(!isGraphFullScreen)} className="p-1.5 bg-gray-100 hover:bg-gray-200 dark:bg-slate-800 dark:hover:bg-slate-700 rounded text-gray-600 dark:text-slate-400 transition-colors" title={isGraphFullScreen ? "Minimize" : "Full Screen"}>
                  {isGraphFullScreen ? <Minimize className="w-3.5 h-3.5 sm:w-4 sm:h-4" /> : <Maximize className="w-3.5 h-3.5 sm:w-4 sm:h-4" />}
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
              <div className="p-3 sm:p-4 border-b border-gray-200 dark:border-slate-800 flex flex-col sm:flex-row justify-between items-start sm:items-center bg-gray-50 dark:bg-slate-900 gap-3">
                <h2 className="text-sm sm:text-base font-bold">Summary Table</h2>
                <div className="flex flex-wrap items-center gap-2 sm:gap-3 w-full sm:w-auto">
                  <div className="flex items-center gap-2 bg-white dark:bg-slate-800 border border-gray-300 dark:border-slate-600 rounded px-2 sm:px-3 py-1.5 shadow-inner flex-1 sm:flex-none">
                    <Search className="w-3.5 h-3.5 sm:w-4 sm:h-4 text-gray-400 shrink-0" />
                    <input
                      type="text"
                      placeholder="Search Job ID..."
                      value={tableSearch}
                      onChange={(e) => setTableSearch(e.target.value)}
                      className="bg-transparent outline-none text-xs sm:text-sm w-full sm:w-48 text-gray-700 dark:text-gray-200 placeholder-gray-400"
                    />
                  </div>
                  <button onClick={downloadReport} className="flex items-center justify-center gap-1 text-xs sm:text-sm bg-blue-600 hover:bg-blue-700 text-white px-3 sm:px-4 py-1.5 sm:py-2 rounded shadow transition-colors flex-1 sm:flex-none whitespace-nowrap">
                    <Download className="w-3.5 h-3.5 sm:w-4 sm:h-4" /> Export CSV
                  </button>
                </div>
              </div>

              <div className="overflow-x-auto w-full custom-scrollbar">
                <div className="flex flex-col w-full text-xs sm:text-sm min-w-[1050px]">
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
                                <div className="w-[24%] flex items-center px-4 font-mono text-[10px] sm:text-[11px] truncate" title={j.job_id}>{j.job_id}</div>
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
                    <div className="w-[24%] flex items-center px-4 uppercase tracking-wider text-[10px] sm:text-xs">Overall</div>
                    <div className="w-[10%] flex items-center justify-center text-center px-1 text-[10px] sm:text-[11px]">{completedCount} Jobs</div>
                    <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono text-[10px] sm:text-xs">{avgWait.toFixed(0)} <span className="text-[9px] sm:text-[10px] ml-1 opacity-70">(avg)</span></div>
                    <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono text-[10px] sm:text-xs">{avgExec.toFixed(0)} <span className="text-[9px] sm:text-[10px] ml-1 opacity-70">(avg)</span></div>
                    <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono text-[10px] sm:text-xs" style={{ color: getGPUColor('ACTIVE', overallMin) }}>{overallMin.toFixed(1)} <span className="text-[9px] sm:text-[10px] ml-1 opacity-70">(avg)</span></div>
                    <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono text-[10px] sm:text-xs" style={{ color: getGPUColor('ACTIVE', overallMax) }}>{overallMax.toFixed(1)} <span className="text-[9px] sm:text-[10px] ml-1 opacity-70">(avg)</span></div>
                    <div className="w-[8%] flex items-center justify-center text-center px-1 font-mono text-[10px] sm:text-xs" style={{ color: getGPUColor('ACTIVE', overallMean) }}>{overallMean.toFixed(1)} <span className="text-[9px] sm:text-[10px] ml-1 opacity-70">(avg)</span></div>
                    <div className="w-[7%] flex items-center justify-center text-center px-1 font-mono text-[10px] sm:text-xs">{avgStdDev.toFixed(1)} <span className="text-[9px] sm:text-[10px] ml-1 opacity-70">(avg)</span></div>
                    <div className="w-[8%] flex flex-col items-center justify-center text-center px-1 text-[10px] sm:text-xs">
                      <span>{totalThrottledJobs}</span>
                      <span className="text-[9px] sm:text-[10px] opacity-70">({((totalThrottledJobs/completedCount)*100 || 0).toFixed(0)}%)</span>
                    </div>
                    <div className="w-[11%] flex items-center justify-center text-center px-1 font-mono text-[10px] sm:text-xs">{totalThrottleTime.toFixed(0)} <span className="text-[9px] sm:text-[10px] ml-1 opacity-70">(total)</span></div>
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