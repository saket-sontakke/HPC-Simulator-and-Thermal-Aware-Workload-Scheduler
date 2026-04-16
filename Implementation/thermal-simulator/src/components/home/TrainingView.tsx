"use client";

import React, { useEffect, useState } from 'react';
import { ArrowLeft, Brain, Activity, TrendingDown, Database, Sun, Moon, Braces, Settings2, ShieldAlert, Maximize2, X, Info } from 'lucide-react';
import 'katex/dist/katex.min.css';
import { BlockMath, InlineMath } from 'react-katex';
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip as ChartTooltip, Legend, Filler } from 'chart.js';
import { Line } from 'react-chartjs-2';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, ChartTooltip, Legend, Filler);

interface TrainingViewProps {
  theme: 'dark' | 'light';
  onToggleTheme: () => void;
  onGoHome: () => void;
}

const sigmoidDerivation = String.raw`
\begin{aligned}
\text{norm} &= \frac{\text{val} - \text{low}}{\text{high} - \text{low}} \\[1em]
\sigma^{-1}(\text{norm}) &= \ln \left( \frac{\text{norm}}{1 - \text{norm}} \right)
\end{aligned}
`;

const lossDerivation = String.raw`
\mathcal{L}(\theta) = \frac{\sum_{i=1}^{B} \sum_{t=0}^{S} \left[ \left( \hat{T}_{die}^{(i,t)}(\theta) - T_{true}^{(i,t)} \right)^2 \cdot M^{(i,t)} \right]}{\sum_{i=1}^{B} \sum_{t=0}^{S} M^{(i,t)}}
`;

export default function TrainingView({ theme, onToggleTheme, onGoHome }: TrainingViewProps) {
  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);

  const [copied, setCopied] = useState(false);
  const [csvData, setCsvData] = useState<any>(null);
  const [expandedChart, setExpandedChart] = useState<{ title: string, data: any } | null>(null);
  const [paramTab, setParamTab] = useState<'initial' | 'bounds' | 'final'>('final');

  useEffect(() => {
    fetch('/training_log.csv')
      .then(response => response.text())
      .then(csvText => {
        const lines = csvText.trim().split('\n');
        if (lines.length < 2) return;
        
        const headers = lines[0].split(',').map(h => h.trim().replace(/\r/g, ''));
        
        const parsed: Record<string, number[]> = {
          epochs: [], train_rmse: [], val_rmse: [], train_mae: [], val_mae: [], lr: [],
          C_die_0: [], C_die_1: [], C_sink_0: [], C_sink_1: [],
          R_paste_0: [], R_paste_1: [], k01: [], k10: [],
          q0: [], q1: [], h_base_0: [], h_base_1: [],
          h_active_0: [], h_active_1: [], T_thresh_0: [], T_thresh_1: []
        };

        const idx = {
          epoch: headers.indexOf('epoch'),
          train_rmse: headers.indexOf('train_rmse'), val_rmse: headers.indexOf('val_rmse'),
          train_mae: headers.indexOf('train_mae'), val_mae: headers.indexOf('val_mae'),
          lr: headers.indexOf('lr'),
          C_die_0: headers.indexOf('C_die0'), C_die_1: headers.indexOf('C_die1'),
          C_sink_0: headers.indexOf('C_sink0'), C_sink_1: headers.indexOf('C_sink1'),
          R_paste_0: headers.indexOf('R_pst0'), R_paste_1: headers.indexOf('R_pst1'),
          k01: headers.indexOf('k01'), k10: headers.indexOf('k10'),
          q0: headers.indexOf('q0'), q1: headers.indexOf('q1'),
          h_base_0: headers.indexOf('h_base0'), h_base_1: headers.indexOf('h_base1'),
          h_active_0: headers.indexOf('h_act0'), h_active_1: headers.indexOf('h_act1'),
          T_thresh_0: headers.indexOf('T_thr0'), T_thresh_1: headers.indexOf('T_thr1')
        };

        for (let i = 1; i < lines.length; i++) {
          const cols = lines[i].split(',').map(c => c.trim().replace(/\r/g, ''));
          if (cols.length < headers.length) continue;

          const ep = parseInt(cols[idx.epoch]);
          if (ep > 81) break;

          const parseCol = (index: number) => index !== -1 && cols[index] ? parseFloat(cols[index]) : NaN;

          parsed.epochs.push(ep);
          parsed.train_rmse.push(parseCol(idx.train_rmse));
          parsed.val_rmse.push(parseCol(idx.val_rmse));
          parsed.train_mae.push(parseCol(idx.train_mae));
          parsed.val_mae.push(parseCol(idx.val_mae));
          parsed.lr.push(parseCol(idx.lr));
          
          parsed.C_die_0.push(parseCol(idx.C_die_0));
          parsed.C_die_1.push(parseCol(idx.C_die_1));
          parsed.C_sink_0.push(parseCol(idx.C_sink_0));
          parsed.C_sink_1.push(parseCol(idx.C_sink_1));
          parsed.R_paste_0.push(parseCol(idx.R_paste_0));
          parsed.R_paste_1.push(parseCol(idx.R_paste_1));
          
          parsed.k01.push(parseCol(idx.k01));
          parsed.k10.push(parseCol(idx.k10));
          parsed.q0.push(parseCol(idx.q0));
          parsed.q1.push(parseCol(idx.q1));
          
          parsed.h_base_0.push(parseCol(idx.h_base_0));
          parsed.h_base_1.push(parseCol(idx.h_base_1));
          parsed.h_active_0.push(parseCol(idx.h_active_0));
          parsed.h_active_1.push(parseCol(idx.h_active_1));
          parsed.T_thresh_0.push(parseCol(idx.T_thresh_0));
          parsed.T_thresh_1.push(parseCol(idx.T_thresh_1));
        }
        setCsvData(parsed);
      })
      .catch(err => console.error("Error loading CSV:", err));
  }, []);

  const jsonString = `{
    "C_die_0": 8.932404518127441,
    "C_die_1": 8.870683670043945,
    "C_sink_0": 4713.5888671875,
    "C_sink_1": 4831.154296875,
    "R_paste_0": 0.03658164292573929,
    "R_paste_1": 0.033572204411029816,
    "k01": 0.01670851558446884,
    "k10": 0.002835234859958291,
    "q0": -8.92023754119873,
    "q1": -8.94283390045166,
    "h_base_0": 3.979130983352661,
    "h_base_1": 4.761749267578125,
    "h_active_0": 20.911367416381836,
    "h_active_1": 19.817806243896484,
    "T_thresh_0": 70.25069427490234,
    "T_thresh_1": 67.51343536376953
}`;

  const initialParamsString = `{
    "C_die_0": 2.0,
    "C_die_1": 2.0,
    "C_sink_0": 500.0,
    "C_sink_1": 500.0,
    "R_paste_0": 0.05,
    "R_paste_1": 0.05,
    "k01": 0.0780,
    "k10": 0.0281,
    "q0": 0.0,
    "q1": 0.0,
    "h_base_0": 4.0,
    "h_base_1": 4.0,
    "h_active_0": 12.0,
    "h_active_1": 12.0,
    "T_thresh_0": 65.0,
    "T_thresh_1": 65.0,
    "beta_0": 0.5,
    "beta_1": 0.5
}`;

  const boundsString = `{
    "C_die": [0.1, 10.0],
    "C_sink": [100.0, 5000.0],
    "R_paste": [0.001, 0.2],
    "k01, k10": [0.0, 0.5],
    "q0, q1": [-10.0, 10.0],
    "h_base": [0.5, 10.0],
    "h_active": [0.0, 30.0],
    "T_thresh": [40.0, 85.0],
    "beta": [0.05, 2.0]
}`;

  const copyToClipboard = () => {
    const textToCopy = paramTab === 'initial' ? initialParamsString 
                     : paramTab === 'bounds' ? boundsString 
                     : jsonString;
    navigator.clipboard.writeText(textToCopy);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const getChartOptions = (isExpanded: boolean = false) => ({
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index' as const, intersect: false },
    scales: {
      x: { 
        display: true,
        grid: { display: false }, 
        ticks: { color: theme === 'dark' ? '#94a3b8' : '#64748b', maxTicksLimit: 10 },
        title: { display: true, text: 'Epoch', color: theme === 'dark' ? '#94a3b8' : '#64748b' }
      },
      y: { 
        display: true,
        border: { display: false }, 
        grid: { color: theme === 'dark' ? '#334155' : '#e2e8f0' },
        ticks: { color: theme === 'dark' ? '#94a3b8' : '#64748b' }
      }
    },
    plugins: {
      legend: { 
        display: true, 
        position: 'top' as const, 
        labels: { boxWidth: 10, usePointStyle: true, color: theme === 'dark' ? '#cbd5e1' : '#475569', font: { size: isExpanded ? 14 : 12 } } 
      }
    }
  });

  const buildDataset = (label1: string, data1: number[], label2?: string, data2?: number[]) => {
    if (!csvData) return { labels: [], datasets: [] };
    const datasets = [{ label: label1, data: data1, borderColor: '#10b981', tension: 0.2, pointRadius: 0, borderWidth: 2 }];
    if (label2 && data2) {
      datasets.push({ label: label2, data: data2, borderColor: '#f59e0b', tension: 0.2, pointRadius: 0, borderWidth: 2 });
    }
    return { labels: csvData.epochs, datasets };
  };

  return (
    <div className="flex-1 overflow-y-auto custom-scrollbar p-8 relative">
      
      {/* Fullscreen Chart Modal */}
      {expandedChart && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-6 bg-gray-900/80 backdrop-blur-sm animate-in fade-in duration-200">
          <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-700 w-full max-w-6xl h-[85vh] rounded-2xl shadow-2xl flex flex-col overflow-hidden">
            <div className="flex justify-between items-center p-4 border-b border-gray-200 dark:border-slate-800 bg-gray-50 dark:bg-slate-950">
              <h3 className="text-lg font-bold text-gray-800 dark:text-white flex items-center gap-2">
                <Activity className="w-5 h-5 text-emerald-500" /> {expandedChart.title}
              </h3>
              <button 
                onClick={() => setExpandedChart(null)}
                className="p-2 bg-gray-200 dark:bg-slate-800 hover:bg-gray-300 dark:hover:bg-slate-700 rounded-lg text-gray-600 dark:text-gray-300 transition-colors"
              >
                <X className="w-5 h-5" />
              </button>
            </div>
            <div className="flex-1 p-8 min-h-0 bg-white dark:bg-slate-900">
              <Line data={expandedChart.data} options={getChartOptions(true)} />
            </div>
          </div>
        </div>
      )}

      <div className="max-w-7xl mx-auto w-full space-y-8 pb-12">
        {/* Header */}
        <div className="flex justify-between items-center border-b border-gray-200 dark:border-slate-800 pb-6">
          <div className="flex items-center gap-4">
            <button 
              onClick={onGoHome} 
              className="p-2 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 hover:bg-gray-100 dark:hover:bg-slate-700 rounded-lg text-gray-600 dark:text-slate-400 transition-colors"
              title="Back to Home"
            >
              <ArrowLeft className="w-5 h-5" />
            </button>
            <div>
              <h1 className="text-3xl font-bold tracking-tight">Model Calibration & Training</h1>
              <p className="text-gray-500 dark:text-slate-400 mt-1">PyTorch optimization and ODE parameter discovery.</p>
            </div>
          </div>
          
          <button 
            onClick={onToggleTheme} 
            className="p-2 bg-gray-200 dark:bg-slate-800 rounded-lg text-gray-700 dark:text-slate-300 hover:bg-gray-300 dark:hover:bg-slate-700 transition-colors"
          >
            {theme === 'dark' ? <Sun className="w-4 h-4"/> : <Moon className="w-4 h-4"/>}
          </button>
        </div>

        {/* Intro Banner */}
        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-6 text-emerald-900 dark:text-emerald-100 text-justify shadow-sm">
          <p className="text-lg leading-relaxed">
            The numerical Ordinary Differential Equations (ODEs) defined in the Physics Engine require precise, real-world constants to function accurately. 
            To create a digital twin of the MIT TX-Gaia cluster, a custom <strong>PyTorch Autograd Pipeline</strong> was constructed. 
            By integrating the ODE forward in time and calculating the error against real hardware telemetry, gradient descent automatically 
            calibrated the physical heat capacities, cooling coefficients, and thermal resistances of the V100 nodes.
          </p>
        </div>

        {/* System Architecture & Memory */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-6 shadow-sm flex flex-col">
              <div className="flex items-center gap-3 mb-4">
                <div className="bg-emerald-100 dark:bg-emerald-900/30 p-2 rounded-lg">
                  <Database className="w-6 h-6 text-emerald-600 dark:text-emerald-400" />
                </div>
                <h2 className="text-xl font-bold">VRAM Data Prefetching</h2>
              </div>
              <p className="text-gray-600 dark:text-slate-400 text-sm flex-1 leading-relaxed text-justify">
                To evaluate thousands of workloads per epoch, a <code>MultiChunkPrefetcher</code> was engineered. 
                This daemon thread loads 10,000-segment data chunks from SSD into pinned CPU RAM, and uses non-blocking CUDA streams to transfer them to GPU VRAM in the background. 
                This effectively masked I/O latency, achieving continuous GPU utilization during the forward numerical integrations.
              </p>
            </div>

            <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-6 shadow-sm flex flex-col h-full">
              <div className="flex items-center gap-3 mb-4">
                <div className="bg-emerald-100 dark:bg-emerald-900/30 p-2 rounded-lg">
                  <Settings2 className="w-6 h-6 text-emerald-600 dark:text-emerald-400" />
                </div>
                <h2 className="text-xl font-bold">Bounded Gradient Descent</h2>
              </div>
              <p className="leading-relaxed text-gray-600 dark:text-slate-400 text-sm text-justify">
                  Standard gradient descent can push parameters into non-physical realms (e.g., negative heat capacity). 
                  To strictly enforce physical bounds (e.g., <InlineMath math="C_{die} \in [0.1, 10.0]" />), raw `nn.Parameters` were initialized using an <strong>inverse sigmoid transform</strong>, and mapped back during the forward pass.
              </p>
            </div>
        </div>

        {/* Mathematics & Loss Function */}
        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-6 shadow-sm">
          <div className="flex items-center gap-3 mb-6">
            <div className="bg-emerald-100 dark:bg-emerald-900/30 p-2 rounded-lg">
              <Brain className="w-6 h-6 text-emerald-600 dark:text-emerald-400" />
            </div>
            <h2 className="text-xl font-bold">Masked MSE Loss Optimization</h2>
          </div>
          <p className="text-gray-600 dark:text-slate-400 text-sm leading-relaxed mb-6 text-justify">
            Because datacenter jobs are of highly variable lengths, pad-tokens were utilized to create dense matrix batches. 
            The Loss Function is a <strong>Masked Mean Squared Error (MSE)</strong>, ensuring that only valid computational timesteps (<InlineMath math="M^{(i,t)} = 1" />) 
            contribute to the gradient of the simulated die temperatures (<InlineMath math="\hat{T}_{die}" />) against the ground-truth (<InlineMath math="T_{true}" />).
            Optimization was handled by the <strong>Adam</strong> optimizer with a `ReduceLROnPlateau` scheduler.
          </p>
          
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 pt-2">
              <div className="bg-gray-50 dark:bg-slate-950 rounded-xl p-6 border border-gray-100 dark:border-slate-800 flex flex-col justify-start overflow-x-auto text-gray-800 dark:text-slate-200 shadow-inner">
                <div className="text-gray-400 dark:text-slate-500 mb-4 text-xs uppercase tracking-wider font-sans font-bold text-center">Bounded Parameter Transform</div>
                <div className="py-2 mt-2 font-mono text-[0.95rem]">
                  <BlockMath math={sigmoidDerivation} />
                </div>
              </div>

              <div className="bg-gray-50 dark:bg-slate-950 rounded-xl p-6 border border-gray-100 dark:border-slate-800 flex flex-col justify-start overflow-x-auto text-gray-800 dark:text-slate-200 shadow-inner">
                <div className="text-gray-400 dark:text-slate-500 mb-4 text-xs uppercase tracking-wider font-sans font-bold text-center">Masked Loss Objective</div>
                <div className="py-2 mt-4 font-mono text-[0.95rem]">
                  <BlockMath math={lossDerivation} />
                </div>
              </div>
          </div>
        </div>

        {/* Training Metrics (Epoch 81) */}
        <div>
          <div className="flex items-center gap-3 mb-4 pl-2">
            <TrendingDown className="w-6 h-6 text-emerald-600 dark:text-emerald-500" />
            <h2 className="text-xl font-bold text-gray-900 dark:text-white">Convergence (Best Epoch: 81)</h2>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <MetricCard title="Train RMSE" value="2.26 °C" sub="Root Mean Square Error" />
              <MetricCard title="Train MAE" value="1.42 °C" sub="Mean Absolute Error" />
              <MetricCard title="Val RMSE" value="2.28 °C" sub="Validation Root Mean Square Error" />
              <MetricCard title="Val MAE" value="1.44 °C" sub="Validation Mean Absolute Error" />
          </div>
        </div>

        {/* Testing Results */}
        <div className="bg-gray-50 dark:bg-slate-900/50 border border-gray-200 dark:border-slate-800 rounded-2xl p-6 shadow-inner mt-8">
          <div className="flex items-center justify-between mb-6 border-b border-gray-200 dark:border-slate-700 pb-4">
            <div className="flex items-center gap-3">
              <ShieldAlert className="w-6 h-6 text-emerald-500" />
              <h2 className="text-xl font-bold">Final Model Testing Metrics (Inference)</h2>
            </div>
            <span className="bg-white dark:bg-slate-800 text-gray-600 dark:text-gray-300 text-xs font-bold px-3 py-1 rounded-full border border-gray-200 dark:border-slate-700">
              443,815,380 Timesteps Evaluated
            </span>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
            <div className="space-y-4">
              <h3 className="text-sm font-bold text-gray-500 dark:text-slate-400 uppercase tracking-wider">Global Accuracy</h3>
              <ul className="space-y-3 font-mono text-sm">
                <MetricRow label="RMSE" value="2.2849 °C" colorClass="text-emerald-600 dark:text-emerald-500" tooltip="Root Mean Square Error across all testing nodes. Standard deviation of prediction errors." />
                <MetricRow label="MAE" value="1.4561 °C" colorClass="text-emerald-600 dark:text-emerald-500" tooltip="Mean Absolute Error. The average absolute difference between predicted and actual temps." />
                <MetricRow label="Directional Bias" value="+0.1955 °C" colorClass="text-amber-500 dark:text-amber-400" tooltip="Slight positive bias indicates the model predicts temperatures roughly ~0.2°C hotter than reality (a safe fail-state)." />
              </ul>
            </div>
            
            <div className="space-y-4">
              <h3 className="text-sm font-bold text-gray-500 dark:text-slate-400 uppercase tracking-wider">Tail-Risk Metrics</h3>
              <ul className="space-y-3 font-mono text-sm">
                <MetricRow label="95th Percentile Error" value="4.6601 °C" colorClass="text-amber-500 dark:text-amber-400" tooltip="95% of all predictions in the testing dataset are accurate within 4.6°C." />
                <MetricRow label="99th Percentile Error" value="7.9498 °C" colorClass="text-orange-500 dark:text-orange-400" tooltip="99% of all predictions in the testing dataset are accurate within 7.9°C." />
                <MetricRow label="Absolute Max Error" value="34.6839 °C" colorClass="text-red-500 dark:text-red-400" tooltip="The absolute worst single prediction out of 443 Million timesteps. Usually caused by anomalous hardware sensor spikes." />
              </ul>
            </div>

            <div className="space-y-4">
              <h3 className="text-sm font-bold text-gray-500 dark:text-slate-400 uppercase tracking-wider">Stability & Danger Zone</h3>
              <ul className="space-y-3 font-mono text-sm">
                <MetricRow label="Danger Zone RMSE" value="5.3853 °C" colorClass="text-orange-500 dark:text-orange-400" tooltip="The model's accuracy specifically when the real server hardware exceeds 70°C (thermal throttling territory)." />
                <MetricRow label="First 10% RMSE" value="3.7968 °C" colorClass="text-amber-500 dark:text-amber-400" tooltip="Accuracy during the initialization phase of jobs. Higher error here is expected as initial ambient state normalizes." />
                <MetricRow label="Last 10% RMSE" value="2.1676 °C" colorClass="text-emerald-600 dark:text-emerald-500" tooltip="Accuracy at the end of jobs once thermodynamic equilibrium is achieved." />
              </ul>
            </div>
          </div>
        </div>

        {/* Parameters Block (Initial, Bounds, Final) */}
        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-6 shadow-sm">
          <div className="flex flex-col xl:flex-row xl:items-center justify-between mb-4 gap-4">
            
            <div className="flex items-center gap-3">
              <div className="bg-emerald-100 dark:bg-emerald-900/30 p-2 rounded-lg">
                <Braces className="w-6 h-6 text-emerald-600 dark:text-emerald-400" />
              </div>
              <h2 className="text-xl font-bold">Physics Parameters</h2>
            </div>
            
            <div className="flex items-center justify-between xl:justify-end gap-4 w-full xl:w-auto">
              {/* Tabs */}
              <div className="flex bg-gray-100 dark:bg-slate-800 p-1 rounded-lg">
                <button 
                  onClick={() => setParamTab('initial')}
                  className={`px-3 py-1.5 text-xs font-bold rounded-md transition-all ${paramTab === 'initial' ? 'bg-white dark:bg-slate-900 text-emerald-600 dark:text-emerald-400 shadow-sm' : 'text-gray-500 dark:text-slate-400 hover:text-gray-700 dark:hover:text-slate-200'}`}
                >
                  Initializations
                </button>
                <button 
                  onClick={() => setParamTab('bounds')}
                  className={`px-3 py-1.5 text-xs font-bold rounded-md transition-all ${paramTab === 'bounds' ? 'bg-white dark:bg-slate-900 text-emerald-600 dark:text-emerald-400 shadow-sm' : 'text-gray-500 dark:text-slate-400 hover:text-gray-700 dark:hover:text-slate-200'}`}
                >
                  Bounds
                </button>
                <button 
                  onClick={() => setParamTab('final')}
                  className={`px-3 py-1.5 text-xs font-bold rounded-md transition-all ${paramTab === 'final' ? 'bg-white dark:bg-slate-900 text-emerald-600 dark:text-emerald-400 shadow-sm' : 'text-gray-500 dark:text-slate-400 hover:text-gray-700 dark:hover:text-slate-200'}`}
                >
                  Final Calibrated
                </button>
              </div>

              <button 
                onClick={copyToClipboard}
                className="text-xs font-bold text-emerald-700 dark:text-emerald-400 bg-emerald-100 hover:bg-emerald-200 dark:bg-emerald-900/30 dark:hover:bg-emerald-900/50 px-3 py-1.5 rounded transition-colors whitespace-nowrap"
              >
                {copied ? 'Copied!' : 'Copy JSON'}
              </button>
            </div>
          </div>

          <div className="bg-gray-900 rounded-xl p-4 overflow-x-auto border border-gray-800 shadow-inner custom-scrollbar">
            <pre className="text-emerald-400 font-mono text-sm leading-relaxed">
              <code>
                {paramTab === 'initial' && initialParamsString}
                {paramTab === 'bounds' && boundsString}
                {paramTab === 'final' && jsonString}
              </code>
            </pre>
          </div>
        </div>

        {/* Dynamic CSV Visualizations */}
        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-6 shadow-sm">
          <div className="flex items-center gap-3 mb-6">
            <div className="bg-emerald-100 dark:bg-emerald-900/30 p-2 rounded-lg">
              <Activity className="w-6 h-6 text-emerald-600 dark:text-emerald-400" />
            </div>
            <h2 className="text-xl font-bold">Training Parameter Evolution</h2>
          </div>
          
          <p className="text-gray-600 dark:text-slate-400 text-sm leading-relaxed mb-6 text-justify">
             Analytical plots populated natively from the training logs. Graphs are truncated at the convergent epoch (81). Notice how the internal thermal mass constraints 
             (<InlineMath math="C_{sink}" />) quickly stabilize, allowing the active cooling parameters (<InlineMath math="h_{active}" />) 
             to fine-tune.
          </p>

          {!csvData ? (
            <div className="flex justify-center items-center h-48 bg-gray-50 dark:bg-slate-950 rounded-xl border border-gray-200 dark:border-slate-800">
               <span className="text-gray-500 animate-pulse font-bold text-sm">Fetching and Parsing training_log.csv...</span>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              
              <GraphCard title="Loss Curve (RMSE)" data={buildDataset('Train RMSE', csvData.train_rmse, 'Val RMSE', csvData.val_rmse)} options={getChartOptions()} onExpand={setExpandedChart} />
              <GraphCard title="Loss Curve (MAE)" data={buildDataset('Train MAE', csvData.train_mae, 'Val MAE', csvData.val_mae)} options={getChartOptions()} onExpand={setExpandedChart} />
              <GraphCard title="Optimizer Learning Rate" data={buildDataset('Learning Rate', csvData.lr)} options={getChartOptions()} onExpand={setExpandedChart} />

              <GraphCard title="Die Thermal Mass (C_die_0 vs C_die_1)" data={buildDataset('C_die_0', csvData.C_die_0, 'C_die_1', csvData.C_die_1)} options={getChartOptions()} onExpand={setExpandedChart} />
              <GraphCard title="Heatsink Thermal Mass (C_sink_0 vs C_sink_1)" data={buildDataset('C_sink_0', csvData.C_sink_0, 'C_sink_1', csvData.C_sink_1)} options={getChartOptions()} onExpand={setExpandedChart} />
              <GraphCard title="Thermal Paste Resistance (R_paste_0 vs R_paste_1)" data={buildDataset('R_paste_0', csvData.R_paste_0, 'R_paste_1', csvData.R_paste_1)} options={getChartOptions()} onExpand={setExpandedChart} />
              
              <GraphCard title="Thermal Crosstalk (k01 vs k10)" data={buildDataset('k01', csvData.k01, 'k10', csvData.k10)} options={getChartOptions()} onExpand={setExpandedChart} />
              <GraphCard title="Ambient Heat Transfer (q0 vs q1)" data={buildDataset('q0', csvData.q0, 'q1', csvData.q1)} options={getChartOptions()} onExpand={setExpandedChart} />
              <GraphCard title="Base Convection (h_base_0 vs h_base_1)" data={buildDataset('h_base_0', csvData.h_base_0, 'h_base_1', csvData.h_base_1)} options={getChartOptions()} onExpand={setExpandedChart} />
              
              <GraphCard title="Active Fan Curve (h_active_0 vs h_active_1)" data={buildDataset('h_active_0', csvData.h_active_0, 'h_active_1', csvData.h_active_1)} options={getChartOptions()} onExpand={setExpandedChart} />
              <GraphCard title="Fan Activation Temp (T_thresh_0 vs T_thresh_1)" data={buildDataset('T_thresh_0', csvData.T_thresh_0, 'T_thresh_1', csvData.T_thresh_1)} options={getChartOptions()} onExpand={setExpandedChart} />

            </div>
          )}
        </div>

      </div>
    </div>
  );
}

function MetricCard({ title, value, sub }: { title: string, value: string, sub: string }) {
  return (
    <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-xl p-5 shadow-sm text-center flex flex-col justify-center">
      <h3 className="text-xs font-bold text-gray-500 dark:text-slate-400 uppercase tracking-wider mb-2">{title}</h3>
      <p className="text-2xl font-extrabold text-gray-900 dark:text-white mb-1">{value}</p>
      <p className="text-[10px] text-gray-400 dark:text-slate-500 font-medium">{sub}</p>
    </div>
  );
}

function MetricRow({ label, value, colorClass, tooltip }: { label: string, value: string, colorClass: string, tooltip: string }) {
  return (
    <li className="flex justify-between border-b border-gray-200 dark:border-slate-700 pb-2 relative group cursor-help">
       <span className="text-gray-600 dark:text-slate-300 flex items-center gap-1.5">
          {label} <Info className="w-3.5 h-3.5 text-gray-400 group-hover:text-emerald-500 transition-colors" />
       </span>
       <span className={`font-bold ${colorClass}`}>{value}</span>
       
       <div className="absolute left-0 bottom-full mb-2 w-64 p-3 bg-gray-900 text-gray-100 text-[11px] leading-relaxed rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-10 border border-gray-700">
         {tooltip}
       </div>
    </li>
  );
}

function GraphCard({ title, data, options, onExpand }: { title: string, data: any, options: any, onExpand: (data: any) => void }) {
  return (
    <div className="bg-gray-50 dark:bg-slate-950 border border-gray-200 dark:border-slate-800 rounded-xl overflow-hidden shadow-inner flex flex-col group relative">
      <div className="p-3 border-b border-gray-200 dark:border-slate-800 bg-white dark:bg-slate-900 flex justify-between items-center shrink-0">
        <span className="text-xs font-bold text-gray-700 dark:text-slate-300 truncate pr-2" title={title}>{title}</span>
        <button 
          onClick={() => onExpand({ title, data })}
          className="text-gray-400 hover:text-emerald-500 bg-gray-100 hover:bg-emerald-50 dark:bg-slate-800 dark:hover:bg-slate-700 p-1.5 rounded flex items-center gap-1 transition-colors z-10"
          title="Expand Graph"
        >
          <Maximize2 className="w-3.5 h-3.5" />
          <span className="text-[10px] font-bold uppercase tracking-wider hidden sm:inline">Expand</span>
        </button>
      </div>
      <div className="w-full h-52 p-4 bg-white dark:bg-slate-900">
        <Line data={data} options={options} />
      </div>
    </div>
  );
}