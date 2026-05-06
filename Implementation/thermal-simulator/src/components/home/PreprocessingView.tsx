"use client";

import React, { useEffect } from 'react';
import { ArrowLeft, Sun, Moon, Database, Filter, Layers, Zap, Info } from 'lucide-react';
import { FaDatabase } from "react-icons/fa";

interface PreprocessingViewProps {
  theme: 'dark' | 'light';
  onToggleTheme: () => void;
  onGoHome: () => void;
}

export default function PreprocessingView({ theme, onToggleTheme, onGoHome }: PreprocessingViewProps) {
  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);

  const splittingData = [
    { name: 'Short_Low', total: 938, train: 750, val: 93, test: 95 },
    { name: 'Short_Medium', total: 1028, train: 822, val: 102, test: 104 },
    { name: 'Short_High', total: 244, train: 195, val: 24, test: 25 },
    { name: 'Medium_Low', total: 401, train: 320, val: 40, test: 41 },
    { name: 'Medium_Medium', total: 715, train: 572, val: 71, test: 72 },
    { name: 'Medium_High', total: 1093, train: 874, val: 109, test: 110 },
    { name: 'Long_Low', total: 871, train: 696, val: 87, test: 88 },
    { name: 'Long_Medium', total: 466, train: 372, val: 46, test: 48 },
    { name: 'Long_High', total: 873, train: 698, val: 87, test: 88 },
  ];

  return (
    <div className="flex-1 overflow-y-auto custom-scrollbar p-4 sm:p-6 md:p-8">
      <div className="max-w-7xl mx-auto w-full space-y-6 sm:space-y-8 pb-12">
        
        {/* Header */}
        <div className="flex flex-row justify-between items-start sm:items-center border-b border-gray-200 dark:border-slate-800 pb-6 gap-4">
          <div className="flex items-start sm:items-center gap-3 sm:gap-4">
            <button 
              onClick={onGoHome} 
              className="p-2 mt-1 sm:mt-0 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 hover:bg-gray-100 dark:hover:bg-slate-700 rounded-lg text-gray-600 dark:text-slate-400 transition-colors shrink-0"
              title="Back to Home"
            >
              <ArrowLeft className="w-5 h-5" />
            </button>
            <div>
              <h1 className="text-2xl sm:text-3xl font-bold tracking-tight">Data Preprocessing & Tensor Engineering</h1>
              <p className="text-sm sm:text-base text-gray-500 dark:text-slate-400 mt-1">Pipeline from raw telemetry CSVs to PyTorch-ready ODE tensors.</p>
            </div>
          </div>
          
          <button 
            onClick={onToggleTheme} 
            className="p-2 mt-1 sm:mt-0 bg-gray-200 dark:bg-slate-800 rounded-lg text-gray-700 dark:text-slate-300 hover:bg-gray-300 dark:hover:bg-slate-700 transition-colors shrink-0"
          >
            {theme === 'dark' ? <Sun className="w-4 h-4"/> : <Moon className="w-4 h-4"/>}
          </button>
        </div>

        {/* Intro Banner */}
        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 text-indigo-900 dark:text-indigo-100 text-justify shadow-sm">
          <p className="text-base sm:text-lg leading-relaxed">
            Before the ODE can be trained, the raw MIT Supercloud Dataset must be heavily processed. 
            This multi-stage pipeline converts the raw CSV telemetry into strict typed Parquet files, filters out inconsistent hardware logs, 
            extracts baseline thermodynamic priors, and stacks the time-series into chunked PyTorch tensors to prevent RAM exhaustion during gradient descent.
          </p>
        </div>

        {/* Dataset Scope Note */}
        <div className="flex flex-col sm:flex-row items-start gap-3 bg-indigo-50 dark:bg-indigo-900/20 border border-indigo-200 dark:border-indigo-800/50 rounded-xl p-4 sm:p-5 shadow-sm">
          <Database className="w-5 h-5 text-indigo-600 dark:text-indigo-400 shrink-0 sm:mt-0.5" />
          <p className="text-sm text-indigo-900 dark:text-indigo-200 leading-relaxed text-justify break-words w-full">
            <strong>Dataset Scope & Initialization:</strong> To optimize development and computational overhead, the pipeline was initialized using exactly 20% of the available GPU data subfolders. Specifically, the telemetry within subfolders <code className="bg-indigo-100 dark:bg-indigo-800/50 px-1.5 py-0.5 rounded text-xs font-mono break-words">0000_parquet</code> through <code className="bg-indigo-100 dark:bg-indigo-800/50 px-1.5 py-0.5 rounded text-xs font-mono break-words">0019_parquet</code> (out of 100 total subfolders, 0000 to 0099) were utilized. This provided a massive, representative multi-gigabyte sample before Phase 1 began.
          </p>
        </div>

        {/* Stage 1: Filtering Funnel */}
        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm">
          <div className="flex items-center gap-3 mb-6">
            <div className="bg-indigo-100 dark:bg-indigo-900/30 p-2 rounded-lg shrink-0">
              <Filter className="w-5 h-5 sm:w-6 sm:h-6 text-indigo-600 dark:text-indigo-400" />
            </div>
            <h2 className="text-lg sm:text-xl font-bold leading-tight">Phase 1: Dual-GPU Validation & Cleaning</h2>
          </div>
          
          <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-6 relative">
            <div className="hidden md:block absolute top-1/2 left-1/6 right-1/6 h-0.5 bg-gradient-to-r from-indigo-200 via-indigo-300 to-indigo-400 dark:from-slate-700 dark:via-indigo-900/50 dark:to-indigo-800/50 -z-10 transform -translate-y-1/2"></div>
            
            <StepCard 
              step="1"
              title="Type Safety & Parquet" 
              value="22,471" 
              subtitle="Total Files Processed"
              desc="Raw CSVs were converted to PyArrow Parquet format for strict schema enforcement, massive compression, and rapid I/O operations."
            />
            <StepCard 
              step="2"
              title="Dual-GPU Consistency" 
              value="6,681" 
              subtitle="Valid Files Retained"
              desc="Filtered out nodes that lacked perfectly overlapping GPU 0 and GPU 1 telemetry traces (max 5% row count diff, 5s overlap tolerance)."
            />
            <StepCard 
              step="3"
              title="Thermodynamic Activity" 
              value="6,629" 
              subtitle="Final Cleaned Files"
              desc="Removed files lacking dynamic thermal excitation (required at least a 1.0°C or 5.0W delta). Timestamps normalized to t=0."
            />
          </div>
        </div>

        {/* Stage 2: 2D Stratified Split */}
        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm flex flex-col w-full overflow-hidden">
          <div className="flex items-center gap-3 mb-4">
            <div className="bg-indigo-100 dark:bg-indigo-900/30 p-2 rounded-lg shrink-0">
              <FaDatabase className="w-4 h-4 sm:w-5 sm:h-5 text-indigo-600 dark:text-indigo-400" />
            </div>
            <h2 className="text-lg sm:text-xl font-bold leading-tight">Phase 2: 2D Stratified Split</h2>
          </div>
          <p className="text-gray-600 dark:text-slate-400 text-sm leading-relaxed mb-6 sm:mb-8 text-justify">
            To ensure the model generalizes across diverse datacenter workloads and avoids bias toward common short/idle jobs, the 6,629 cleaned jobs were binned into a 3x3 matrix based on two dimensions: <strong>Job Length</strong> (Short, Medium, Long) and <strong>Workload Density</strong> (Low, Medium, High). Random sampling within these strata ensures edge cases are perfectly represented.
          </p>
          
          <div className="grid grid-cols-1 xl:grid-cols-12 gap-6 sm:gap-8 items-start w-full">
            {/* Matrix 1: 2D Stratification */}
            <div className="xl:col-span-5 bg-gray-50 dark:bg-slate-950 rounded-xl p-4 sm:p-5 border border-gray-200 dark:border-slate-800 shadow-inner w-full">
              <h3 className="text-[10px] sm:text-xs font-bold text-gray-500 dark:text-slate-400 uppercase tracking-wider mb-4">The 2D Stratification Matrix</h3>
              <div className="overflow-x-auto w-full custom-scrollbar">
                <table className="w-full text-sm text-left whitespace-nowrap min-w-[350px]">
                  <thead>
                    <tr className="border-b border-gray-200 dark:border-slate-700 text-gray-500 dark:text-slate-400">
                      <th className="py-2 sm:py-3 px-2 sm:px-3 font-semibold">len_bin \ den_bin</th>
                      <th className="py-2 sm:py-3 px-2 sm:px-3 font-semibold text-right">Low</th>
                      <th className="py-2 sm:py-3 px-2 sm:px-3 font-semibold text-right">Medium</th>
                      <th className="py-2 sm:py-3 px-2 sm:px-3 font-semibold text-right">High</th>
                      <th className="py-2 sm:py-3 px-2 sm:px-3 font-bold text-right">Total</th>
                    </tr>
                  </thead>
                  <tbody className="font-mono">
                    <tr className="border-b border-gray-100 dark:border-slate-800/50">
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-gray-700 dark:text-slate-300">Short</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right">938</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right">1028</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right">244</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right font-semibold">2210</td>
                    </tr>
                    <tr className="border-b border-gray-100 dark:border-slate-800/50">
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-gray-700 dark:text-slate-300">Medium</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right">401</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right">715</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right">1093</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right font-semibold">2209</td>
                    </tr>
                    <tr className="border-b border-gray-200 dark:border-slate-700">
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-gray-700 dark:text-slate-300">Long</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right">871</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right">466</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right">873</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right font-semibold">2210</td>
                    </tr>
                    <tr className="bg-gray-100/50 dark:bg-slate-800/30 font-bold">
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-gray-900 dark:text-white rounded-bl-lg">Total</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right">2210</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right">2209</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right">2210</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right rounded-br-lg">6629</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* Matrix 2: 80/10/10 Logic */}
            <div className="xl:col-span-7 bg-gray-50 dark:bg-slate-950 rounded-xl p-4 sm:p-5 border border-gray-200 dark:border-slate-800 shadow-inner w-full">
              <h3 className="text-[10px] sm:text-xs font-bold text-gray-500 dark:text-slate-400 uppercase tracking-wider mb-4">Detailed 80/10/10 Splitting Logic</h3>
              <div className="overflow-x-auto w-full custom-scrollbar">
                <table className="w-full text-sm text-left whitespace-nowrap min-w-[400px]">
                  <thead>
                    <tr className="border-b border-gray-200 dark:border-slate-700 text-gray-500 dark:text-slate-400">
                      <th className="py-2 px-2 sm:px-3 font-semibold">STRATA NAME</th>
                      <th className="py-2 px-2 sm:px-3 font-semibold text-right">TOTAL</th>
                      <th className="py-2 px-2 sm:px-3 font-semibold text-right">TRAIN (80%)</th>
                      <th className="py-2 px-2 sm:px-3 font-semibold text-right">VAL (10%)</th>
                      <th className="py-2 px-2 sm:px-3 font-semibold text-right">TEST (10%)</th>
                    </tr>
                  </thead>
                  <tbody className="font-mono">
                    {splittingData.map((row, i) => (
                      <tr key={i} className="border-b border-gray-100 dark:border-slate-800/50 hover:bg-gray-100/50 dark:hover:bg-slate-800/30 transition-colors">
                        <td className="py-1.5 sm:py-2 px-2 sm:px-3 text-gray-700 dark:text-slate-300">{row.name}</td>
                        <td className="py-1.5 sm:py-2 px-2 sm:px-3 text-right">{row.total}</td>
                        <td className="py-1.5 sm:py-2 px-2 sm:px-3 text-right text-indigo-600 dark:text-indigo-400">{row.train}</td>
                        <td className="py-1.5 sm:py-2 px-2 sm:px-3 text-right text-teal-600 dark:text-teal-400">{row.val}</td>
                        <td className="py-1.5 sm:py-2 px-2 sm:px-3 text-right text-rose-600 dark:text-rose-400">{row.test}</td>
                      </tr>
                    ))}
                    <tr className="bg-gray-100/50 dark:bg-slate-800/30 font-bold border-t border-gray-200 dark:border-slate-700">
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-gray-900 dark:text-white rounded-bl-lg">GRAND TOTAL</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right">6629</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right text-indigo-600 dark:text-indigo-400">5299</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right text-teal-600 dark:text-teal-400">659</td>
                      <td className="py-2 sm:py-3 px-2 sm:px-3 text-right text-rose-600 dark:text-rose-400 rounded-br-lg">671</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>

        {/* Stage 3: Tensor Stacking */}
        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm flex flex-col w-full">
          <div className="flex items-center gap-3 mb-4">
            <div className="bg-indigo-100 dark:bg-indigo-900/30 p-2 rounded-lg shrink-0">
              <Layers className="w-5 h-5 sm:w-6 sm:h-6 text-indigo-600 dark:text-indigo-400" />
            </div>
            <h2 className="text-lg sm:text-xl font-bold leading-tight">Phase 3: ODE Tensor Chunking</h2>
          </div>
          <p className="text-gray-600 dark:text-slate-400 text-sm leading-relaxed mb-6 text-justify">
            Jobs were segmented into exactly 5,000-row arrays (padding shorter sequences). To prevent RAM exhaustion during PyTorch data loading, training segments were stacked into massive <code className="bg-gray-100 dark:bg-slate-800 px-1 rounded">.pt</code> chunk files, while test files were mapped 1-to-1 to preserve true physical trajectories.
          </p>
          
          <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-4 sm:gap-6 w-full">
            <div className="bg-gray-50 dark:bg-slate-950 rounded-xl p-5 border border-gray-200 dark:border-slate-800 shadow-sm flex flex-col items-center justify-center text-center hover:border-indigo-400 dark:hover:border-indigo-500/50 transition-colors">
              <span className="text-gray-500 dark:text-slate-400 text-[10px] sm:text-xs font-bold uppercase tracking-wider mb-2">Train Segments</span>
              <span className="text-2xl sm:text-3xl font-extrabold text-indigo-600 dark:text-indigo-400 font-mono">343,503</span>
              <span className="text-gray-400 text-[10px] sm:text-xs mt-2 font-medium">Batched in .pt chunks</span>
            </div>
            
            <div className="bg-gray-50 dark:bg-slate-950 rounded-xl p-5 border border-gray-200 dark:border-slate-800 shadow-sm flex flex-col items-center justify-center text-center hover:border-teal-400 dark:hover:border-teal-500/50 transition-colors">
              <span className="text-gray-500 dark:text-slate-400 text-[10px] sm:text-xs font-bold uppercase tracking-wider mb-2">Val Segments</span>
              <span className="text-2xl sm:text-3xl font-extrabold text-teal-600 dark:text-teal-400 font-mono">39,533</span>
              <span className="text-gray-400 text-[10px] sm:text-xs mt-2 font-medium">Combined 1 Bundle</span>
            </div>

            <div className="bg-gray-50 dark:bg-slate-950 rounded-xl p-5 border border-gray-200 dark:border-slate-800 shadow-sm flex flex-col items-center justify-center text-center hover:border-rose-400 dark:hover:border-rose-500/50 transition-colors sm:col-span-2 md:col-span-1">
              <span className="text-gray-500 dark:text-slate-400 text-[10px] sm:text-xs font-bold uppercase tracking-wider mb-2">Test Segments</span>
              <span className="text-2xl sm:text-3xl font-extrabold text-rose-600 dark:text-rose-400 font-mono">44,691</span>
              <span className="text-gray-400 text-[10px] sm:text-xs mt-2 font-medium">Mapped 1:1 Files</span>
            </div>
          </div>
        </div>

        {/* Stage 4: Physics Prior Extraction */}
        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm w-full">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between mb-6 border-b border-gray-200 dark:border-slate-700 pb-4 gap-3">
            <div className="flex items-center gap-3">
              <div className="bg-indigo-100 dark:bg-indigo-900/30 p-2 rounded-lg shrink-0">
                <Zap className="w-5 h-5 sm:w-6 sm:h-6 text-indigo-600 dark:text-indigo-400" />
              </div>
              <h2 className="text-lg sm:text-xl font-bold leading-tight">Phase 4: Thermodynamic Prior Calculations</h2>
            </div>
            <span className="bg-indigo-100 dark:bg-indigo-900/30 text-indigo-800 dark:text-indigo-400 text-[10px] sm:text-xs font-bold px-3 py-1.5 rounded-full border border-indigo-200 dark:border-indigo-800/50 self-start sm:self-auto text-center">
              Empirical Physics Initialization
            </span>
          </div>

          <p className="text-gray-600 dark:text-slate-400 text-sm leading-relaxed text-justify mb-8">
            Gradient descent performs best when initialized near the true global minima. Specialized scripts were written to scan the dataset for isolated <strong>asymmetric heating events</strong> and to fit <strong>exponential step-response curves</strong> to the data. This allows the calculation of the real-world thermal resistances and crosstalk boundaries of the hardware before ODE training began.
          </p>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-8 sm:gap-10">
            {/* GPU 0 Column */}
            <div className="space-y-4">
              <h3 className="text-sm font-bold text-gray-500 dark:text-slate-400 uppercase tracking-wider border-b border-gray-100 dark:border-slate-800/50 pb-2">GPU 0 (Downstream) Priors</h3>
              <ul className="space-y-3 font-mono text-xs sm:text-sm">
                <PriorRow 
                  label="Resistance Prior (R_th_0)" 
                  value="0.2053 °C/W" 
                  tooltip="Thermal resistance (°C/W). Extracted by fitting an exponential heating curve to isolated workload spikes. Calculated as Maximum Temperature Amplitude divided by the Power Delta."
                />
                <PriorRow 
                  label="Convective Cooling (h_0)" 
                  value="4.8713 W/°C" 
                  tooltip="Baseline convective cooling coefficient (W/°C). Calculated strictly as the inverse of thermal resistance (1 / R_th)."
                />
                <PriorRow 
                  label="Crosstalk Flow (GPU 1 → 0)" 
                  value="k_01 : 0.01602 °C/W" 
                  tooltip="Parasitic heat transfer (°C/W). Calculated by isolating events where GPU 1 was fully stressed (>150W) while GPU 0 idled (<50W), tracking GPU 0's temperature rise."
                />
                <PriorRow 
                  label="Dimensionless Crosstalk" 
                  value="κ_01 : 0.078038" 
                  tooltip="Unitless thermal crosstalk fraction. Calculated by multiplying the crosstalk flow coefficient (k) by the downstream convective cooling coefficient (h)."
                />
              </ul>
            </div>
            
            {/* GPU 1 Column */}
            <div className="space-y-4">
              <h3 className="text-sm font-bold text-gray-500 dark:text-slate-400 uppercase tracking-wider border-b border-gray-100 dark:border-slate-800/50 pb-2">GPU 1 (Upstream) Priors</h3>
              <ul className="space-y-3 font-mono text-xs sm:text-sm">
                <PriorRow 
                  label="Resistance Prior (R_th_1)" 
                  value="0.1856 °C/W" 
                  tooltip="Thermal resistance (°C/W). Extracted by fitting an exponential heating curve to isolated workload spikes. Calculated as Maximum Temperature Amplitude divided by the Power Delta."
                />
                <PriorRow 
                  label="Convective Cooling (h_1)" 
                  value="5.3871 W/°C" 
                  tooltip="Baseline convective cooling coefficient (W/°C). Calculated strictly as the inverse of thermal resistance (1 / R_th)."
                />
                <PriorRow 
                  label="Crosstalk Flow (GPU 0 → 1)" 
                  value="k_10 : 0.00522 °C/W" 
                  tooltip="Parasitic heat transfer (°C/W). Calculated by isolating events where GPU 0 was fully stressed (>150W) while GPU 1 idled (<50W), tracking GPU 1's temperature rise."
                />
                <PriorRow 
                  label="Dimensionless Crosstalk" 
                  value="κ_10 : 0.028120" 
                  tooltip="Unitless thermal crosstalk fraction. Calculated by multiplying the crosstalk flow coefficient (k) by the downstream convective cooling coefficient (h)."
                />
              </ul>
            </div>
          </div>
        </div>

      </div>
    </div>
  );
}

function StepCard({ step, title, value, subtitle, desc }: { step: string, title: string, value: string, subtitle: string, desc: string }) {
  return (
    <div className="bg-gray-50 dark:bg-slate-950 border border-gray-200 dark:border-slate-800 rounded-xl p-4 sm:p-5 shadow-sm relative z-10 flex flex-col h-full hover:border-indigo-400 dark:hover:border-indigo-500/50 transition-colors">
      <div className="absolute -top-3 -left-3 w-7 h-7 sm:w-8 sm:h-8 bg-indigo-500 text-white rounded-full flex items-center justify-center font-bold text-xs sm:text-sm shadow-md border-2 border-white dark:border-slate-900">
        {step}
      </div>
      <h3 className="font-bold text-gray-900 dark:text-white text-sm sm:text-base mt-2 mb-4 text-center leading-tight">{title}</h3>
      <div className="text-center mb-4">
        <p className="text-2xl sm:text-3xl font-extrabold text-indigo-600 dark:text-indigo-400">{value}</p>
        <p className="text-[10px] sm:text-xs font-bold text-gray-400 uppercase tracking-wider mt-1">{subtitle}</p>
      </div>
      <p className="text-[11px] sm:text-xs text-gray-500 dark:text-slate-400 leading-relaxed text-justify mt-auto">{desc}</p>
    </div>
  );
}

function PriorRow({ label, value, tooltip }: { label: string, value: string, tooltip: string }) {
  return (
    <li className="flex justify-between border-b border-gray-200 dark:border-slate-700 pb-2 relative group cursor-help flex-col xl:flex-row gap-1 z-10 hover:z-20">
       <span className="text-gray-600 dark:text-slate-300 flex items-center gap-1.5">
          {label} <Info className="w-3.5 h-3.5 text-gray-400 group-hover:text-indigo-500 transition-colors shrink-0" />
       </span>
       <span className="font-bold text-indigo-600 dark:text-indigo-500 xl:text-right">{value}</span>
       
       <div className="absolute -left-2 sm:left-0 bottom-full mb-2 w-[85vw] max-w-[300px] sm:w-64 p-3 bg-gray-900 text-gray-100 text-[10px] sm:text-[11px] leading-relaxed rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none border border-gray-700 z-30">
         {tooltip}
       </div>
    </li>
  );
}