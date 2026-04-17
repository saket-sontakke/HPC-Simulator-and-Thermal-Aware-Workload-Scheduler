"use client";

import React, { useEffect, useState } from 'react';
import { ArrowLeft, Database, Download, Server, Cpu, Activity, Sun, Moon, HardDrive, FileText, Info } from 'lucide-react';

interface DatasetViewProps {
  theme: 'dark' | 'light';
  onToggleTheme: () => void;
  onGoHome: () => void;
}

const CreativeCommonsIcon = ({ className }: { className?: string }) => (
  <svg 
    xmlns="http://www.w3.org/2000/svg" 
    viewBox="0 0 24 24" 
    fill="none" 
    stroke="currentColor" 
    strokeWidth="2" 
    strokeLinecap="round" 
    strokeLinejoin="round" 
    className={className}
  >
    <circle cx="12" cy="12" r="10"></circle>
    <path d="M10.5 9.5a2.5 2.5 0 0 0-3.5 0v5a2.5 2.5 0 0 0 3.5 0"></path>
    <path d="M17.5 9.5a2.5 2.5 0 0 0-3.5 0v5a2.5 2.5 0 0 0 3.5 0"></path>
  </svg>
);

export default function DatasetView({ theme, onToggleTheme, onGoHome }: DatasetViewProps) {
  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);

  const [copied, setCopied] = useState(false);
  const awsCommand = "aws s3 cp s3://mit-supercloud-dataset/datacenter-challenge datacenter-challenge --recursive --no-sign-request";

  const copyToClipboard = () => {
    navigator.clipboard.writeText(awsCommand);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

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
              <h1 className="text-2xl sm:text-3xl font-bold tracking-tight">MIT Supercloud Dataset</h1>
              <p className="text-sm sm:text-base text-gray-500 dark:text-slate-400 mt-1">Ground-truth telemetry utilized for digital twin calibration.</p>
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
        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 text-purple-900 dark:text-purple-100 text-justify shadow-sm">
          <p className="text-base sm:text-lg leading-relaxed">
            This simulator is calibrated against the <strong>MIT Supercloud Dataset</strong>, a massive collection of monitoring data from the MIT Supercloud TX-Gaia cluster. 
            The dataset includes traces from over 460,000 jobs, including 98,177 jobs that requested GPUs for AI/ML training and inference. 
            It contains anonymized scheduler logs, time-series data from CPUs and GPUs, and environmental monitoring data.
          </p>
        </div>

        {/* Data Architecture & Organization */}
        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm">
          <div className="flex items-center gap-3 mb-6">
            <div className="bg-purple-100 dark:bg-purple-900/30 p-2 rounded-lg shrink-0">
              <Database className="w-5 h-5 sm:w-6 sm:h-6 text-purple-600 dark:text-purple-400" />
            </div>
            <h2 className="text-lg sm:text-xl font-bold leading-tight">Dataset Organization (~2 TB Scale)</h2>
          </div>
          
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
            <DataCard 
              title="GPU Utilization" 
              icon={<Server className="w-5 h-5 text-purple-500" />} 
              desc="Time-series data collected via nvidia-smi on all GPUs assigned to a job, sampled at 100 ms intervals."
            />
            <DataCard 
              title="CPU Utilization" 
              icon={<Cpu className="w-5 h-5 text-purple-500" />} 
              desc="Time-series profiling collected via Slurm plugin. Includes CPU usage, memory usage, and I/O at 10s intervals."
            />
            <DataCard 
              title="Compute Nodes" 
              icon={<HardDrive className="w-5 h-5 text-purple-500" />} 
              desc="System load, active users, memory footprint, and Lustre RPC calls sampled every 5 minutes."
            />
            <DataCard 
              title="Slurm Scheduler" 
              icon={<FileText className="w-5 h-5 text-purple-500" />} 
              desc="Anonymized accounting logs tracking requested resources (TRES), job durations, and node allocations."
            />
          </div>
        </div>

        {/* GPU Sample Table */}
        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm overflow-hidden flex flex-col">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between mb-4 gap-3">
            <div className="flex items-center gap-3">
              <div className="bg-purple-100 dark:bg-purple-900/30 p-2 rounded-lg shrink-0">
                <Activity className="w-5 h-5 sm:w-6 sm:h-6 text-purple-600 dark:text-purple-400" />
              </div>
              <h2 className="text-lg sm:text-xl font-bold leading-tight">GPU Time-Series Telemetry Sample</h2>
            </div>
            <span className="bg-purple-100 dark:bg-purple-900/30 text-purple-800 dark:text-purple-400 text-xs font-bold px-3 py-1.5 rounded-full border border-purple-200 dark:border-purple-800/50 self-start sm:self-auto text-center">
              100ms Granularity
            </span>
          </div>

          <div className="overflow-x-auto rounded-xl border border-gray-200 dark:border-slate-800 w-full">
            <table className="w-full text-left border-collapse min-w-[800px]">
              <thead>
                <tr className="bg-gray-50 dark:bg-slate-950/50 text-gray-500 dark:text-slate-400 text-[11px] uppercase tracking-wider font-bold">
                  <th className="p-3 border-b border-gray-200 dark:border-slate-800">timestamp</th>
                  <th className="p-3 border-b border-gray-200 dark:border-slate-800">gpu_index</th>
                  <th className="p-3 border-b border-gray-200 dark:border-slate-800">utilization_gpu_pct</th>
                  <th className="p-3 border-b border-gray-200 dark:border-slate-800">utilization_memory_pct</th>
                  <th className="p-3 border-b border-gray-200 dark:border-slate-800">memory_free_MiB</th>
                  <th className="p-3 border-b border-gray-200 dark:border-slate-800">memory_used_MiB</th>
                  <th className="p-3 border-b border-gray-200 dark:border-slate-800">temperature_gpu</th>
                  <th className="p-3 border-b border-gray-200 dark:border-slate-800">temperature_memory</th>
                  <th className="p-3 border-b border-gray-200 dark:border-slate-800">power_draw_W</th>
                  <th className="p-3 border-b border-gray-200 dark:border-slate-800">pcie_link_width</th>
                </tr>
              </thead>
              <tbody className="text-sm font-mono text-gray-700 dark:text-slate-300">
                <tr className="hover:bg-gray-50 dark:hover:bg-slate-800/50 transition-colors">
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800">1627487662.237</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-center">1</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-center">0</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-center">0</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-right">21717</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-right">10793</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-center">54</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-center">52</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-right">48.42</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-center">16</td>
                </tr>
                <tr className="hover:bg-gray-50 dark:hover:bg-slate-800/50 transition-colors">
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800">1627487662.340</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-center">0</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-center">14</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-center">6</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-right">17299</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-right">15211</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-center">61</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-center">63</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-right">211.78</td>
                  <td className="p-3 border-b border-gray-100 dark:border-slate-800 text-center">16</td>
                </tr>
                <tr className="hover:bg-gray-50 dark:hover:bg-slate-800/50 transition-colors">
                  <td className="p-3">1627487662.455</td>
                  <td className="p-3 text-center">1</td>
                  <td className="p-3 text-center">60</td>
                  <td className="p-3 text-center">28</td>
                  <td className="p-3 text-right">21717</td>
                  <td className="p-3 text-right">10793</td>
                  <td className="p-3 text-center">59</td>
                  <td className="p-3 text-center">57</td>
                  <td className="p-3 text-right">173.24</td>
                  <td className="p-3 text-center">16</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* Data Source & Attribution */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm flex flex-col h-full">
            <div className="flex items-center gap-3 mb-4">
              <div className="bg-purple-100 dark:bg-purple-900/30 p-2 rounded-lg shrink-0">
                <CreativeCommonsIcon className="w-5 h-5 sm:w-6 sm:h-6 text-purple-600 dark:text-purple-400" />
              </div>
              <h2 className="text-lg sm:text-xl font-bold leading-tight">Data Source Attribution</h2>
            </div>
            <p className="text-gray-600 dark:text-slate-400 text-sm leading-relaxed text-justify mb-4">
              This project is built using telemetry from the MIT Supercloud Dataset. We gratefully acknowledge the researchers and engineers at MIT for making this data publicly available for academic and analytical use.
            </p>
            <div className="bg-gray-50 dark:bg-slate-950 rounded-xl p-4 border border-gray-200 dark:border-slate-800 shadow-inner mb-4">
              <p className="text-gray-700 dark:text-gray-300 text-sm italic mb-3">
                Samsi, Siddharth, Weiss, Matthew, Bestor, David, et al. "The MIT Supercloud Dataset." 2021 IEEE High Performance Extreme Computing Conference (HPEC). IEEE, 2021.
              </p>
              <p className="text-gray-600 dark:text-gray-400 text-xs">
                Full text of the paper is available at{' '}
                <a href="https://ieeexplore.ieee.org/abstract/document/9622850" target="_blank" rel="noreferrer" className="text-purple-600 dark:text-purple-400 hover:underline">
                  IEEE Xplore
                </a>{' '}
                or{' '}
                <a href="https://arxiv.org/abs/2108.02037" target="_blank" rel="noreferrer" className="text-purple-600 dark:text-purple-400 hover:underline">
                  arXiv
                </a>.
              </p>
            </div>
            <div className="mt-auto flex flex-col gap-2">
               <a href="https://dcc.mit.edu" target="_blank" rel="noreferrer" className="text-sm font-semibold text-purple-600 dark:text-purple-500 hover:underline break-words">
                 Visit the official MIT Datacenter Challenge Website →
               </a>
               <a href="http://creativecommons.org/licenses/by-nc-nd/4.0/" target="_blank" rel="noreferrer" className="text-xs text-gray-500 hover:underline break-words">
                 Data used under CC BY-NC-ND 4.0 License
               </a>
            </div>
          </div>

          <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm flex flex-col h-full">
            <div className="flex items-center gap-3 mb-4">
              <div className="bg-purple-100 dark:bg-purple-900/30 p-2 rounded-lg shrink-0">
                <Download className="w-5 h-5 sm:w-6 sm:h-6 text-purple-600 dark:text-purple-400" />
              </div>
              <h2 className="text-lg sm:text-xl font-bold leading-tight">MIT Supercloud Data Access</h2>
            </div>
            
            <p className="text-gray-600 dark:text-slate-400 text-sm mb-3 leading-relaxed text-justify">
              The dataset is available for download from the Amazon Open Data Registry via the following bucket (released January 2022). It is highly recommended to use the AWS CLI tools for access.
            </p>

            <a href="https://dcc.mit.edu/data/" target="_blank" rel="noreferrer" className="text-sm font-semibold text-purple-600 dark:text-purple-500 hover:underline mb-4 block break-words">
              Official MIT Data Page & Instructions →
            </a>

            <div className="flex items-start gap-2 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800/50 rounded-lg p-3 mb-5 mt-auto">
              <Info className="w-4 h-4 text-amber-600 dark:text-amber-500 shrink-0 mt-0.5" />
              <p className="text-xs text-amber-800 dark:text-amber-400 leading-relaxed">
                <strong>Note:</strong> The dataset is approximately <strong>2TB</strong>. Please ensure you have sufficient storage space available. For questions regarding the dataset, contact <a href="mailto:mit-dcc@mit.edu" className="font-semibold hover:underline">mit-dcc@mit.edu</a>.
              </p>
            </div>

            <div className="mt-0 w-full overflow-hidden">
                <div className="flex justify-between items-center mb-1.5">
                  <span className="text-xs font-bold text-gray-500 dark:text-slate-400 uppercase tracking-wider">Download Command</span>
                  <button 
                    onClick={copyToClipboard}
                    className="text-[10px] font-bold text-purple-700 dark:text-purple-400 bg-purple-100 hover:bg-purple-200 dark:bg-purple-900/30 dark:hover:bg-purple-900/50 px-2 py-1 rounded transition-colors"
                  >
                    {copied ? 'Copied!' : 'Copy'}
                  </button>
                </div>
                <div className="bg-gray-900 rounded-lg p-3 border border-gray-800 overflow-x-auto w-full custom-scrollbar">
                  <code className="text-purple-400 font-mono text-xs whitespace-nowrap">
                    {awsCommand}
                  </code>
                </div>
            </div>
          </div>
        </div>

      </div>
    </div>
  );
}

function DataCard({ title, icon, desc }: { title: string, icon: React.ReactNode, desc: string }) {
  return (
    <div className="bg-gray-50 dark:bg-slate-950 border border-gray-200 dark:border-slate-800 rounded-xl p-4 sm:p-5 hover:border-purple-400 dark:hover:border-purple-500/50 transition-colors shadow-sm h-full flex flex-col">
      <div className="flex items-center gap-2 mb-3">
        <div className="shrink-0">{icon}</div>
        <h3 className="font-bold text-gray-900 dark:text-white text-sm leading-tight">{title}</h3>
      </div>
      <p className="text-xs text-gray-500 dark:text-slate-400 leading-relaxed flex-1">{desc}</p>
    </div>
  );
}