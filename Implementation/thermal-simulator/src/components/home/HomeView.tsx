"use client";

import React from 'react';
import { Server, Activity, ChartLine, Database, ChevronRight, Sun, Moon, Filter, Mail } from 'lucide-react';
import { TbMathFunction } from "react-icons/tb";

const GithubIcon = ({ className }: { className?: string }) => (
  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" className={className}>
    <path d="M12 2C6.477 2 2 6.477 2 12c0 4.42 2.865 8.166 6.839 9.489.5.092.682-.217.682-.482 0-.237-.008-.866-.013-1.7-2.782.603-3.369-1.34-3.369-1.34-.454-1.156-1.11-1.462-1.11-1.462-.908-.62.069-.608.069-.608 1.003.07 1.531 1.03 1.531 1.03.892 1.529 2.341 1.087 2.91.831.092-.646.35-1.086.636-1.336-2.22-.253-4.555-1.11-4.555-4.943 0-1.091.39-1.984 1.029-2.683-.103-.253-.446-1.27.098-2.647 0 0 .84-.269 2.75 1.025A9.578 9.578 0 0112 6.836c.85.004 1.705.114 2.504.336 1.909-1.294 2.747-1.025 2.747-1.025.546 1.379.203 2.394.1 2.647.64.699 1.028 1.592 1.028 2.683 0 3.842-2.339 4.687-4.566 4.935.359.309.678.919.678 1.852 0 1.336-.012 2.415-.012 2.743 0 .267.18.578.688.48C19.138 20.161 22 16.416 22 12c0-5.523-4.477-10-10-10z" />
  </svg>
);

interface HomeViewProps {
  theme: 'dark' | 'light';
  onToggleTheme: () => void;
  onNavigate: (view: 'CONFIG' | 'PHYSICS' | 'PREPROCESSING' | 'TRAINING' | 'DATASET') => void;
}

export default function HomeView({ theme, onToggleTheme, onNavigate }: HomeViewProps) {
  return (
    <div className="flex flex-col min-h-screen w-full">
      
      <header className="px-4 py-4 sm:px-6 flex justify-between items-center border-b border-gray-200 dark:border-slate-800 bg-white/50 dark:bg-slate-900/50 backdrop-blur-md sticky top-0 z-20">
        <div className="flex items-center gap-2 sm:gap-3">
          <Server className="w-6 h-6 sm:w-8 sm:h-8 text-blue-600 dark:text-blue-500" />
          <h1 className="text-xl sm:text-2xl font-bold tracking-tight">Thermal<span className="text-blue-600 dark:text-blue-500">ODE</span></h1>
        </div>
        <div className="flex items-center gap-4 sm:gap-5">
          
          {/* New Contact Us Link */}
          <a 
            href="https://mail.google.com/mail/?view=cm&fs=1&to=saket.s.sontakke@gmail.com" 
            target="_blank" 
            rel="noopener noreferrer" 
            className="flex items-center gap-1.5 text-sm font-semibold text-gray-600 hover:text-blue-600 dark:text-slate-300 dark:hover:text-blue-400 transition-colors"
          >
            <Mail className="w-5 h-5 sm:w-5 sm:h-5" />
            <span className="hidden sm:inline">Contact Us</span>
          </a>

          {/* Divider line for larger screens */}
          <div className="w-px h-5 bg-gray-300 dark:bg-slate-700 hidden sm:block"></div>

          <a href="https://github.com/saket-sontakke/Thermal-Aware-HPC-Simulator-and-Workload-Scheduler.git" target="_blank" rel="noopener noreferrer" className="text-gray-500 hover:text-gray-900 dark:text-slate-400 dark:hover:text-white transition-colors">
            <GithubIcon className="w-6 h-6 sm:w-7 sm:h-7" />
          </a>
          <button onClick={onToggleTheme} className="p-2 bg-gray-200 dark:bg-slate-800 rounded-lg text-gray-700 dark:text-slate-300 hover:bg-gray-300 dark:hover:bg-slate-700 transition-colors">
            {theme === 'dark' ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
          </button>
        </div>
      </header>

      {/* flex-1 allows this container to grow. py-10 ensures there's breathing room if it scrolls */}
      <main className="flex-1 flex flex-col justify-center p-4 py-10 sm:p-6 lg:p-8 w-full max-w-[90rem] mx-auto">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-10 lg:gap-12 xl:gap-16 items-center w-full max-w-7xl mx-auto my-auto">
          
          {/* Left Column: Hero & Action */}
          <div className="space-y-5 sm:space-y-6 text-left lg:ml-0 xl:ml-7 -mt-14">
            <h2 className="text-4xl sm:text-5xl lg:text-[3rem] xl:text-[3.5rem] font-extrabold tracking-tight leading-[1.15]">
              {/* Thermal-Aware HPC Simulator and Workload Scheduler */}
              HPC Simulator and Thermal-Aware Workload Scheduler
            </h2>
            <div className="pt-2">
              <button 
                onClick={() => onNavigate('CONFIG')} 
                className="group inline-flex items-center gap-2 sm:gap-3 bg-blue-600 hover:bg-blue-700 text-white font-bold text-base sm:text-lg px-6 sm:px-8 py-3 sm:py-4 rounded-2xl shadow-lg shadow-blue-500/30 transition-all hover:-translate-y-1"
              >
                Launch Dashboard 
                <ChevronRight className="w-5 h-5 sm:w-6 sm:h-6 group-hover:translate-x-1 transition-transform" />
              </button>
            </div>
          </div>

          {/* Right Column: Information Cards */}
          <div className="flex flex-col gap-3 sm:gap-4 w-full">
            
            <button 
              onClick={() => onNavigate('PHYSICS')} 
              className="group flex flex-row items-center gap-4 text-left bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 hover:border-amber-400 dark:hover:border-amber-500 p-4 sm:p-5 rounded-2xl shadow-sm hover:shadow-md transition-all"
            >
              <div className="bg-amber-100 dark:bg-amber-900/30 p-3 rounded-xl text-amber-600 dark:text-amber-400 group-hover:scale-110 transition-transform shrink-0">
                <TbMathFunction className="w-6 h-6" />
              </div>
              <div>
                <h3 className="text-base font-bold mb-0.5">Physics & ODE Engine</h3>
                <p className="text-gray-600 dark:text-slate-400 text-xs sm:text-sm leading-snug">Deep dive into the two-mass thermal model equations and how the simulation calculates temperature delta.</p>
              </div>
            </button>

            <button 
              onClick={() => onNavigate('PREPROCESSING')} 
              className="group flex flex-row items-center gap-4 text-left bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 hover:border-indigo-400 dark:hover:border-indigo-500 p-4 sm:p-5 rounded-2xl shadow-sm hover:shadow-md transition-all"
            >
              <div className="bg-indigo-100 dark:bg-indigo-900/30 p-3 rounded-xl text-indigo-600 dark:text-indigo-400 group-hover:scale-110 transition-transform shrink-0">
                <Filter className="w-6 h-6" />
              </div>
              <div>
                <h3 className="text-base font-bold mb-0.5">Data Preprocessing</h3>
                <p className="text-gray-600 dark:text-slate-400 text-xs sm:text-sm leading-snug">Pipeline from raw telemetry CSVs to PyTorch-ready ODE tensors and PINN prior extraction.</p>
              </div>
            </button>

            <button 
              onClick={() => onNavigate('TRAINING')} 
              className="group flex flex-row items-center gap-4 text-left bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 hover:border-emerald-400 dark:hover:border-emerald-500 p-4 sm:p-5 rounded-2xl shadow-sm hover:shadow-md transition-all"
            >
              <div className="bg-emerald-100 dark:bg-emerald-900/30 p-3 rounded-xl text-emerald-600 dark:text-emerald-400 group-hover:scale-110 transition-transform shrink-0">
                <ChartLine className="w-6 h-6" />
              </div>
              <div>
                <h3 className="text-base font-bold mb-0.5">Model Calibration</h3>
                <p className="text-gray-600 dark:text-slate-400 text-xs sm:text-sm leading-snug">Explore how we utilized PyTorch and gradient descent to fit our ODE parameters to real-world hardware.</p>
              </div>
            </button>

            <button 
              onClick={() => onNavigate('DATASET')} 
              className="group flex flex-row items-center gap-4 text-left bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 hover:border-purple-400 dark:hover:border-purple-500 p-4 sm:p-5 rounded-2xl shadow-sm hover:shadow-md transition-all"
            >
              <div className="bg-purple-100 dark:bg-purple-900/30 p-3 rounded-xl text-purple-600 dark:text-purple-400 group-hover:scale-110 transition-transform shrink-0">
                <Database className="w-6 h-6" />
              </div>
              <div>
                <h3 className="text-base font-bold mb-0.5">MIT Supercloud Dataset</h3>
                <p className="text-gray-600 dark:text-slate-400 text-xs sm:text-sm leading-snug">Learn about the MIT Supercloud dataset that made this project possible, featuring dual-V100 telemetry.</p>
              </div>
            </button>
            
          </div>

        </div>
      </main>
    </div>
  );
}