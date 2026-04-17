"use client";

import React from 'react';
import { ThemeableNumberInput } from '../ui/ThemeableNumberInput';
import { SchedulingMode } from '../../lib/simulator/types';
import { Settings, Upload, Sun, Moon, Home, Info } from 'lucide-react';

interface ConfigViewProps {
  theme: 'dark' | 'light';
  onToggleTheme: () => void;
  nodeCount: number | '';
  onNodeChange: (e: any) => void;
  onNodeBlur: () => void;
  ambientTemp: number | '';
  onTempChange: (e: any) => void;
  onTempBlur: () => void;
  mode: SchedulingMode;
  onModeChange: (mode: SchedulingMode) => void;
  isABTest: boolean;
  onABTestChange: (val: boolean) => void;
  isUploading: boolean;
  uploadStats: { current: number; total: number };
  jobCount: number;
  onFileUpload: (e: React.ChangeEvent<HTMLInputElement>) => void;
  onLaunch: () => void;
  onGoHome: () => void;
}

export default function ConfigView(props: ConfigViewProps) {
  const percent = props.uploadStats.total > 0 
    ? Math.round((props.uploadStats.current / props.uploadStats.total) * 100) 
    : 0;

  return (
    <div className="flex-1 flex flex-col items-center justify-center p-4 sm:p-8">
      <div className="max-w-2xl w-full bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl shadow-xl overflow-hidden">
        <div className="p-4 sm:p-6 border-b border-gray-100 dark:border-slate-800 flex flex-row justify-between items-center bg-gray-50 dark:bg-slate-900/50 gap-4">
          <div className="flex items-center gap-3 sm:gap-4">
            <button 
              onClick={props.onGoHome} 
              className="p-2 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 hover:bg-gray-100 dark:hover:bg-slate-700 rounded-lg text-gray-600 dark:text-slate-400 transition-colors shrink-0"
              title="Return to Home"
            >
              <Home className="w-5 h-5" />
            </button>
            <div className="w-px h-6 bg-gray-300 dark:bg-slate-700 hidden sm:block"></div>
            <h2 className="text-xl sm:text-2xl font-bold flex items-center gap-2">
              <Settings className="w-5 h-5 sm:w-6 sm:h-6 text-blue-500 shrink-0"/> Configuration
            </h2>
          </div>
          <button onClick={props.onToggleTheme} className="p-2 bg-gray-200 dark:bg-slate-800 rounded-lg text-gray-700 dark:text-slate-300 hover:bg-gray-300 dark:hover:bg-slate-700 transition-colors shrink-0">
            {props.theme === 'dark' ? <Sun className="w-4 h-4"/> : <Moon className="w-4 h-4"/>}
          </button>
        </div>
        <div className="p-4 sm:p-8 space-y-6 sm:space-y-8">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 sm:gap-6">
            <div className="space-y-2">
              <div className="flex justify-between">
                <label className="text-sm font-semibold">Datacenter Nodes</label>
                <span className="text-xs text-gray-500">Max: 250</span>
              </div>
              <ThemeableNumberInput value={props.nodeCount} onChange={props.onNodeChange} onBlur={props.onNodeBlur} min={1} max={250} />
            </div>
            <div className="space-y-2">
              <div className="flex justify-between">
                <label className="text-sm font-semibold">HVAC Ambient (°C)</label>
                <span className="text-xs text-gray-500">Range: 15-45°C</span>
              </div>
              <ThemeableNumberInput value={props.ambientTemp} onChange={props.onTempChange} onBlur={props.onTempBlur} min={15} max={45} />
            </div>
          </div>
          <div className="space-y-2 border-t border-gray-100 dark:border-slate-800 pt-4 sm:pt-6">
            
            <div className="flex flex-col sm:flex-row sm:justify-between sm:items-center gap-2 mb-2">
              <label className="text-sm font-semibold">Scheduling Policy</label>
              
              <div className="flex items-center gap-3">
                {/* Info Tooltip */}
                <div className="group relative flex items-center">
                  <Info className="w-4 h-4 text-gray-400 hover:text-blue-500 transition-colors cursor-help" />
                  <div className="absolute right-0 sm:right-0 top-6 w-[80vw] max-w-xs p-2.5 bg-gray-800 dark:bg-gray-700 text-white text-xs rounded-lg opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-10 shadow-xl leading-relaxed">
                    Run both the Standard and Thermal-Aware schedulers side-by-side with identical workloads and identical configuration to compare their thermal efficiency and performance.
                  </div>
                </div>

                {/* Modern Animated Toggle Switch */}
                <div 
                  className="flex items-center gap-2 cursor-pointer group select-none"
                  onClick={() => props.onABTestChange(!props.isABTest)}
                >
                  <span className={`text-xs font-bold transition-colors ${props.isABTest ? 'text-blue-600 dark:text-blue-400' : 'text-gray-500 dark:text-slate-400 group-hover:text-gray-800 dark:group-hover:text-slate-200'}`}>
                    A/B Testing
                  </span>
                  <button
                    type="button"
                    className={`relative inline-flex h-5 w-9 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none ${props.isABTest ? 'bg-blue-600' : 'bg-gray-300 dark:bg-slate-600'}`}
                    role="switch"
                    aria-checked={props.isABTest}
                  >
                    <span
                      aria-hidden="true"
                      className={`pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow-sm ring-0 transition duration-200 ease-in-out ${props.isABTest ? 'translate-x-4' : 'translate-x-0'}`}
                    />
                  </button>
                </div>
              </div>
            </div>

            <div className={`flex flex-col sm:flex-row bg-gray-100 dark:bg-slate-800 p-1 rounded-lg border border-gray-200 dark:border-slate-700 transition-opacity ${props.isABTest ? 'opacity-50 pointer-events-none' : ''}`}>
              <button onClick={() => props.onModeChange('STANDARD')} className={`flex-1 px-4 py-2 sm:py-2.5 rounded-md text-[13px] sm:text-sm font-bold transition-all ${props.mode === 'STANDARD' ? 'bg-amber-500 text-white shadow' : 'text-gray-500 hover:text-gray-700 dark:text-slate-400'}`}>Standard (Thermal-Unaware)</button>
              <button onClick={() => props.onModeChange('THERMAL_AWARE')} className={`flex-1 px-4 py-2 sm:py-2.5 rounded-md text-[13px] sm:text-sm font-bold transition-all ${props.mode === 'THERMAL_AWARE' ? 'bg-emerald-600 text-white shadow' : 'text-gray-500 hover:text-gray-700 dark:text-slate-400'}`}>Thermal-Aware (ODE)</button>
            </div>
          </div>
          <div className="space-y-2 border-t border-gray-100 dark:border-slate-800 pt-4 sm:pt-6">
            <label className="text-sm font-semibold">Workload Queue</label>
            <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-3 sm:gap-4">
              <label className={`flex justify-center items-center gap-2 text-white px-4 py-2.5 sm:py-2 rounded-lg transition-colors text-center ${props.isUploading ? 'bg-gray-400 dark:bg-slate-700 cursor-not-allowed' : 'bg-gray-800 hover:bg-gray-900 dark:bg-slate-800 dark:hover:bg-slate-700 cursor-pointer'}`}>
                <Upload className="w-4 h-4 shrink-0" /> <span className="text-[13px] sm:text-sm font-medium">{props.isUploading ? 'Processing...' : 'Upload Job Traces (.csv)'}</span>
                <input type="file" accept=".csv" multiple className="hidden" onChange={props.onFileUpload} disabled={props.isUploading} />
              </label>
              
              <div className="flex flex-col flex-1 w-full sm:max-w-[280px] gap-1">
                {props.isUploading ? (
                  <>
                    <div className="flex justify-between items-end text-xs text-gray-500 dark:text-slate-400 font-medium px-1">
                      <span>Processing {props.uploadStats.current} of {props.uploadStats.total} files</span>
                      <span className="font-bold text-blue-600 dark:text-blue-400">{percent}%</span>
                    </div>
                    <div className="w-full bg-gray-200 dark:bg-slate-800 rounded-full h-2.5 overflow-hidden">
                      <div className="bg-blue-600 h-2.5 rounded-full transition-all duration-100" style={{ width: `${percent}%` }}></div>
                    </div>
                  </>
                ) : (
                  <span className="text-xs sm:text-sm font-medium bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 px-3 py-1.5 sm:py-1 rounded-full text-center">
                    {props.jobCount} Jobs Queued
                  </span>
                )}
              </div>
            </div>
          </div>
        </div>
        <div className="p-4 sm:p-6 bg-gray-50 dark:bg-slate-900/50 border-t border-gray-100 dark:border-slate-800">
          <button onClick={props.onLaunch} className="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-4 rounded-xl shadow-md transition-colors text-sm sm:text-base">
            Initialize & Launch Dashboard
          </button>
        </div>
      </div>
    </div>
  );
}