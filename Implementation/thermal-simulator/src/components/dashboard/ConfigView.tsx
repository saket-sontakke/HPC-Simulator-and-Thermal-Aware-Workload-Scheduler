"use client";

import React, { useState, useMemo } from 'react';
import { ThemeableNumberInput } from '../ui/ThemeableNumberInput';
import { SchedulingMode } from '../../lib/simulator/types';
import { Settings, Upload, Sun, Moon, Home, Folder, X, CheckSquare, Play } from 'lucide-react';

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
  onLoadSampleFiles: (paths: string[]) => void;
  onInstantQuickStart: () => void;
  onLaunch: () => void;
  onGoHome: () => void;
}

// --- SAMPLE DATA MANIFEST ---
const SAMPLE_MANIFEST: Record<string, string[]> = {
  "bert-base-uncased": [
    "1181085738973-r7343737-n851693.csv", "1262594548610-r629115-n43543.csv", "1808531699064-r7343737-n911952.csv", "2003720312370-r1682297-n911952.csv", "2312801397194-r8333645-n851693.csv", "2676655762435-r8939293-n136082.csv", "2896243579938-r4858666-n830961.csv", "3054925459831-r8579942-n208530.csv", "4225549041985-r3041626-n851693.csv", "439033388226-r2998125-n208530.csv", "4610501415540-r3879907-n136082.csv", "4652979158115-r1485405-n43543.csv", "6067762498061-r4179716-n911952.csv", "6156279086493-r9102715-n830961.csv", "684781084440-r5715171-n136082.csv"
  ],
  "conv": [
    "12259060320474-r2825489-n139058.csv", "15179646833041-r1682297-n851693.csv", "27804537652393-r8333645-n685852.csv", "32249095466700-r3741709-n685852.csv", "37981585217450-r3879907-n208530.csv", "56445763544776-r4822976-n139058.csv", "5774828082734-r3741709-n685852.csv", "58476744174890-r9189566-n830961.csv", "62125685928952-r8333645-n685852.csv", "63937018334140-r4179716-n851693.csv", "66128261934918-r4179716-n386398.csv", "71369780233844-r7343737-n386398.csv", "741295796350-r4229531-n386398.csv", "75893069107267-r7217787-n851693.csv", "9002016628354-r4858666-n976057.csv"
  ]
  // Note: Truncated sample data for brevity, keep your full SAMPLE_MANIFEST here
};

export default function ConfigView(props: ConfigViewProps) {
  const percent = props.uploadStats.total > 0 
    ? Math.round((props.uploadStats.current / props.uploadStats.total) * 100) 
    : 0;

  // --- Modal State ---
  const [isSampleModalOpen, setIsSampleModalOpen] = useState(false);
  const [randomCount, setRandomCount] = useState<number | ''>(100);
  const [selectedCategory, setSelectedCategory] = useState<string>(Object.keys(SAMPLE_MANIFEST)[0]);
  const [selectedFiles, setSelectedFiles] = useState<Set<string>>(new Set());

  const totalSampleJobs = useMemo(() => Object.values(SAMPLE_MANIFEST).flat().length, []);

  // --- HANDLERS ---
  const handleRandomCountChange = (e: React.ChangeEvent<HTMLInputElement> | { target: { value: string } }) => {
  const val = e.target.value;
  if (val === '') { 
    setRandomCount(''); 
    return; 
  }
  
  let num = parseInt(val);
  if (isNaN(num)) return;
  
  if (num > totalSampleJobs) num = totalSampleJobs;
  setRandomCount(num);
};

  const handleRandomCountBlur = () => {
    if (randomCount === '' || randomCount < 1) {
      setRandomCount(1);
    }
  };

  const handleRandomize = () => {
    if (!randomCount || randomCount <= 0) return;
    const allPaths = Object.entries(SAMPLE_MANIFEST).flatMap(([category, files]) => 
      files.map(filename => `/samples/${category}/${filename}`)
    );
    const shuffled = [...allPaths].sort(() => 0.5 - Math.random());
    const selected = shuffled.slice(0, Math.min(randomCount, allPaths.length));
    
    props.onLoadSampleFiles(selected);
    setIsSampleModalOpen(false); 
  };

  const handleLoadManual = () => {
    const paths = Array.from(selectedFiles).map(file => `/samples/${selectedCategory}/${file}`);
    props.onLoadSampleFiles(paths);
    setSelectedFiles(new Set()); 
    setIsSampleModalOpen(false); 
  };

  const toggleFile = (filename: string) => {
    const next = new Set(selectedFiles);
    if (next.has(filename)) next.delete(filename);
    else next.add(filename);
    setSelectedFiles(next);
  };

  const handleSelectAll = () => {
    const currentFiles = SAMPLE_MANIFEST[selectedCategory];
    if (selectedFiles.size === currentFiles.length) {
      setSelectedFiles(new Set());
    } else {
      setSelectedFiles(new Set(currentFiles));
    }
  };

  return (
    <div className="w-full h-full flex flex-col items-center justify-center p-4 bg-gray-50 dark:bg-slate-950 overflow-hidden box-border">
      
      {/* Sample Modal */}
      {isSampleModalOpen && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 sm:p-8 bg-gray-900/60 dark:bg-black/80 backdrop-blur-sm animate-in fade-in duration-200">
          <div className="bg-white dark:bg-slate-900 w-full max-w-5xl rounded-2xl shadow-2xl flex flex-col max-h-full border border-gray-200 dark:border-slate-800 overflow-hidden animate-in zoom-in-95 duration-200">
            <div className="flex justify-between items-center p-4 sm:p-5 border-b border-gray-100 dark:border-slate-800 bg-gray-50 dark:bg-slate-900/50 shrink-0">
              <h2 className="text-lg sm:text-xl font-bold text-gray-800 dark:text-gray-100">Load Sample Job Traces</h2>
              <button onClick={() => setIsSampleModalOpen(false)} className="p-2 text-gray-500 hover:bg-gray-200 dark:hover:bg-slate-700 rounded-lg transition-colors" title="Close">
                <X className="w-5 h-5" />
              </button>
            </div>
            <div className="flex-1 overflow-y-auto p-4 sm:p-6 flex flex-col md:flex-row gap-6 bg-gray-50/50 dark:bg-slate-950/50 min-h-0 custom-scrollbar">
              <div className="w-full md:w-1/3 flex flex-col gap-6 shrink-0">
                <div className="bg-white dark:bg-slate-800 p-4 sm:p-5 rounded-xl border border-gray-200 dark:border-slate-700 shadow-sm flex flex-col gap-3">
                   <div className="flex justify-between items-center mb-1">
                      <h3 className="font-bold text-blue-600 dark:text-blue-400 text-sm">Auto-Randomizer</h3>
                      <span className="text-[11px] sm:text-xs text-blue-800 dark:text-blue-300 font-medium bg-blue-100 dark:bg-blue-900/50 px-2 py-1 rounded">{totalSampleJobs} Available</span>
                   </div>
                   <div className="space-y-1">
                     <label className="text-xs text-gray-500 dark:text-slate-400">Number of random jobs:</label>
                     <ThemeableNumberInput value={randomCount} onChange={handleRandomCountChange} onBlur={handleRandomCountBlur} min={1} max={totalSampleJobs} />
                   </div>
                   <button onClick={handleRandomize} className="w-full bg-blue-600 hover:bg-blue-700 text-white mt-1 px-4 py-2.5 rounded-lg text-sm font-bold transition-colors shadow-sm">
                     Queue Random Jobs
                   </button>
                </div>
                <div className="bg-white dark:bg-slate-800 p-4 sm:p-5 rounded-xl border border-gray-200 dark:border-slate-700 shadow-sm flex flex-col gap-3 flex-1">
                   <h3 className="font-bold text-gray-800 dark:text-gray-200 text-sm flex items-center gap-2 mb-1">
                     <Folder className="w-4 h-4 text-gray-500"/> Browse Directory
                   </h3>
                   <label className="text-xs text-gray-500 dark:text-slate-400">Select a model category:</label>
                   <select 
                     value={selectedCategory} 
                     onChange={(e) => { setSelectedCategory(e.target.value); setSelectedFiles(new Set()); }}
                     className="w-full bg-gray-50 dark:bg-slate-900 border border-gray-300 dark:border-slate-600 rounded-lg px-3 py-2.5 text-sm text-gray-700 dark:text-slate-200 outline-none focus:ring-2 focus:ring-blue-500 transition-shadow"
                   >
                     {Object.keys(SAMPLE_MANIFEST).map(cat => <option key={cat} value={cat}>{cat} ({SAMPLE_MANIFEST[cat].length} traces)</option>)}
                   </select>
                </div>
              </div>
              <div className="w-full md:w-2/3 bg-white dark:bg-slate-800 border border-gray-200 dark:border-slate-700 rounded-xl shadow-sm flex flex-col min-h-[300px] md:min-h-0 overflow-hidden">
                 <div className="p-3 sm:p-4 border-b border-gray-100 dark:border-slate-700 bg-gray-50 dark:bg-slate-900/50 flex justify-between items-center shrink-0">
                   <span className="text-sm font-bold text-gray-800 dark:text-gray-200 font-mono">{selectedCategory}/</span>
                   <button onClick={handleSelectAll} className="text-xs sm:text-sm font-medium text-blue-600 dark:text-blue-400 hover:text-blue-700 dark:hover:text-blue-300 flex items-center gap-1 transition-colors">
                     <CheckSquare className="w-3.5 h-3.5" />
                     {selectedFiles.size === SAMPLE_MANIFEST[selectedCategory].length ? 'Deselect All' : 'Select All'}
                   </button>
                 </div>
                 <div className="flex-1 overflow-y-auto p-2 sm:p-3 custom-scrollbar bg-white dark:bg-slate-800">
                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                      {SAMPLE_MANIFEST[selectedCategory]?.map(filename => (
                        <label key={filename} className="flex items-center gap-3 p-2.5 sm:p-3 hover:bg-gray-50 dark:hover:bg-slate-700 rounded-lg cursor-pointer text-xs font-mono transition-colors border border-transparent hover:border-gray-200 dark:hover:border-slate-600">
                          <input 
                            type="checkbox" 
                            checked={selectedFiles.has(filename)} 
                            onChange={() => toggleFile(filename)} 
                            className="rounded text-blue-600 focus:ring-blue-500 w-4 h-4 cursor-pointer"
                          />
                          <span className="truncate text-gray-700 dark:text-slate-300">{filename}</span>
                        </label>
                      ))}
                    </div>
                 </div>
                 <div className="p-4 border-t border-gray-100 dark:border-slate-700 bg-gray-50 dark:bg-slate-900/50 shrink-0">
                    <button 
                      onClick={handleLoadManual} 
                      disabled={selectedFiles.size === 0} 
                      className="w-full bg-gray-800 hover:bg-gray-900 dark:bg-slate-700 dark:hover:bg-slate-600 disabled:bg-gray-300 dark:disabled:bg-slate-800 disabled:text-gray-500 text-white px-4 py-3 rounded-lg text-sm font-bold transition-colors shadow-sm"
                    >
                      Queue {selectedFiles.size} Selected Files
                    </button>
                 </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Main Spacious Layout (Now spans full width and height without scrolling) */}
      <div className="w-full h-full flex flex-col gap-4 overflow-hidden">
        
        {/* Top Header Bar */}
        <div className="shrink-0 flex justify-between items-center bg-white dark:bg-slate-900 p-3 sm:p-4 rounded-xl shadow-sm border border-gray-200 dark:border-slate-800">
          <div className="flex items-center gap-4">
            <button onClick={props.onGoHome} className="p-2.5 bg-gray-50 dark:bg-slate-800 border border-gray-200 dark:border-slate-700 hover:bg-gray-100 dark:hover:bg-slate-700 rounded-xl text-gray-600 dark:text-slate-400 transition-colors shrink-0" title="Return to Home">
              <Home className="w-5 h-5" />
            </button>
            <div className="w-px h-8 bg-gray-300 dark:bg-slate-700 hidden sm:block"></div>
            <div>
              <h1 className="text-xl font-bold text-gray-900 dark:text-white leading-tight flex items-center gap-2">
                <Settings className="w-6 h-6 text-blue-500 shrink-0 hidden sm:block"/> Simulator Setup
              </h1>
              <p className="text-xs text-gray-500 dark:text-slate-400 mt-0.5">Configure hardware infrastructure, scheduler policy, and data traces.</p>
            </div>
          </div>
          <button onClick={props.onToggleTheme} className="p-2.5 bg-gray-100 dark:bg-slate-800 rounded-xl text-gray-700 dark:text-slate-300 hover:bg-gray-200 dark:hover:bg-slate-700 transition-colors shrink-0">
            {props.theme === 'dark' ? <Sun className="w-5 h-5"/> : <Moon className="w-5 h-5"/>}
          </button>
        </div>

        {/* Content Grid (Fills remaining height) */}
        <div className="grid grid-cols-1 md:grid-cols-12 gap-4 w-full flex-1 min-h-0">
          
          {/* Left Column: Configs */}
          <div className="md:col-span-5 flex flex-col gap-4 h-full min-h-0">
            
            {/* 1. Hardware Box */}
            <div className="bg-white dark:bg-slate-900 p-4 rounded-xl shadow-sm border border-gray-200 dark:border-slate-800 flex flex-col shrink-0 gap-4">
              <h2 className="text-base font-bold text-gray-800 dark:text-gray-100">
                Infrastructure
              </h2>
              <div className="space-y-1">
                <div className="flex justify-between items-center">
                  <label className="text-sm font-semibold text-gray-700 dark:text-slate-300">Datacenter Nodes</label>
                  <span className="text-xs text-gray-500 dark:text-slate-500">Max: 250</span>
                </div>
                <ThemeableNumberInput value={props.nodeCount} onChange={props.onNodeChange} onBlur={props.onNodeBlur} min={1} max={250} />
              </div>
              <div className="space-y-1">
                <div className="flex justify-between items-center">
                  <label className="text-sm font-semibold text-gray-700 dark:text-slate-300">HVAC Ambient (°C)</label>
                  <span className="text-xs text-gray-500 dark:text-slate-500">Range: 15-45°C</span>
                </div>
                <ThemeableNumberInput value={props.ambientTemp} onChange={props.onTempChange} onBlur={props.onTempBlur} min={15} max={45} />
              </div>
            </div>

            {/* 2. Scheduler Box */}
            <div className="bg-white dark:bg-slate-900 p-4 rounded-xl shadow-sm border border-gray-200 dark:border-slate-800 flex flex-col flex-1 min-h-0 gap-4 overflow-y-auto custom-scrollbar">
              <div className="flex justify-between items-center shrink-0">
                <h2 className="text-base font-bold text-gray-800 dark:text-gray-100">
                  Scheduling Policy
                </h2>
                
                {/* Modern A/B Test Toggle */}
                <div className="group relative flex items-center gap-2 cursor-pointer select-none" onClick={() => props.onABTestChange(!props.isABTest)}>
                  <span className={`text-xs font-bold transition-colors ${props.isABTest ? 'text-blue-600 dark:text-blue-400' : 'text-gray-400 dark:text-slate-500'}`}>
                    A/B Test Mode
                  </span>
                  <button type="button" className={`relative inline-flex h-5 w-9 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none ${props.isABTest ? 'bg-blue-600' : 'bg-gray-300 dark:bg-slate-600'}`} role="switch" aria-checked={props.isABTest}>
                    <span aria-hidden="true" className={`pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow-sm ring-0 transition duration-200 ease-in-out ${props.isABTest ? 'translate-x-4' : 'translate-x-0'}`} />
                  </button>
                  <div className="absolute right-0 top-8 w-64 p-3 bg-gray-800 dark:bg-gray-700 text-white text-xs rounded-xl opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-10 shadow-xl leading-relaxed">
                    Runs both the Standard and Thermal-Aware schedulers side-by-side with identical workloads to compare their thermal efficiency.
                  </div>
                </div>
              </div>

              <div className={`flex flex-col gap-3 transition-opacity duration-300 ${props.isABTest ? 'opacity-40 pointer-events-none' : ''}`}>
                <label className={`flex items-center p-3 sm:p-4 rounded-xl border-2 cursor-pointer transition-all ${props.mode === 'STANDARD' ? 'border-amber-500 bg-amber-50 dark:bg-amber-900/10' : 'border-gray-200 dark:border-slate-700 hover:border-amber-300 dark:hover:border-slate-600'}`}>
                  <input type="radio" name="schedulerMode" checked={props.mode === 'STANDARD'} onChange={() => props.onModeChange('STANDARD')} className="hidden" />
                  <div className={`w-4 h-4 rounded-full border flex items-center justify-center mr-3 shrink-0 ${props.mode === 'STANDARD' ? 'border-amber-500' : 'border-gray-400 dark:border-slate-500'}`}>
                    {props.mode === 'STANDARD' && <div className="w-2 h-2 rounded-full bg-amber-500" />}
                  </div>
                  <div>
                    <div className={`font-bold text-sm ${props.mode === 'STANDARD' ? 'text-amber-700 dark:text-amber-500' : 'text-gray-700 dark:text-slate-300'}`}>Standard Scheduler</div>
                    <div className="text-xs text-gray-500 dark:text-slate-400 mt-0.5">Utilizes a First-Fit placement strategy, allocating jobs to the first available hardware without thermal consideration.</div>
                  </div>
                </label>

                <label className={`flex items-center p-3 sm:p-4 rounded-xl border-2 cursor-pointer transition-all ${props.mode === 'THERMAL_AWARE' ? 'border-emerald-500 bg-emerald-50 dark:bg-emerald-900/10' : 'border-gray-200 dark:border-slate-700 hover:border-emerald-300 dark:hover:border-slate-600'}`}>
                  <input type="radio" name="schedulerMode" checked={props.mode === 'THERMAL_AWARE'} onChange={() => props.onModeChange('THERMAL_AWARE')} className="hidden" />
                  <div className={`w-4 h-4 rounded-full border flex items-center justify-center mr-3 shrink-0 ${props.mode === 'THERMAL_AWARE' ? 'border-emerald-500' : 'border-gray-400 dark:border-slate-500'}`}>
                    {props.mode === 'THERMAL_AWARE' && <div className="w-2 h-2 rounded-full bg-emerald-500" />}
                  </div>
                  <div>
                    <div className={`font-bold text-sm ${props.mode === 'THERMAL_AWARE' ? 'text-emerald-700 dark:text-emerald-500' : 'text-gray-700 dark:text-slate-300'}`}>Thermal-Aware (ODE)</div>
                    <div className="text-xs text-gray-500 dark:text-slate-400 mt-0.5">Utilizes Ordinary Differential Equations (ODEs) to predict thermal state of GPUs and optimize job placement.</div>
                  </div>
                </label>
              </div>
            </div>
          </div>

          {/* Right Column: Workloads */}
          <div className="md:col-span-7 flex flex-col h-full min-h-0">
            <div className="bg-white dark:bg-slate-900 p-4 rounded-xl shadow-sm border border-gray-200 dark:border-slate-800 flex flex-col h-full min-h-0 gap-4">
              <h2 className="text-base font-bold text-gray-800 dark:text-gray-100 shrink-0">
                Workload Provisioning
              </h2>
              
              <div className="flex-1 flex flex-col gap-4 min-h-0">
                
                {/* Option 1: Instant Quick Start */}
                <button 
                  onClick={props.onInstantQuickStart}
                  disabled={props.isUploading}
                  className="w-full shrink-0 overflow-hidden bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 text-white rounded-xl shadow-md transition-all disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center py-4 px-6"
                >
                  <div className="flex flex-col text-center">
                    <span className="font-bold text-lg leading-tight tracking-wide">Load Sample Traces</span>
                    <span className="text-sm text-blue-100 mt-1">Instantly queue a pre-configured suite of 52 diverse jobs.</span>
                  </div>
                </button>

                <div className="flex items-center gap-4 py-1 shrink-0">
                  <div className="h-px bg-gray-200 dark:bg-slate-700 flex-1"></div>
                  <span className="text-xs font-bold text-gray-400 dark:text-slate-500 uppercase tracking-widest">OR</span>
                  <div className="h-px bg-gray-200 dark:bg-slate-700 flex-1"></div>
                </div>

                {/* Options 2 & 3: Upload & Browse */}
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 flex-1 min-h-0">
                <label className={`flex flex-col items-center justify-center p-4 border-2 border-dashed border-gray-300 dark:border-slate-600 rounded-xl transition-all h-full ${props.isUploading ? 'opacity-50 cursor-not-allowed bg-gray-50 dark:bg-slate-800' : 'hover:border-indigo-400 hover:bg-indigo-50 dark:hover:bg-slate-800 dark:hover:border-indigo-500 cursor-pointer'}`}>
                    <Upload className="w-8 h-8 text-indigo-500 mb-3" />
                    <span className="font-bold text-gray-800 dark:text-gray-200">Upload CSV Traces</span>
                    <span className="text-xs text-gray-500 dark:text-slate-400 text-center mt-2">Import power trace datasets (.csv) for simulation.</span>
                    <input type="file" accept=".csv" multiple className="hidden" onChange={props.onFileUpload} disabled={props.isUploading} />
                  </label>

                  <button 
                    type="button"
                    onClick={() => setIsSampleModalOpen(true)}
                    disabled={props.isUploading}
                    className={`flex flex-col items-center justify-center p-4 border-2 border-gray-200 dark:border-slate-700 rounded-xl transition-all h-full ${props.isUploading ? 'opacity-50 cursor-not-allowed' : 'hover:border-blue-400 hover:bg-blue-50 dark:hover:bg-slate-800 dark:hover:border-blue-500'}`}
                  >
                    <Folder className="w-8 h-8 text-blue-500 mb-3" />
                    <span className="font-bold text-gray-800 dark:text-gray-200">Browse Library</span>
                    <span className="text-xs text-gray-500 dark:text-slate-400 text-center mt-2">Select Job Traces from specific models from the MIT Supercloud Dataset.</span>
                  </button>

                </div>

              </div>

              {/* Status Footer */}
              <div className="mt-2 bg-gray-50 dark:bg-slate-900/50 p-4 rounded-xl border border-gray-200 dark:border-slate-700 shrink-0">
                {props.isUploading ? (
                  <div className="flex flex-col gap-2">
                    <div className="flex justify-between items-end text-sm text-gray-600 dark:text-slate-300 font-medium">
                      <span>Parsing {props.uploadStats.current} of {props.uploadStats.total} files...</span>
                      <span className="font-bold text-blue-600 dark:text-blue-400">{percent}%</span>
                    </div>
                    <div className="w-full bg-gray-200 dark:bg-slate-800 rounded-full h-3 overflow-hidden shadow-inner">
                      <div className="bg-blue-600 h-full rounded-full transition-all duration-100" style={{ width: `${percent}%` }}></div>
                    </div>
                  </div>
                ) : (
                  <div className="flex items-center justify-between">
                    <div className="flex flex-col">
                      <span className="text-xs text-gray-500 dark:text-slate-400 uppercase tracking-wide font-bold">Current Queue Status</span>
                      <div className="flex items-center gap-2 mt-0.5">
                        <div className={`w-3 h-3 rounded-full ${props.jobCount > 0 ? 'bg-emerald-500 animate-pulse' : 'bg-gray-400 dark:bg-slate-600'}`}></div>
                        <span className={`text-lg font-bold ${props.jobCount > 0 ? 'text-emerald-700 dark:text-emerald-400' : 'text-gray-500 dark:text-slate-400'}`}>
                          {props.jobCount} Jobs Ready
                        </span>
                      </div>
                    </div>
                    
                    {/* The Big Launch Button */}
                    <button 
                      onClick={props.onLaunch} 
                      disabled={props.jobCount === 0 || props.isUploading}
                      className="bg-emerald-600 hover:bg-emerald-500 disabled:bg-gray-300 dark:disabled:bg-slate-800 disabled:text-gray-500 text-white font-bold py-3 px-6 rounded-xl shadow-lg transition-all flex items-center gap-2 disabled:cursor-not-allowed group"
                    >
                      <Play className={`w-5 h-5 ${props.jobCount > 0 ? 'fill-white' : ''}`} />
                      Launch Dashboard
                    </button>
                  </div>
                )}
              </div>
            </div>
          </div>

        </div>
      </div>
    </div>
  );
}