"use client";

import React, { useState, useRef, useEffect } from 'react';
import { 
  Server, Activity, ChartLine, Database, ChevronRight, Sun, Moon, 
  Filter, Mail, Copy, Check, ChevronDown, ExternalLink 
} from 'lucide-react';
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
  const [isDropdownOpen, setIsDropdownOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const emailAddress = "saket.s.sontakke@gmail.com";

  // Handle Copy Action
  const handleCopyEmail = (e: React.MouseEvent) => {
    e.stopPropagation(); // Prevents the dropdown from closing when clicking copy
    navigator.clipboard.writeText(emailAddress);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  // Close dropdown when clicking outside of it
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsDropdownOpen(false);
      }
    };
    
    // Only attach listener if dropdown is open
    if (isDropdownOpen) {
      document.addEventListener('mousedown', handleClickOutside);
    }
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [isDropdownOpen]);

  return (
    <div className="flex flex-col min-h-screen w-full overflow-x-hidden">
      
      <header className="px-3 py-3 sm:px-6 sm:py-4 flex justify-between items-center border-b border-gray-200 dark:border-slate-800 bg-white/50 dark:bg-slate-900/50 backdrop-blur-md sticky top-0 z-20">
        <div className="flex items-center gap-2 sm:gap-3 shrink-0">
          <h1 className="text-lg sm:text-2xl font-bold tracking-tight">Thermal<span className="text-blue-600 dark:text-blue-500">ODE</span></h1>
        </div>
        
        <div className="flex items-center gap-3 sm:gap-5">
            
          {/* Enhanced Dropdown Contact Us */}
          <div className="relative shrink-0" ref={dropdownRef}>
            <button 
              onClick={() => setIsDropdownOpen(!isDropdownOpen)}
              className="flex items-center gap-1.5 text-sm font-semibold text-gray-600 hover:text-blue-600 dark:text-slate-300 dark:hover:text-blue-400 transition-colors cursor-pointer outline-none"
            >
              <Mail className="w-4 h-4 sm:w-5 sm:h-5" />
              <span className="hidden sm:inline">Contact Us</span>
              <ChevronDown className={`hidden sm:block w-4 h-4 transition-transform duration-200 ${isDropdownOpen ? 'rotate-180' : ''}`} />
            </button>

            {/* Dropdown Menu */}
            {isDropdownOpen && (
              <div className="absolute top-full right-0 mt-3 w-72 bg-white dark:bg-slate-800 rounded-xl shadow-xl border border-gray-200 dark:border-slate-700 p-3 z-50 transform origin-top-right transition-all">
                <p className="text-xs text-gray-500 dark:text-slate-400 mb-2 font-medium px-1">Send us an email at:</p>
                
                <div className="flex items-center justify-between bg-gray-50 dark:bg-slate-900/50 rounded-lg p-2 border border-gray-200 dark:border-slate-700">
                  <span className="text-sm font-medium text-gray-800 dark:text-slate-200 truncate select-all">
                    {emailAddress}
                  </span>
                  
                  <button
                    onClick={handleCopyEmail}
                    className="ml-2 p-1.5 hover:bg-gray-200 dark:hover:bg-slate-700 rounded-md transition-colors text-gray-500 dark:text-slate-400 flex-shrink-0"
                    title="Copy email address"
                  >
                    {copied ? (
                      <Check className="w-4 h-4 text-green-500" />
                    ) : (
                      <Copy className="w-4 h-4" />
                    )}
                  </button>
                </div>

                {/* Added Google Form Section */}
                <div className="mt-3 pt-3 border-t border-gray-100 dark:border-slate-700">
                  <p className="text-xs text-gray-500 dark:text-slate-400 mb-2 font-medium px-1">Or fill out our form:</p>
                  <a 
                    href="https://forms.gle/rYJ5BWJCB5mXsbgK6" 
                    target="_blank" 
                    rel="noopener noreferrer"
                    className="flex items-center justify-center gap-2 w-full bg-blue-50 hover:bg-blue-100 dark:bg-blue-900/30 dark:hover:bg-blue-900/50 text-blue-600 dark:text-blue-400 text-sm font-medium py-2 px-3 rounded-lg transition-colors border border-blue-100 dark:border-blue-800/50"
                  >
                    <ExternalLink className="w-4 h-4" />
                    Submit Google Form
                  </a>
                </div>

              </div>
            )}
          </div>

          <div className="w-px h-4 sm:h-5 bg-gray-300 dark:bg-slate-700 hidden sm:block shrink-0"></div>

          <a href="https://github.com/saket-sontakke/Thermal-Aware-HPC-Simulator-and-Workload-Scheduler.git" target="_blank" rel="noopener noreferrer" className="text-gray-500 hover:text-gray-900 dark:text-slate-400 dark:hover:text-white transition-colors shrink-0">
            <GithubIcon className="w-5 h-5 sm:w-7 sm:h-7" />
          </a>
          
          <button onClick={onToggleTheme} className="p-1.5 sm:p-2 bg-gray-200 dark:bg-slate-800 rounded-lg text-gray-700 dark:text-slate-300 hover:bg-gray-300 dark:hover:bg-slate-700 transition-colors shrink-0">
            {theme === 'dark' ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
          </button>
          
        </div>
      </header>

      <main className="flex-1 flex flex-col justify-center p-4 py-8 sm:p-6 lg:p-8 w-full max-w-[90rem] mx-auto">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 lg:gap-12 xl:gap-16 items-center w-full max-w-7xl mx-auto my-auto">
          
          <div className="space-y-5 sm:space-y-6 text-left lg:ml-0 xl:ml-7 mt-4 lg:-mt-14">
            <h2 className="text-3xl sm:text-4xl md:text-5xl lg:text-[3rem] xl:text-[3.5rem] font-extrabold tracking-tight leading-[1.15]">
              HPC Simulator and Thermal-Aware Workload Scheduler
            </h2>
            <div className="pt-2">
              <button 
                onClick={() => onNavigate('CONFIG')} 
                className="group inline-flex items-center justify-center w-full sm:w-auto gap-2 sm:gap-3 bg-blue-600 hover:bg-blue-700 text-white font-bold text-base sm:text-lg px-6 sm:px-8 py-3.5 sm:py-4 rounded-2xl shadow-lg shadow-blue-500/30 transition-all hover:-translate-y-1"
              >
                Launch Dashboard 
                <ChevronRight className="w-5 h-5 sm:w-6 sm:h-6 group-hover:translate-x-1 transition-transform" />
              </button>
            </div>
          </div>

          <div className="flex flex-col gap-3 sm:gap-4 w-full">
            
            <button 
              onClick={() => onNavigate('PHYSICS')} 
              className="group flex flex-col sm:flex-row items-start sm:items-center gap-3 sm:gap-4 text-left bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 hover:border-amber-400 dark:hover:border-amber-500 p-4 sm:p-5 rounded-2xl shadow-sm hover:shadow-md transition-all w-full"
            >
              <div className="bg-amber-100 dark:bg-amber-900/30 p-3 rounded-xl text-amber-600 dark:text-amber-400 group-hover:scale-110 transition-transform shrink-0 self-start sm:self-auto">
                <TbMathFunction className="w-6 h-6" />
              </div>
              <div>
                <h3 className="text-base sm:text-lg font-bold mb-1">Physics & ODE Engine</h3>
                <p className="text-gray-600 dark:text-slate-400 text-sm leading-snug">Deep dive into the two-mass thermal model equations and how the simulation calculates temperature delta.</p>
              </div>
            </button>

            <button 
              onClick={() => onNavigate('PREPROCESSING')} 
              className="group flex flex-col sm:flex-row items-start sm:items-center gap-3 sm:gap-4 text-left bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 hover:border-indigo-400 dark:hover:border-indigo-500 p-4 sm:p-5 rounded-2xl shadow-sm hover:shadow-md transition-all w-full"
            >
              <div className="bg-indigo-100 dark:bg-indigo-900/30 p-3 rounded-xl text-indigo-600 dark:text-indigo-400 group-hover:scale-110 transition-transform shrink-0 self-start sm:self-auto">
                <Filter className="w-6 h-6" />
              </div>
              <div>
                <h3 className="text-base sm:text-lg font-bold mb-1">Data Preprocessing</h3>
                <p className="text-gray-600 dark:text-slate-400 text-sm leading-snug">Pipeline from raw telemetry CSVs to PyTorch-ready ODE tensors and PINN prior extraction.</p>
              </div>
            </button>

            <button 
              onClick={() => onNavigate('TRAINING')} 
              className="group flex flex-col sm:flex-row items-start sm:items-center gap-3 sm:gap-4 text-left bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 hover:border-emerald-400 dark:hover:border-emerald-500 p-4 sm:p-5 rounded-2xl shadow-sm hover:shadow-md transition-all w-full"
            >
              <div className="bg-emerald-100 dark:bg-emerald-900/30 p-3 rounded-xl text-emerald-600 dark:text-emerald-400 group-hover:scale-110 transition-transform shrink-0 self-start sm:self-auto">
                <ChartLine className="w-6 h-6" />
              </div>
              <div>
                <h3 className="text-base sm:text-lg font-bold mb-1">Model Calibration</h3>
                <p className="text-gray-600 dark:text-slate-400 text-sm leading-snug">Explore how we utilized PyTorch and gradient descent to fit our ODE parameters to real-world hardware.</p>
              </div>
            </button>

            <button 
              onClick={() => onNavigate('DATASET')} 
              className="group flex flex-col sm:flex-row items-start sm:items-center gap-3 sm:gap-4 text-left bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 hover:border-purple-400 dark:hover:border-purple-500 p-4 sm:p-5 rounded-2xl shadow-sm hover:shadow-md transition-all w-full"
            >
              <div className="bg-purple-100 dark:bg-purple-900/30 p-3 rounded-xl text-purple-600 dark:text-purple-400 group-hover:scale-110 transition-transform shrink-0 self-start sm:self-auto">
                <Database className="w-6 h-6" />
              </div>
              <div>
                <h3 className="text-base sm:text-lg font-bold mb-1">MIT Supercloud Dataset</h3>
                <p className="text-gray-600 dark:text-slate-400 text-sm leading-snug">Learn about the MIT Supercloud dataset that made this project possible, featuring dual-V100 telemetry.</p>
              </div>
            </button>
            
          </div>

        </div>
      </main>
    </div>
  );
}