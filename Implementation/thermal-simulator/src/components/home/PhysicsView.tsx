"use client";

import React, { useState, useEffect } from 'react';
import { ArrowLeft, Cpu, BadgeCheck, BookOpenCheck, ChevronDown, ChevronUp, Sun, Moon, Ruler, Info, FastForward } from 'lucide-react';
import { TbMathFunction } from "react-icons/tb";
import { PiFanFill } from "react-icons/pi";
import 'katex/dist/katex.min.css';
import { BlockMath, InlineMath } from 'react-katex';

interface PhysicsViewProps {
  theme: 'dark' | 'light';
  onToggleTheme: () => void;
  onGoHome: () => void;
}

const variableUnits = [
  { symbol: 'T_{die, sink}', name: 'Temperature', unit: '^\\circ\\text{C}' },
  { symbol: 't', name: 'Time', unit: '\\text{s}' },
  { symbol: 'P_{self, adj}', name: 'Power Input', unit: '\\text{W} \\quad (\\text{J/s})' },
  { symbol: 'q_{0, 1}', name: 'Ambient Heat Transfer', unit: '\\text{W}' },
  { symbol: 'C_{die, sink}', name: 'Heat Capacity', unit: '\\text{J}/^\\circ\\text{C}' },
  { symbol: 'R_{paste}', name: 'Thermal Resistance', unit: '^\\circ\\text{C}/\\text{W}' },
  { symbol: 'h_{base, act}', name: 'Cooling Coefficient', unit: '\\text{W}/^\\circ\\text{C}' },
  { symbol: 'k_{01, 10}', name: 'Thermal Crosstalk', unit: '\\text{Unitless}' },
  { symbol: '\\beta', name: 'Fan Steepness', unit: '1/^\\circ\\text{C}' },
];

const allCalibratedParams = [
  { symbol: 'C_{die}', name: 'Silicon Die Heat Capacity', unit: 'J/°C', gpu0: '8.93', gpu1: '8.87', desc: 'The thermal mass of the silicon chip itself. Under load, it heats up almost instantly.' },
  { symbol: 'C_{sink}', name: 'Heatsink Heat Capacity', unit: 'J/°C', gpu0: '4713.59', gpu1: '4831.15', desc: 'The massive thermal capacity of the metal cooling block and chassis. Takes much longer to heat up and cool down.' },
  { symbol: 'R_{paste}', name: 'Thermal Resistance', unit: '°C/W', gpu0: '0.0366', gpu1: '0.0336', desc: 'The resistance to heat flow through the thermal paste joining the die to the heat sink.' },
  { symbol: 'h_{base}', name: 'Passive Cooling Rate', unit: 'W/°C', gpu0: '3.98', gpu1: '4.76', desc: 'Baseline convective cooling inner chassis at minimum idle speeds.' },
  { symbol: 'h_{active}', name: 'Max Active Cooling', unit: 'W/°C', gpu0: '20.91', gpu1: '19.82', desc: 'Additional convective cooling applied at 100% duty cycle.' },
  { symbol: 'T_{thresh}', name: 'Fan Activation Temp', unit: '°C', gpu0: '70.25', gpu1: '67.51', desc: 'Temperature threshold die where BIOS begins spinning up cooling fans.' },
  { symbol: '\\beta', name: 'Fan Curve Steepness', unit: '1/°C', gpu0: '1.67', gpu1: '1.34', desc: 'Slope/aggression of the sigmoid fan curve. Higher values mean steeper ramp up.' },
  { symbol: 'k', name: 'Crosstalk Coefficient', unit: 'Unitless', gpu0: '0.0167', gpu1: '0.0028', desc: 'Adjacent GPU power draw parasitic bleed over.' },
  { symbol: 'q', name: 'Ambient Heat Transfer', unit: 'W', gpu0: '-8.92', gpu1: '-8.94', desc: 'Static environmental heat transfer bias compensating external chassis factors.' },
];

const dieDerivation = String.raw`
\begin{aligned}
\frac{dT_{die}}{dt} &= \frac{P_{self} - \frac{T_{die} - T_{sink}}{R_{paste}}}{C_{die}} \\[1em]
\left[ \frac{^\circ\text{C}}{\text{s}} \right] &= \frac{[\text{W}] - \frac{[^\circ\text{C}]}{[^\circ\text{C}/\text{W}]}}{[\text{J}/^\circ\text{C}]} \\[1em]
\left[ \frac{^\circ\text{C}}{\text{s}} \right] &= \frac{[\text{W}] - [\text{W}]}{[\text{J}/^\circ\text{C}]} \\[1em]
\left[ \frac{^\circ\text{C}}{\text{s}} \right] &= \frac{[\text{W}]}{[\text{J}/^\circ\text{C}]} \\[1em]
\left[ \frac{^\circ\text{C}}{\text{s}} \right] &= \frac{[\text{J/s}]}{[\text{J}/^\circ\text{C}]} \\[1em]
\left[ \frac{^\circ\text{C}}{\text{s}} \right] &= \left[ \frac{^\circ\text{C}}{\text{s}} \right] \quad \textcolor{#10b981}{\text{\LARGE \checkmark}}
\end{aligned}
`;

const sinkDerivation = String.raw`
\begin{aligned}
\frac{dT_{sink}}{dt} &= \frac{\frac{T_{die} - T_{sink}}{R_{paste}} + k \cdot P_{adj} - h_{fan}(T_{sink} - T_{amb}) + q}{C_{sink}} \\[1em]
\left[ \frac{^\circ\text{C}}{\text{s}} \right] &= \frac{\frac{[^\circ\text{C}]}{[^\circ\text{C}/\text{W}]} + [\text{Unitless}] \cdot [\text{W}] - [\text{W}/^\circ\text{C}]([^\circ\text{C}]) + [\text{W}]}{[\text{J}/^\circ\text{C}]} \\[1em]
\left[ \frac{^\circ\text{C}}{\text{s}} \right] &= \frac{[\text{W}] + [\text{W}] - [\text{W}] + [\text{W}]}{[\text{J}/^\circ\text{C}]} \\[1em]
\left[ \frac{^\circ\text{C}}{\text{s}} \right] &= \frac{[\text{W}]}{[\text{J}/^\circ\text{C}]} \\[1em]
\left[ \frac{^\circ\text{C}}{\text{s}} \right] &= \frac{[\text{J/s}]}{[\text{J}/^\circ\text{C}]} \\[1em]
\left[ \frac{^\circ\text{C}}{\text{s}} \right] &= \left[ \frac{^\circ\text{C}}{\text{s}} \right] \quad \textcolor{#10b981}{\text{\LARGE \checkmark}}
\end{aligned}
`;

export default function PhysicsView({ theme, onToggleTheme, onGoHome }: PhysicsViewProps) {
  const [showProofs, setShowProofs] = useState(false);

  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);

  return (
    <div className="flex-1 overflow-y-auto custom-scrollbar p-4 sm:p-6 md:p-8">
      <div className="max-w-7xl mx-auto w-full space-y-6 sm:space-y-8 pb-12">
        
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
              <h1 className="text-2xl sm:text-3xl font-bold tracking-tight">Physics & ODE Engine</h1>
              <p className="text-sm sm:text-base text-gray-500 dark:text-slate-400 mt-1">The mathematical foundation of the ThermalODE digital twin.</p>
            </div>
          </div>
          
          <button 
            onClick={onToggleTheme} 
            className="p-2 mt-1 sm:mt-0 bg-gray-200 dark:bg-slate-800 rounded-lg text-gray-700 dark:text-slate-300 hover:bg-gray-300 dark:hover:bg-slate-700 transition-colors shrink-0"
          >
            {theme === 'dark' ? <Sun className="w-4 h-4"/> : <Moon className="w-4 h-4"/>}
          </button>
        </div>

        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 text-amber-900 dark:text-amber-100 text-justify">
          <p className="text-base sm:text-lg leading-relaxed">
            Standard datacenter schedulers are &quot;Thermally blind&quot;, meaning they assign workloads without knowing if a node is about to overheat. 
            To create a proactive scheduler, a <strong>Physics-Informed Ordinary Differential Equation (ODE)</strong> was developed. 
            By modeling the physical heat transfer between the GPU silicon die, the heat sink, and the ambient air, the simulator can 
            look into the future and predict overheating events before they occur.
          </p>
        </div>

        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm">
          <div className="flex items-center gap-3 mb-4 sm:mb-6">
            <div className="bg-amber-100 dark:bg-amber-900/30 p-2 rounded-lg shrink-0">
              <Ruler className="w-5 h-5 sm:w-6 sm:h-6 text-amber-600 dark:text-amber-400" />
            </div>
            <h2 className="text-lg sm:text-xl font-bold leading-tight">Units Dictionary</h2>
          </div>
          <p className="text-gray-600 dark:text-slate-400 text-sm leading-relaxed mb-6 text-justify">
             To prove dimensional consistency, the units for all system variables and calibrated parameters must be established. These constants have been calibrated directly to real-world server hardware (dual NVIDIA V100s).
          </p>
          
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 sm:gap-5">
            {variableUnits.map(v => (
               <div key={v.name} className="flex items-center p-3 sm:p-4 bg-gray-50 dark:bg-slate-950 border border-gray-200 dark:border-slate-800 rounded-xl h-full shadow-sm hover:shadow-md transition-shadow">
                   <div className="min-w-[4.5rem] sm:min-w-[5rem] flex justify-center text-base sm:text-lg text-amber-600 dark:text-amber-400 shrink-0 px-1">
                        <div className="w-full text-center overflow-visible">
                            <InlineMath math={v.symbol} />
                        </div>
                    </div>
                   <div className="w-px h-8 sm:h-10 bg-gray-200 dark:bg-slate-700 mx-2 sm:mx-3 shrink-0"></div>
                   <div className="flex-1 flex flex-row items-center justify-between overflow-hidden gap-2">
                       <span className="text-xs sm:text-sm font-bold text-gray-800 dark:text-slate-200 leading-tight truncate">{v.name}</span>
                       <span className="text-[10px] sm:text-[11px] font-mono font-bold text-gray-600 dark:text-slate-400 bg-white dark:bg-slate-900 px-1.5 sm:px-2 py-1 rounded shadow-sm border border-gray-200 dark:border-slate-700 whitespace-nowrap shrink-0">
                         <InlineMath math={v.unit} />
                       </span>
                   </div>
               </div>
            ))}
          </div>
        </div>

        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm overflow-hidden">
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-6">
            <div className="flex items-center gap-3">
                <div className="bg-amber-100 dark:bg-amber-900/30 p-2 rounded-lg shrink-0">
                  <TbMathFunction className="w-5 h-5 sm:w-6 sm:h-6 text-amber-600 dark:text-amber-400" />
                </div>
                <h2 className="text-lg sm:text-xl font-bold leading-tight">Two-Mass System ODEs</h2>
            </div>
            <button 
              onClick={() => setShowProofs(!showProofs)} 
              className="p-2 sm:px-4 sm:py-2 border border-gray-200 dark:border-slate-700 rounded-full hover:bg-gray-100 dark:hover:bg-slate-800 flex items-center justify-center gap-2 text-amber-600 dark:text-amber-400 transition-colors w-full md:w-auto"
            >
              <BookOpenCheck className="w-4 h-4 shrink-0" />
              <span className="text-xs sm:text-sm font-semibold pr-1 truncate">{showProofs ? "Hide Dimensional Proofs" : "Verify Dimensional Consistency"}</span>
              {showProofs ? <ChevronUp className="w-4 h-4 shrink-0" /> : <ChevronDown className="w-4 h-4 shrink-0" />}
            </button>
          </div>
          
          <p className="text-gray-600 dark:text-slate-400 text-sm leading-relaxed mb-6 text-justify">
            Each GPU was modelled as two connected thermal masses: the silicon die (where heat is generated) and the massive heat sink (where heat is dissipated into the chassis). Heat flows from the die to the sink based on thermal resistance, and escapes the sink via active convection.
          </p>
          
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 pt-2">
              <div className="bg-gray-50 dark:bg-slate-950 rounded-xl p-4 sm:p-6 border border-gray-100 dark:border-slate-800 flex flex-col justify-start text-gray-800 dark:text-slate-200 shadow-inner w-full">
                <div className="text-gray-400 dark:text-slate-500 mb-4 sm:mb-6 text-[10px] sm:text-xs uppercase tracking-wider font-sans font-bold text-center w-full">Die Temperature Derivative</div>
                <div className="py-2 overflow-x-auto w-full custom-scrollbar">
                  <BlockMath math="\frac{dT_{die}}{dt} = \frac{P_{self} - \frac{T_{die} - T_{sink}}{R_{paste}}}{C_{die}}" />
                </div>
                {showProofs && (
                  <div className="mt-6 sm:mt-8 pt-4 sm:pt-6 border-t border-gray-200 dark:border-slate-800 font-mono text-[0.85rem] sm:text-[0.95rem] w-full overflow-x-auto custom-scrollbar animate-in fade-in slide-in-from-top-2 duration-300">
                    <BlockMath math={dieDerivation} />
                  </div>
                )}
              </div>

              <div className="bg-gray-50 dark:bg-slate-950 rounded-xl p-4 sm:p-6 border border-gray-100 dark:border-slate-800 flex flex-col justify-start text-gray-800 dark:text-slate-200 shadow-inner w-full">
                <div className="text-gray-400 dark:text-slate-500 mb-4 sm:mb-6 text-[10px] sm:text-xs uppercase tracking-wider font-sans font-bold text-center w-full">Sink Temperature Derivative</div>
                <div className="py-2 overflow-x-auto w-full custom-scrollbar">
                  <BlockMath math="\frac{dT_{sink}}{dt} = \frac{\frac{T_{die} - T_{sink}}{R_{paste}} + k \cdot P_{adj} - h_{fan}(T_{sink} - T_{amb}) + q}{C_{sink}}" />
                </div>
                {showProofs && (
                  <div className="mt-6 sm:mt-8 pt-4 sm:pt-6 border-t border-gray-200 dark:border-slate-800 font-mono text-[0.85rem] sm:text-[0.95rem] w-full overflow-x-auto custom-scrollbar animate-in fade-in slide-in-from-top-2 duration-300">
                    <BlockMath math={sinkDerivation} />
                  </div>
                )}
              </div>
          </div>
        </div>

        {/* Mechanics Section */}
        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm overflow-hidden">
          <div className="flex items-center gap-3 mb-6">
              <div className="bg-amber-100 dark:bg-amber-900/30 p-2 rounded-lg shrink-0">
                <FastForward className="w-5 h-5 sm:w-6 sm:h-6 text-amber-600 dark:text-amber-400" />
              </div>
              <h2 className="text-lg sm:text-xl font-bold leading-tight">Simulation Mechanics</h2>
          </div>
          
          <p className="text-gray-600 dark:text-slate-400 text-sm leading-relaxed mb-6 text-justify">
            While the ODEs describe the instantaneous rate of change, the digital twin must simulate actual time passing. This requires establishing a starting physical state at <InlineMath math="t=0" /> and stepping the equations forward using numerical integration.
          </p>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6 pt-2">
              <div className="bg-gray-50 dark:bg-slate-950 rounded-xl p-4 sm:p-6 border border-gray-100 dark:border-slate-800 flex flex-col justify-start text-gray-800 dark:text-slate-200 shadow-inner w-full h-full">
                <h3 className="text-gray-900 dark:text-white font-bold text-sm mb-2">Initial Steady-State</h3>
                <p className="text-xs text-gray-600 dark:text-slate-400 mb-4 leading-relaxed">
                  Calculates the unobserved heatsink temperature at sequence start by assuming initial thermal equilibrium through the paste.
                </p>
                <div className="py-2 overflow-x-auto w-full custom-scrollbar bg-white dark:bg-slate-900 rounded-lg border border-gray-200 dark:border-slate-800">
                  <BlockMath math="T_{sink}(0) = T_{die}(0) - (P(0) \cdot R_{paste})" />
                </div>
              </div>

              <div className="bg-gray-50 dark:bg-slate-950 rounded-xl p-4 sm:p-6 border border-gray-100 dark:border-slate-800 flex flex-col justify-start text-gray-800 dark:text-slate-200 shadow-inner w-full h-full">
                <h3 className="text-gray-900 dark:text-white font-bold text-sm mb-2">Forward Euler Integration</h3>
                <p className="text-xs text-gray-600 dark:text-slate-400 mb-4 leading-relaxed">
                  Advances the simulation by discretizing time into steps of <InlineMath math="\Delta t = 0.11" /> seconds.
                </p>
                <div className="py-2 overflow-x-auto w-full custom-scrollbar bg-white dark:bg-slate-900 rounded-lg border border-gray-200 dark:border-slate-800 flex flex-col gap-2">
                  <BlockMath math="T_{die}(t+\Delta t) = T_{die}(t) + \Delta t \cdot \frac{dT_{die}}{dt}" />
                  <BlockMath math="T_{sink}(t+\Delta t) = T_{sink}(t) + \Delta t \cdot \frac{dT_{sink}}{dt}" />
                </div>
              </div>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm flex flex-col w-full overflow-hidden">
              <div className="flex items-center gap-3 mb-4">
                <div className="bg-amber-100 dark:bg-amber-900/30 p-2 rounded-lg shrink-0">
                  <PiFanFill className="w-5 h-5 sm:w-6 sm:h-6 text-amber-600 dark:text-amber-400" />
                </div>
                <h2 className="text-lg sm:text-xl font-bold leading-tight">Non-Linear Fan Curve</h2>
              </div>
              <p className="text-gray-600 dark:text-slate-400 mb-4 text-sm flex-1 leading-relaxed text-justify">
                Server fans are not linear. The Sigmoid function <span className="inline-block overflow-visible whitespace-nowrap"><InlineMath math="\sigma(x) = \frac{1}{1+e^{-x}}" /></span> elegantly models the realistic threshold-based ramp-up inside dense chassis nodes. <span className="inline-block overflow-visible whitespace-nowrap"><InlineMath math="\sigma" /></span> is dimensionless and its output scales between 0 and 1.
              </p>
              <div className="bg-gray-50 dark:bg-slate-950 rounded-xl p-3 sm:p-4 border border-gray-100 dark:border-slate-800 text-gray-800 dark:text-slate-200 overflow-x-auto w-full text-base sm:text-lg custom-scrollbar">
                <BlockMath math="h_{fan} = h_{base} + h_{active} \cdot \sigma(\beta(T_{die} - T_{thresh}))" />
              </div>
            </div>

            <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm flex flex-col h-full w-full">
              <div className="flex items-center gap-3 mb-4">
                <div className="bg-amber-100 dark:bg-amber-900/30 p-2 rounded-lg shrink-0">
                  <Info className="w-5 h-5 sm:w-6 sm:h-6 text-amber-600 dark:text-amber-400" />
                </div>
                <h2 className="text-lg sm:text-xl font-bold leading-tight truncate">Why isn&apos;t <InlineMath math="\sigma" /> in parameters?</h2>
              </div>
              <p className="leading-relaxed text-gray-600 dark:text-slate-400 text-sm text-justify">
                  Unlike <span className="inline-block overflow-visible whitespace-nowrap align-bottom"><InlineMath math="C_{sink}" /></span> or <span className="inline-block overflow-visible whitespace-nowrap align-bottom"><InlineMath math="h_{base}" /></span>, which are physical properties of the NVIDIA V100 hardware and server chassis that we calibrate during optimization, <span className="inline-block overflow-visible whitespace-nowrap align-bottom"><InlineMath math="\sigma" /></span> (the Sigmoid operator) is a <strong>fixed mathematical function</strong> of the ODE itself. It acts as an dimensional switch for the fan curve. Since it has no calibrated value or physical units, it is excluded from the calibrated parameters below.
              </p>
            </div>
        </div>

        <div className="bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm">
          <div className="flex items-center gap-3 mb-6">
            <div className="bg-amber-100 dark:bg-amber-900/30 p-2 rounded-lg shrink-0">
              <Cpu className="w-5 h-5 sm:w-6 sm:h-6 text-amber-600 dark:text-amber-400" />
            </div>
            <h2 className="text-lg sm:text-xl font-bold leading-tight">Calibrated Parameters</h2>
          </div>
          <div className="space-y-6 mb-8 w-full">
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                {allCalibratedParams.map(p => <ParameterCard key={p.symbol} {...p} />)}
              </div>
          </div>

          <div className="bg-gray-50 dark:bg-slate-950 border border-gray-100 dark:border-slate-800 rounded-xl p-4 sm:p-6 mt-6 sm:mt-10 flex flex-col sm:flex-row gap-4 items-start w-full">
             <BadgeCheck className="w-8 h-8 sm:w-10 sm:h-10 text-emerald-600 dark:text-emerald-400 shrink-0 mt-1" />
             <div>
                <h3 className="font-bold text-base sm:text-lg mb-2 text-gray-800 dark:text-slate-200">Model Physical Credibility Proof</h3>
                <p className="leading-relaxed text-gray-600 dark:text-slate-400 text-sm text-justify">
                  The credibility of this physics engine rests on two mathematical pillars. First, the <strong>dimensional consistency</strong> of the ODEs ensures the mathematical framework strictly obeys the laws of thermodynamics, safely converting power directly into accurate temperature rates.
                  <br/><br/>
                  Second, a striking similarity exists between the calibrated values for GPU 0 and GPU 1. Because these are physically identical NVIDIA V100 accelerators sitting in the same chassis node, their true physical thermal masses (<span className="inline-block overflow-visible whitespace-nowrap align-bottom"><InlineMath math="C" /></span>) and paste resistances (<span className="inline-block overflow-visible whitespace-nowrap align-bottom"><InlineMath math="R" /></span>) are inherently near-identical. 
                  The fact that the gradient descent optimization independently converged on nearly matching physical profiles for both independent GPUs, without being forced to do so, serves as strong evidence that we have successfully modeled the <strong>true underlying thermodynamics</strong> of the server hardware, rather than relying on arbitrary statistical fits.
                </p>
             </div>
          </div>
        </div>

      </div>
    </div>
  );
}

function ParameterCard({ symbol, name, unit, gpu0, gpu1, desc }: { symbol: string, name: string, unit: string, gpu0: string, gpu1: string, desc: string }) {
  return (
    <div className="p-4 bg-gray-200/60 dark:bg-slate-950/60 rounded-xl border border-gray-300/50 dark:border-slate-800 flex flex-col h-full text-center hover:shadow-md transition-shadow">
      <div className="flex justify-between items-start mb-2 text-left w-full overflow-hidden gap-2">
        <span className="text-base sm:text-lg text-amber-600 dark:text-amber-400 shrink-0 max-w-[60%] overflow-visible whitespace-nowrap">
          <InlineMath math={symbol} />
        </span>
        {unit && <span className="text-[10px] sm:text-xs font-bold text-gray-500 bg-white dark:bg-slate-900 px-1.5 py-0.5 rounded shadow-inner whitespace-nowrap border border-gray-200 dark:border-slate-700/50 shrink-0 mt-1">{unit}</span>}
      </div>
      <h3 className="font-bold text-gray-800 dark:text-slate-200 mb-2 text-left text-sm sm:text-base leading-tight">{name}</h3>
      <p className="text-xs text-gray-500 dark:text-slate-400 flex-1 mb-4 leading-relaxed text-left">{desc}</p>
      
      <div className="flex flex-row justify-between items-center text-sm font-mono pt-3 border-t border-gray-300/50 dark:border-slate-700/50">
        <div className="flex flex-col text-left">
          <span className="text-[9px] sm:text-[10px] text-gray-400 uppercase tracking-wide block">GPU 0</span>
          <span className="text-gray-800 dark:text-slate-200 font-bold text-xs sm:text-sm">{gpu0}</span>
        </div>
        <div className="w-px h-6 bg-gray-300 dark:bg-slate-700 shrink-0 mx-2"></div>
        <div className="flex flex-col text-right">
          <span className="text-[9px] sm:text-[10px] text-gray-400 uppercase tracking-wide block">GPU 1</span>
          <span className="text-gray-800 dark:text-slate-200 font-bold text-xs sm:text-sm">{gpu1}</span>
        </div>
      </div>
    </div>
  );
}