"use client";

import React from 'react';
import { ChevronUp, ChevronDown } from 'lucide-react';

export const ThemeableNumberInput = ({ value, onChange, onBlur, min, max }: any) => {
  const handleIncrement = () => {
    let val = typeof value === 'number' ? value : min;
    if (val < max) onChange({ target: { value: String(val + 1) } });
  };
  
  const handleDecrement = () => {
    let val = typeof value === 'number' ? value : min;
    if (val > min) onChange({ target: { value: String(val - 1) } });
  };

  return (
    <div className="relative w-full">
      <input 
        type="number" 
        value={value} 
        onChange={onChange} 
        onBlur={onBlur} 
        className="w-full bg-gray-50 dark:bg-slate-800 border border-gray-200 dark:border-slate-700 rounded-lg pl-4 pr-10 py-2 outline-none focus:ring-2 focus:ring-blue-500"
      />
      <div className="absolute right-1 top-1 bottom-1 flex flex-col justify-between w-7 rounded bg-gray-100 dark:bg-slate-800 border border-transparent overflow-hidden">
        <button type="button" onClick={handleIncrement} className="flex-1 flex items-center justify-center hover:bg-gray-200 dark:hover:bg-slate-700 text-gray-500 dark:text-slate-400 transition-colors">
          <ChevronUp className="w-4 h-4" />
        </button>
        <button type="button" onClick={handleDecrement} className="flex-1 flex items-center justify-center hover:bg-gray-200 dark:hover:bg-slate-700 text-gray-500 dark:text-slate-400 transition-colors">
          <ChevronDown className="w-4 h-4" />
        </button>
      </div>
    </div>
  );
};