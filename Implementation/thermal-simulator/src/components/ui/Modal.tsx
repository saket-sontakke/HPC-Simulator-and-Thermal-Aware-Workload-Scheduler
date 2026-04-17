import React from 'react';

interface ModalProps {
  isOpen: boolean;
  title: string;
  children: React.ReactNode;
  actions?: React.ReactNode;
  onClose?: () => void;
}

export default function Modal({ isOpen, title, children, actions, onClose }: ModalProps) {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-[200] flex items-center justify-center p-4">
      {/* Dark semi-transparent overlay */}
      <div 
        className="absolute inset-0 bg-black/60 backdrop-blur-sm transition-opacity" 
        onClick={onClose}
      />
      
      {/* Modal Box */}
      <div className="relative bg-white dark:bg-slate-900 border border-gray-200 dark:border-slate-700 rounded-2xl shadow-2xl w-full max-w-md mx-auto overflow-hidden animate-in fade-in zoom-in-95 duration-200">
        <div className="px-4 sm:px-6 py-3 sm:py-4 border-b border-gray-100 dark:border-slate-800 flex justify-between items-center bg-gray-50 dark:bg-slate-900/50">
          <h3 className="text-base sm:text-lg font-bold text-gray-900 dark:text-white leading-tight">{title}</h3>
          {onClose && (
            <button onClick={onClose} className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors shrink-0 p-1">
              ✕
            </button>
          )}
        </div>
        
        <div className="p-4 sm:p-6 text-sm sm:text-base text-gray-700 dark:text-slate-300">
          {children}
        </div>
        
        {actions && (
          <div className="px-4 sm:px-6 py-3 sm:py-4 border-t border-gray-100 dark:border-slate-800 bg-gray-50 dark:bg-slate-900/50 flex flex-col sm:flex-row justify-end gap-2 sm:gap-3">
            {actions}
          </div>
        )}
      </div>
    </div>
  );
}