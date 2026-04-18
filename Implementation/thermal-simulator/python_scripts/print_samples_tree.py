"""
===============================================================================
Script Name: print_samples_tree.py
Description: Recursively prints a tree of the directory structure along with 
             subfolder counts (directories and files) attached to each folder.
===============================================================================
"""

import sys
import time
import warnings
from pathlib import Path
from datetime import timedelta

# --- 1. CONFIGURATION ---
warnings.filterwarnings('ignore')

# Set to False because a tree printer typically just logs to terminal/txt, 
# not generating heavy data files.
GENERATES_FILES = False  

# --- 2. TERMINAL LOGGING UTILITY ---
class DualLogger:
    """Intercepts print statements and routes them to both terminal and a text file."""
    def __init__(self, filepath):
        self.terminal = sys.stdout
        self.log = open(filepath, "w", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()


# --- 3. WORKER FUNCTION (Recursive Tree Builder) ---
def get_tree_data(dir_path: Path, prefix: str = "") -> tuple[list, int, int]:
    """
    Recursively builds the tree strings bottom-up.
    Returns a tuple containing (lines_list, directory_count, file_count).
    By building bottom-up, we can append the counts to the parent directory's string.
    """
    dir_count = 0
    file_count = 0
    lines = []
    
    try:
        # Get all entries, sorting directories first, then alphabetically
        entries = sorted(list(dir_path.iterdir()), key=lambda x: (x.is_file(), x.name.lower()))
    except PermissionError:
        return [f"{prefix}└── [Permission Denied]"], 0, 0

    entries_count = len(entries)
    
    for i, entry in enumerate(entries):
        is_last = (i == entries_count - 1)
        connector = "└── " if is_last else "├── "
        extension = "    " if is_last else "│   "
        
        if entry.is_dir():
            # Recurse FIRST to get the counts for this specific subfolder
            sub_lines, sub_dirs, sub_files = get_tree_data(entry, prefix + extension)
            
            # Add counts to total
            dir_count += (1 + sub_dirs)
            file_count += sub_files
            
            # Append the directory name ALONG with its internal counts
            lines.append(f"{prefix}{connector}{entry.name}/ [{sub_dirs} dirs, {sub_files} files]")
            
            # Append the children lines below it
            lines.extend(sub_lines)
        else:
            # It's a file
            file_count += 1
            lines.append(f"{prefix}{connector}{entry.name}")
            
    return lines, dir_count, file_count


# --- 4. MAIN EXECUTION ---
def main():
    # --- Path Resolution ---
    script_path = Path(__file__).resolve()
    global script_name
    script_name = script_path.stem
    
    project_root = script_path.parent.parent 
    scripts_dir = script_path.parent
    
    # Target the public/samples directory based on previous scripts
    data_dir = project_root / "public" / "samples"
    outputs_base_dir = scripts_dir / "outputs" / script_path.parent.name
    
    # --- Dynamic Output Routing ---
    log_dir = outputs_base_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    # --- Initialize Dual Logging ---
    log_path = log_dir / f"{script_name}_terminal_output.txt"
    sys.stdout = DualLogger(log_path)
    
    # --- START TIMER ---
    start_time = time.perf_counter()
    
    print("=" * 70)
    print(f"--- STARTING: {script_name.upper()} ---")
    print("=" * 70)

    # --- Data Discovery ---
    if not data_dir.exists() or not data_dir.is_dir():
        print(f"[ERROR] Target directory does not exist or is not a folder: {data_dir}")
        sys.stdout = sys.stdout.terminal
        return

    print(f"Scanning directory: {data_dir}\n")
    
    # --- Execute Tree Builder ---
    print(f"{data_dir.name}/")
    tree_lines, total_dirs, total_files = get_tree_data(data_dir)
    
    for line in tree_lines:
        print(line)

    # --- END TIMER & CALCULATE ---
    end_time = time.perf_counter()
    elapsed_seconds = end_time - start_time
    formatted_time = str(timedelta(seconds=int(elapsed_seconds)))

    # --- Final Summary ---
    print("\n" + "=" * 70)
    print("=== EXECUTION COMPLETE ===")
    print("=" * 70)
    print(f"Total Subfolders : {total_dirs}")
    print(f"Total Files      : {total_files}")
    print(f"Total Time       : {formatted_time}")
    print(f"\n[!] Terminal logs saved to: {log_path}")

    # Restore standard output just in case
    sys.stdout = sys.stdout.terminal


if __name__ == "__main__":
    main()