import os
from pathlib import Path

def print_tree(dir_path: Path, prefix: str = "") -> tuple[int, int]:
    """
    Recursively prints a tree of the directory structure.
    Returns a tuple containing (directory_count, file_count).
    """
    dir_count = 0
    file_count = 0
    
    try:
        # Get all entries, sorting directories first, then alphabetically
        entries = sorted(list(dir_path.iterdir()), key=lambda x: (x.is_file(), x.name.lower()))
    except PermissionError:
        print(f"{prefix}└── [Permission Denied]")
        return 0, 0

    entries_count = len(entries)
    
    for i, entry in enumerate(entries):
        # Check if the current entry is the last one in the folder
        is_last = (i == entries_count - 1)
        
        # Choose the appropriate drawing characters
        connector = "└── " if is_last else "├── "
        
        # Print the current file or folder
        print(f"{prefix}{connector}{entry.name}")
        
        # If it's a directory, recurse into it with an updated prefix
        if entry.is_dir():
            dir_count += 1
            extension = "    " if is_last else "│   "
            
            # Add the counts from the subdirectories
            sub_dirs, sub_files = print_tree(entry, prefix + extension)
            dir_count += sub_dirs
            file_count += sub_files
        else:
            file_count += 1
            
    return dir_count, file_count

if __name__ == "__main__":
    target_folder = Path("samples")
    
    if target_folder.exists() and target_folder.is_dir():
        print(f"{target_folder.name}/")
        
        # Run the tree printer and catch the returned counts
        total_dirs, total_files = print_tree(target_folder)
        
        # Print the final summary
        print("\n" + "="*30)
        print("📊 Directory Summary")
        print("="*30)
        print(f"Total Subfolders: {total_dirs}")
        print(f"Total Files:      {total_files}")
        print("="*30)
    else:
        print(f"Error: The folder '{target_folder}' does not exist in the current directory.")