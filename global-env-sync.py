#!/usr/bin/env python3
"""
Sync Global Environment Variables Script

This script syncs variables from stacks/global.env to each service's .env file
- Updates existing variables to match global values
- Adds missing global variables
- Preserves service-specific variables
- Creates .env file if it doesn't exist
"""

import os
import re
import sys
import tempfile
import shutil
from pathlib import Path
from typing import Dict, List, Tuple, Optional


def parse_env_file(file_path: Path) -> Tuple[Dict[str, str], List[str]]:
    """Parse an .env file and return a dict of variables and their order."""
    vars_dict = {}
    var_order = []
    
    if not file_path.exists():
        return vars_dict, var_order
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            
            # Parse variable (handle inline comments)
            # Pattern: VAR_NAME=value # comment
            match = re.match(r'^([^#=]+?)=(.*?)(?:\s*#.*)?$', line)
            if match:
                var_name = match.group(1).strip()
                var_value = match.group(2).strip()
                
                # Remove quotes if present
                if var_value.startswith('"') and var_value.endswith('"'):
                    var_value = var_value[1:-1]
                elif var_value.startswith("'") and var_value.endswith("'"):
                    var_value = var_value[1:-1]
                
                vars_dict[var_name] = var_value
                var_order.append(var_name)
    
    return vars_dict, var_order


def parse_existing_env(file_path: Path, global_vars: Dict[str, str]) -> Tuple[Dict[str, str], Dict[str, str], List[str], List[str]]:
    """
    Parse existing .env file and separate global vars, service-specific vars, and header.
    Returns: (existing_vars, existing_comments, service_specific_vars, file_header)
    """
    existing_vars = {}
    existing_comments = {}
    service_specific_vars = []
    file_header = []
    in_header = True
    
    if not file_path.exists():
        return existing_vars, existing_comments, service_specific_vars, file_header
    
    in_global_section = False
    in_service_section = False
    seen_global_marker = False
    seen_service_marker = False
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            original_line = line
            trimmed = line.strip()
            
            # Skip duplicate section markers
            if trimmed == "# Global Variables (from stacks/global.env)":
                if not seen_global_marker:
                    in_header = False
                    in_global_section = True
                    seen_global_marker = True
                continue  # Skip all instances, we'll add it back later
            
            if trimmed == "# Service-Specific Variables":
                if not seen_service_marker:
                    in_global_section = False
                    in_service_section = True
                    seen_service_marker = True
                continue  # Skip all instances, we'll add it back later
            
            # Collect header comments (only before first section marker)
            if in_header and (trimmed.startswith('#') or not trimmed):
                file_header.append(line.rstrip('\n'))
                if trimmed and trimmed.startswith('#'):
                    in_header = True
                continue
            
            # Once we've seen a section marker, we're out of header
            if seen_global_marker:
                in_header = False
            
            # Skip empty lines in body
            if not trimmed:
                continue
            
            # Parse variable
            match = re.match(r'^([^#=]+?)=(.*?)(?:\s*#.*)?$', trimmed)
            if match:
                var_name = match.group(1).strip()
                var_value = match.group(2).strip()
                
                # Remove quotes if present
                if var_value.startswith('"') and var_value.endswith('"'):
                    var_value = var_value[1:-1]
                elif var_value.startswith("'") and var_value.endswith("'"):
                    var_value = var_value[1:-1]
                
                # Check if this is a global variable or service-specific
                if var_name in global_vars:
                    # Only collect global vars if we're in the global section
                    if in_global_section or not seen_global_marker:
                        existing_vars[var_name] = var_value
                        # Preserve comment if present
                        comment_match = re.search(r'#\s*(.+)$', trimmed)
                        if comment_match:
                            existing_comments[var_name] = comment_match.group(1).strip()
                else:
                    # Service-specific variable - only collect if we're past global section
                    if in_service_section or seen_global_marker:
                        service_specific_vars.append(original_line.rstrip('\n'))
            else:
                # Non-variable line (comment, etc.) - only preserve if in service section
                # Skip old/orphaned comments from global section
                if in_service_section or (seen_service_marker and not in_global_section):
                    # Only preserve if it's not an old header comment
                    if not (trimmed.startswith('# =================================') or 
                            trimmed.startswith('# Service:') or
                            trimmed.startswith('# Global variables') or
                            trimmed.startswith('# This file is automatically')):
                        service_specific_vars.append(original_line.rstrip('\n'))
    
    return existing_vars, existing_comments, service_specific_vars, file_header


def is_intentional_override(comment: str) -> bool:
    """Check if a comment indicates an intentional override."""
    if not comment:
        return False
    override_keywords = ['different', 'override', 'service-specific', 'custom', 'note']
    return any(keyword.lower() in comment.lower() for keyword in override_keywords)


def sync_service_env(service_dir: Path, global_vars: Dict[str, str], global_var_order: List[str]) -> Tuple[int, int]:
    """Sync global variables to a service's .env file. Returns (updated_count, added_count)."""
    service_name = service_dir.name
    service_env_path = service_dir / '.env'
    
    print(f"\nProcessing: {service_name}", flush=True)
    
    # Parse existing .env file
    existing_vars, existing_comments, service_specific_vars, file_header = parse_existing_env(
        service_env_path, global_vars
    )
    
    if not service_env_path.exists():
        print("  .env file does not exist, will create it", flush=True)
        # Create default header if file doesn't exist
        file_header = [
            "# ============================================",
            f"# Service: {service_name}",
            "# ============================================",
            "# Global variables (synced from stacks/global.env)",
            "# This file is automatically maintained by global-env-sync.py",
            ""
        ]

    # Build new .env file content
    new_content = []

    # Add header (or preserve existing)
    # Filter out any existing "Global Variables" section markers from header
    filtered_header = []
    for header_line in file_header:
        if header_line.strip() != "# Global Variables (from stacks/global.env)":
            filtered_header.append(header_line)

    if filtered_header:
        new_content.extend(filtered_header)
        # Ensure there's a blank line before the Global Variables section
        if filtered_header and filtered_header[-1].strip():
            new_content.append("")
    else:
        new_content.extend([
            "# ============================================",
            f"# Service: {service_name}",
            "# ============================================",
            "# Global variables (synced from stacks/global.env)",
            "# This file is automatically maintained by global-env-sync.py",
            ""
        ])
    
    # Add global variables section (only once)
    new_content.append("# Global Variables (from stacks/global.env)")
    new_content.append("")
    
    updated_count = 0
    added_count = 0
    
    for var_name in global_var_order:
        var_value = global_vars[var_name]
        comment = ""
        preserve_override = False
        
        # Check if variable existed before
        if var_name in existing_vars:
            old_value = existing_vars[var_name]
            
            # Check if this is a service-specific override
            if old_value != var_value and var_name in existing_comments:
                comment_text = existing_comments[var_name]
                # Check if comment indicates this is intentional
                if is_intentional_override(comment_text):
                    print(f"  Preserved override: {var_name} = {old_value} (comment indicates intentional)", flush=True)
                    preserve_override = True
                    var_value = old_value
                    comment = f" # {comment_text}"
                else:
                    print(f"  Updated: {var_name} = {old_value} -> {var_value}", flush=True)
                    updated_count += 1
                    comment = f" # {comment_text}"
            elif old_value != var_value:
                print(f"  Updated: {var_name} = {old_value} -> {var_value}", flush=True)
                updated_count += 1
            else:
                print(f"  Unchanged: {var_name}", flush=True)
            
            # Preserve existing comment if available and not already set
            if not preserve_override and var_name in existing_comments and not comment:
                comment = f" # {existing_comments[var_name]}"
        else:
            print(f"  Added: {var_name} = {var_value}", flush=True)
            added_count += 1
        
        new_content.append(f"{var_name}={var_value}{comment}")
    
    # Add service-specific variables section if any exist
    # Filter out duplicate section markers and old header content
    filtered_service_vars = []
    for var_line in service_specific_vars:
        trimmed = var_line.strip()
        # Skip duplicate section markers
        if trimmed in ["# Service-Specific Variables", "# Global Variables (from stacks/global.env)"]:
            continue
        # Skip old header patterns that shouldn't be in service-specific section
        if trimmed.startswith("# ====================================") or \
           (trimmed.startswith("# Service:") and "Service:" in trimmed) or \
           (trimmed.startswith("# Global variables") and "synced" in trimmed.lower()) or \
           (trimmed.startswith("# This file is automatically")):
            continue
        filtered_service_vars.append(var_line)
    
    if filtered_service_vars:
        new_content.append("")
        new_content.append("# Service-Specific Variables")
        new_content.append("")
        new_content.extend(filtered_service_vars)
    
    # Write the new .env file
    # Use atomic write pattern: write to temp file, then replace original
    # This is more reliable on network shares and avoids file locking issues
    temp_file = None
    try:
        # Check if directory is writable
        if not os.access(service_dir, os.W_OK):
            print(f"  [ERROR] Permission denied: Cannot write to {service_dir}", flush=True)
            print(f"  Skipping {service_name}", flush=True)
            return 0, 0
        
        # Create temp file in the same directory (more reliable on network shares)
        temp_file = service_env_path.with_suffix('.env.tmp')
        
        # Write to temp file first
        try:
            with open(temp_file, 'w', encoding='utf-8', newline='\n') as f:
                content = '\n'.join(new_content)
                if new_content:  # Add trailing newline if file has content
                    content += '\n'
                f.write(content)
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
        except (PermissionError, OSError) as e:
            print(f"  [ERROR] Cannot write temp file: {e}", flush=True)
            print(f"  Skipping {service_name}", flush=True)
            if temp_file and temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass
            return 0, 0
        
        # Replace original file with temp file (atomic operation)
        try:
            # Remove read-only attribute if it exists (Windows)
            if service_env_path.exists():
                try:
                    # On Windows, remove read-only attribute
                    import stat
                    current_mode = service_env_path.stat().st_mode
                    os.chmod(service_env_path, current_mode | stat.S_IWRITE)
                except (OSError, AttributeError, ImportError):
                    pass  # Ignore if chmod doesn't work
            
            # Replace the file using shutil.move (more reliable than replace on network shares)
            if service_env_path.exists():
                service_env_path.unlink()  # Remove old file
            shutil.move(str(temp_file), str(service_env_path))  # Move temp to final location
        except (PermissionError, OSError) as e:
            # Clean up temp file if replace failed
            if temp_file and temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass
            print(f"  [ERROR] Permission denied: Cannot replace {service_env_path}", flush=True)
            print(f"  Error details: {e}", flush=True)
            print(f"  Skipping {service_name}", flush=True)
            return 0, 0
        
        print(f"  [OK] Synced: {updated_count} updated, {added_count} added", flush=True)
        return updated_count, added_count
    except Exception as e:
        # Clean up temp file if it exists
        if temp_file and temp_file.exists():
            try:
                temp_file.unlink()
            except:
                pass
        print(f"  [ERROR] Unexpected error: {e}", flush=True)
        print(f"  Skipping {service_name}", flush=True)
        return 0, 0


def main():
    """Main function."""
    # Get script directory
    script_dir = Path(__file__).parent.resolve()
    global_env_path = script_dir / "global.env"
    
    # Check if global.env exists
    if not global_env_path.exists():
        print(f"Error: Global environment file not found: {global_env_path}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Reading global variables from: {global_env_path}", flush=True)
    
    # Parse global.env file
    global_vars, global_var_order = parse_env_file(global_env_path)
    
    print(f"Found {len(global_vars)} global variables", flush=True)
    
    # Find all service directories
    service_dirs = [
        d for d in script_dir.iterdir()
        if d.is_dir()
        and d.name not in ['dockge', 'containerd']
        and (d / 'compose.yaml').exists()
    ]
    
    print(f"\nFound {len(service_dirs)} service directories", flush=True)
    
    total_updated = 0
    total_added = 0
    failed_services = []
    
    for service_dir in sorted(service_dirs):
        try:
            updated, added = sync_service_env(service_dir, global_vars, global_var_order)
            total_updated += updated
            total_added += added
        except Exception as e:
            service_name = service_dir.name
            print(f"\n[ERROR] Unexpected error processing {service_name}: {e}", flush=True)
            failed_services.append(service_name)
            continue
    
    print("\n" + "=" * 40, flush=True)
    if failed_services:
        print(f"Sync completed with {len(failed_services)} error(s)!", flush=True)
        print(f"Failed services: {', '.join(failed_services)}", flush=True)
    else:
        print("Sync completed successfully!", flush=True)
    print("=" * 40, flush=True)
    print(f"Total: {total_updated} updated, {total_added} added", flush=True)


if __name__ == "__main__":
    main()

