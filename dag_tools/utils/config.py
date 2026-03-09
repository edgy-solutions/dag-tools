import os
from typing import Any, List
from dagster import config_from_files, file_relative_path

def get_config_key(configs: List[str], config_key: str, config_subkey: str, fix_path: bool = True) -> Any:
    """Walks an inheritance hierarchy of configuration files to resolve nested keys.
    
    This function discovers intermediate configuration definitions between a default file 
    and target file, allowing a deeply nested directory to partially override a global config.
    """
    if not configs:
        return {}
        
    default = os.path.abspath(configs[-1])
    basename = os.path.basename(configs[0])
    
    all_configs = []
    
    for config_file in configs[:-1]:
        base = os.path.dirname(os.path.abspath(config_file))
        
        try:
            # Safely determine the fragment difference avoiding string index splits
            relative = os.path.relpath(os.path.dirname(default), base)
            fragments = relative.split(os.sep)
            
            for fragment in fragments[:-1]:
                if fragment and fragment != '.':
                    base = os.path.join(base, fragment)
                    all_configs.append(os.path.join(base, basename))
        except ValueError:
            # Fallback for Windows path drive mismatch parsing
            pass
            
        all_configs.append(config_file)
        
    all_configs.append(default)
    
    for config_file in all_configs:
        if os.path.exists(config_file):
            config = config_from_files([os.path.abspath(config_file)])
            
            if config_key in config:
                if config_subkey not in config[config_key]:
                    return {}
                    
                value = config[config_key][config_subkey]
                # Fix Path ensures paths inherit safely down the config tree, but only strings are allowed
                if fix_path and isinstance(value, str):
                    return os.path.abspath(file_relative_path(config_file, value))
                return value
                
    return {}
