# src/utils/config_loader.py

import os
import json
import re
from dotenv import load_dotenv

# load environment variables from .env file
load_dotenv()

def load_config_with_env_vars(config_path):
    """
    Load a JSON configuration file and replace environment variable
    placeholders with their actual values.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Configuration dictionary with environment variables resolved
    """
    with open(config_path, 'r') as f:
        config_content = f.read()

    # replace ${ENV_VAR} with actual environment variable value
    def replace_env_var(match):
        env_var = match.group(1)
        return os.environ.get(env_var, '')
    
    # substitute environment variables
    config_content = re.sub(r'\${([^}]+)}', replace_env_var, config_content)

    # parse JSON
    config = json.loads(config_content)

    return config