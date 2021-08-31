'''
Used when running in local
'''
import yaml
import os
from pathlib import Path

def load_env_vars(env_vars_path):
    env_vars_dict = yaml.load(
        open(env_vars_path),
        Loader=yaml.BaseLoader
    )
    for e_v_name, e_v_value in env_vars_dict.items():
        os.environ[e_v_name] = e_v_value