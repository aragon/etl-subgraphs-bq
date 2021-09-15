import os
from pathlib import Path

def correct_env_var(env_var_name):
    env_var = os.getenv(env_var_name)
    if '~' in env_var:
        # As it's relative path to home, reset env var
        env_var_new = Path(env_var).expanduser().as_posix()
        os.environ[env_var_name] = env_var_new
