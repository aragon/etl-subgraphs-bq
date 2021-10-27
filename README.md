## Intro
This repo aims to fetch data in an automatic fashion from `Covalent` and `The Graph`.

It depends on the following main scrips:
- `main.py`: used for `The Graph` queries
- `main_covalent`: used for simple `Covalent` queries
- `main_covalent_multi`: used for simple `Covalent` queries

## Install dependencies


## Run locally
The following statemenst will use `ENV_VARS_PATH` from script, also you can add `--env_vars ./env_vars/polygon_client_daos.env`
- `python main_covalent_multi.py --no-testing --check_last_block`
- `python main_covalent.py --no-testing --check_last_block`


