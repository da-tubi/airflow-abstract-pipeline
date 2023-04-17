# Pants Airflow Minimal
This repo could be separated into two parts:
+ The Airflow Infra described below
+ The demo Airflow dags
  + [00_get_started.py](python/dags/00_get_started.py)
  + [01_example_branch.py](python/dags/01_example_branch.py) from official Airflow demo
  + [02_brancher.py](python/dags/02_brancher.py): How to use BranchPythonOperator or `@task.branch()` when `Trigger Dag w/ config`
  + [03_example_task_group.py](python/dags/03_example_task_group.py) from offical Airflow demo
  + [04_backfill_task_group_with_switcher.py](python/dags/04_backfill_task_group_with_switcher.py): Enable/Disable tasks in the task group
  + [05_backfill_task_group_with_brancher.py](python/dags/05_backfill_task_group_with_brancher.py): Branch to backfill in the task group


## Install Pants and Python
1. Install Pants: https://www.pantsbuild.org/docs/installation
2. Install Python 3.10.x
   + Because in [pants.toml](pants.toml), we set the intepreter to Python 3.10.x
   + Because on Ubuntu 22.04, the default Python is Python 3.10.x
3. No Python Virtual Environment needed

Here is a recommended way to install Python 3.10.x:
```
bin/install_python
```
## One Step Guide to launch Airflow
```
bin/install_airflow && bin/airflow_config && airflow standalone
```

## Step-by-step Guide to launch Airflow
### Step 1: Build the package and install it to `$HOME/airflow/bin/python`
``` bash
bin/install_airflow
```

### Step 2: Airflow Config
``` bash
bin/airflow_config
```
It will:
+ Set `core.dags_folder` to the PEX user code dynamically
+ Set `core.load_examples` to `False`


### Step 3: Launch Airflow
```
airflow standalone
```
If the command line `airflow` is not available, please add `$HOME/bin` to `$PATH`.

For more info, see https://airflow.apache.org/docs/apache-airflow/stable/start.html


## How to remove all the config and get a clean environment
Juse remove the $AIRFLOW_HOME:
```
rm -rf $HOME/airflow
```
