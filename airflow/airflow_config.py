import os
import configparser
from airflow.configuration import AIRFLOW_CONFIG, conf


def config():
    print(f"dags_folder is {conf.get('core', 'dags_folder')}")
    pex_path = os.path.abspath(os.path.dirname(__file__))
    dags_folder = os.path.realpath(f"{pex_path}/dags")
    if os.path.exists(AIRFLOW_CONFIG):
        config = configparser.ConfigParser()
        config.read(AIRFLOW_CONFIG)

        # rewrite the core dags_folder
        if config.get("core", "dags_folder") != dags_folder:
            config.set("core", "dags_folder", dags_folder)
            with open(AIRFLOW_CONFIG, 'w') as f:
                config.write(f)
                print(f"overwrite dags_folder: {dags_folder}")

        # rewrite the core load_examples
        if config.get("core", "load_examples") == 'True':
            config.set("core", "load_examples", "False")
            with open(AIRFLOW_CONFIG, 'w') as f:
                config.write(f)
                print(f"overwrite load_examples: False")

if __name__ == "__main__":
    config()
