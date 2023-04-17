import sys

from airflow.__main__ import main
from airflow.configuration import AIRFLOW_HOME


def airflow_main():
    sys.executable = f"{AIRFLOW_HOME}/bin/python"
    main()

if __name__ == "__main__":
    airflow_main()
