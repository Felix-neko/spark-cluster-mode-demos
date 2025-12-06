
import os
from pathlib import Path
import sys


if __name__ == "__main__":
    print(f"Current working dir: {os.getcwd()}")
    print(f"sys.path: {sys.path}")
    print(f"Python version: {sys.version}")
    print(f"Python executable path: {sys.executable}")

    print(f"etl_repo_exists: {Path('etl_repo').exists()}")
    print(f"etl_repo contents: {os.listdir('etl_repo')}")

    sys.path.insert(0, str(Path('etl_repo').resolve()))

    # import etl
    # etl.main()