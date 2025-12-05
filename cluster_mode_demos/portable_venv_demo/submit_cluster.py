#!/usr/bin/env python3
"""
Скрипт для запуска PySpark приложения в кластерном режиме (YARN Cluster Mode).

Функциональность:
- Копирует все файлы проекта на HDFS
- Создает портабельное виртуальное окружение
- Упаковывает venv в архив и загружает на кластер
- Запускает spark-submit с необходимыми параметрами

Использование:
    python submit_cluster.py --master yarn --deploy-mode cluster --python-version 3.7
"""

import os
import sys
import shutil
import tarfile
import argparse
import subprocess
import tempfile
from pathlib import Path
from typing import List, Optional


# ============================================================================
# Конфигурация
# ============================================================================

class Config:
    """Конфигурация для кластерного запуска."""
    
    def __init__(self, 
                 master: str = "yarn",
                 deploy_mode: str = "cluster",
                 python_version: str = "3.7",
                 hdfs_base_path: str = "/user/spark/jobs",
                 use_conda_pack: bool = False,
                 virtualenv_name: str = "pyspark_env"):
        
        self.master = master
        self.deploy_mode = deploy_mode
        self.python_version = python_version
        self.hdfs_base_path = hdfs_base_path
        self.use_conda_pack = use_conda_pack
        self.virtualenv_name = virtualenv_name
        
        # Пути на локальной машине
        self.project_root = Path(__file__).parent
        self.main_script = self.project_root / "main.py"
        self.requirements_file = self.project_root / "requirements.txt"
        
        # Временная директория
        self.temp_dir = Path(tempfile.gettempdir()) / "pyspark_cluster_prep"


# ============================================================================
# Утилиты для работы с файловой системой
# ============================================================================

def copy_project_files(source_dir: Path, dest_dir: Path) -> None:
    """
    Копировать файлы проекта, сохраняя структуру.
    
    Args:
        source_dir: Исходная директория
        dest_dir: Целевая директория
    """
    print(f"[INFO] Копирование файлов проекта: {source_dir} -> {dest_dir}")
    
    # Список файлов и директорий для копирования
    items_to_copy = [
        "main.py",
        "package_dir",
        "hadoop_configs",
        "requirements.txt",
    ]
    
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    for item in items_to_copy:
        src = source_dir / item
        dst = dest_dir / item
        
        if not src.exists():
            print(f"[WARN] Не найдено: {src}")
            continue
        
        if src.is_dir():
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
            print(f"  ✓ Директория скопирована: {item}")
        else:
            shutil.copy2(src, dst)
            print(f"  ✓ Файл скопирован: {item}")


def create_venv_with_pip(venv_dir: Path, requirements_file: Path) -> None:
    """
    Создать виртуальное окружение с pip.
    
    Args:
        venv_dir: Путь для создания venv
        requirements_file: Файл с зависимостями
    """
    print(f"\n[INFO] Создание venv с pip: {venv_dir}")
    
    # Создать venv
    result = subprocess.run(
        [sys.executable, "-m", "venv", str(venv_dir)],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"[ERROR] Ошибка при создании venv:")
        print(result.stderr)
        raise RuntimeError(f"Failed to create venv: {result.stderr}")
    
    print("  ✓ venv создан")
    
    # Найти pip в venv
    if os.name == 'nt':  # Windows
        pip_cmd = venv_dir / "Scripts" / "pip"
    else:  # Unix-like
        pip_cmd = venv_dir / "bin" / "pip"
    
    # Обновить pip
    print("  • Обновление pip...")
    subprocess.run(
        [str(pip_cmd), "install", "--upgrade", "pip"],
        capture_output=True,
        check=True
    )
    
    # Установить зависимости
    print(f"  • Установка зависимостей из {requirements_file}")
    result = subprocess.run(
        [str(pip_cmd), "install", "-r", str(requirements_file)],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"[ERROR] Ошибка при установке зависимостей:")
        print(result.stderr)
        raise RuntimeError(f"Failed to install requirements: {result.stderr}")
    
    print("  ✓ Зависимости установлены")


def create_venv_with_conda_pack(venv_dir: Path, requirements_file: Path) -> None:
    """
    Создать виртуальное окружение с conda и conda-pack.
    
    Требует: conda, conda-pack установлены
    
    Args:
        venv_dir: Путь для создания venv
        requirements_file: Файл с зависимостями
    """
    print(f"\n[INFO] Создание conda venv с conda-pack: {venv_dir}")
    
    env_name = venv_dir.name
    
    # Создать conda окружение
    print(f"  • Создание conda окружения '{env_name}'...")
    result = subprocess.run(
        ["conda", "create", "-y", "-n", env_name, f"python=3.{9}"],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"[ERROR] Ошибка при создании conda окружения:")
        print(result.stderr)
        raise RuntimeError(f"Failed to create conda env: {result.stderr}")
    
    print("  ✓ Conda окружение создано")
    
    # Установить зависимости
    print(f"  • Установка зависимостей...")
    result = subprocess.run(
        ["conda", "run", "-n", env_name, "pip", "install", "-r", str(requirements_file)],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"[ERROR] Ошибка при установке зависимостей:")
        print(result.stderr)
        raise RuntimeError(f"Failed to install requirements: {result.stderr}")
    
    print("  ✓ Зависимости установлены")


def pack_venv_to_archive(venv_dir: Path, archive_path: Path) -> None:
    """
    Упаковать venv в tar.gz архив.
    
    Args:
        venv_dir: Директория venv
        archive_path: Путь для сохранения архива
    """
    print(f"\n[INFO] Упаковка venv в архив: {archive_path}")
    
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(venv_dir, arcname=venv_dir.name)
    
    archive_size = archive_path.stat().st_size / (1024 * 1024)  # MB
    print(f"  ✓ Архив создан ({archive_size:.1f} MB)")


# ============================================================================
# HDFS операции
# ============================================================================

def upload_to_hdfs(local_path: Path, hdfs_path: str) -> None:
    """
    Загрузить файл/директорию на HDFS.
    
    Args:
        local_path: Локальный путь
        hdfs_path: HDFS путь
    """
    print(f"\n[INFO] Загрузка на HDFS: {local_path} -> {hdfs_path}")
    
    # Удалить если уже существует
    subprocess.run(
        ["hdfs", "dfs", "-rm", "-r", hdfs_path],
        capture_output=True
    )
    
    # Загрузить
    result = subprocess.run(
        ["hdfs", "dfs", "-put", str(local_path), hdfs_path],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"[ERROR] Ошибка при загрузке на HDFS:")
        print(result.stderr)
        raise RuntimeError(f"Failed to upload to HDFS: {result.stderr}")
    
    print(f"  ✓ Загруженно на HDFS")


# ============================================================================
# Spark Submit
# ============================================================================

def build_spark_submit_command(
    main_script: Path,
    project_files_hdfs: str,
    venv_archive_hdfs: str,
    venv_name: str,
    config: Config,
    additional_args: Optional[List[str]] = None
) -> List[str]:
    """
    Построить команду spark-submit.
    
    Args:
        main_script: Локальный путь к main.py
        project_files_hdfs: HDFS путь к файлам проекта
        venv_archive_hdfs: HDFS путь к venv архиву
        venv_name: Имя директории venv
        config: Конфигурация
        additional_args: Дополнительные аргументы для spark-submit
    
    Returns:
        Список аргументов для spark-submit
    """
    cmd = [
        "spark-submit",
        "--master", config.master,
        "--deploy-mode", config.deploy_mode,
        
        # Архив с venv
        "--archives", f"{venv_archive_hdfs}#{venv_name}",
        
        # Переменная окружения для PySpark
        "--conf", f"spark.pyspark.python=./{venv_name}/bin/python",
        "--conf", f"spark.pyspark.driver.python=/usr/bin/python{config.python_version}",
        
        # Подстановка в рабочую директорию
        "--conf", "spark.yarn.appMasterEnv.PYTHONPATH=project",
        
        # Основное приложение
        str(main_script),
    ]
    
    if additional_args:
        cmd.extend(additional_args)
    
    return cmd


def submit_spark_job(cmd: List[str]) -> None:
    """
    Запустить spark-submit команду.
    
    Args:
        cmd: Список аргументов для spark-submit
    """
    print(f"\n[INFO] Запуск spark-submit")
    print(f"  Команда: {' '.join(cmd)}")
    print()
    
    result = subprocess.run(cmd)
    
    if result.returncode != 0:
        print(f"\n[ERROR] spark-submit завершился с кодом ошибки {result.returncode}")
        sys.exit(1)
    
    print(f"\n✓ spark-submit завершился успешно")


# ============================================================================
# Основная функция
# ============================================================================

def main():
    """Главная функция."""
    
    # Парсинг аргументов
    parser = argparse.ArgumentParser(
        description="Запустить PySpark приложение в кластерном режиме"
    )
    parser.add_argument(
        "--master", default="yarn",
        help="Spark master (default: yarn)"
    )
    parser.add_argument(
        "--deploy-mode", default="cluster",
        help="Deploy mode: client или cluster (default: cluster)"
    )
    parser.add_argument(
        "--python-version", default="3.9",
        help="Python версия на кластере (default: 3.9)"
    )
    parser.add_argument(
        "--hdfs-base", default="/user/spark/jobs",
        help="HDFS базовая директория (default: /user/spark/jobs)"
    )
    parser.add_argument(
        "--use-conda-pack", action="store_true",
        help="Использовать conda-pack вместо venv"
    )
    parser.add_argument(
        "--skip-venv", action="store_true",
        help="Не создавать и не загружать venv (использовать окружение кластера)"
    )
    
    args = parser.parse_args()
    
    # Создать конфигурацию
    config = Config(
        master=args.master,
        deploy_mode=args.deploy_mode,
        python_version=args.python_version,
        hdfs_base_path=args.hdfs_base,
        use_conda_pack=args.use_conda_pack,
    )
    
    print("="*70)
    print("PySpark CLUSTER SUBMIT")
    print("="*70)
    print(f"Master: {config.master}")
    print(f"Deploy Mode: {config.deploy_mode}")
    print(f"Python Version: {config.python_version}")
    print(f"HDFS Base: {config.hdfs_base_path}")
    print("="*70 + "\n")
    
    try:
        # Очистить временную директорию
        if config.temp_dir.exists():
            shutil.rmtree(config.temp_dir)
        config.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # 1. Копировать файлы проекта
        project_copy_dir = config.temp_dir / "project"
        copy_project_files(config.project_root, project_copy_dir)
        
        # 2. Создать venv (если не skip)
        venv_archive_hdfs = None
        
        if not args.skip_venv:
            venv_dir = config.temp_dir / config.virtualenv_name
            
            if config.use_conda_pack:
                create_venv_with_conda_pack(venv_dir, config.requirements_file)
            else:
                create_venv_with_pip(venv_dir, config.requirements_file)
            
            # 3. Упаковать venv
            archive_path = config.temp_dir / f"{config.virtualenv_name}.tar.gz"
            pack_venv_to_archive(venv_dir, archive_path)
            
            # 4. Загрузить на HDFS
            venv_archive_hdfs = f"{config.hdfs_base_path}/{config.virtualenv_name}.tar.gz"
            upload_to_hdfs(archive_path, venv_archive_hdfs)
        
        # 5. Загрузить файлы проекта на HDFS
        project_hdfs = f"{config.hdfs_base_path}/project"
        upload_to_hdfs(project_copy_dir, project_hdfs)
        
        # 6. Построить и запустить spark-submit
        cmd = build_spark_submit_command(
            main_script=config.main_script,
            project_files_hdfs=project_hdfs,
            venv_archive_hdfs=venv_archive_hdfs or "",
            venv_name=config.virtualenv_name,
            config=config,
        )
        
        submit_spark_job(cmd)
        
        print("\n" + "="*70)
        print("✓ УСПЕШНО ЗАВЕРШЕНО")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"\n{'='*70}")
        print(f"✗ ОШИБКА: {e}")
        print(f"{'='*70}\n")
        sys.exit(1)
    
    finally:
        # Очистить временную директорию
        if config.temp_dir.exists():
            shutil.rmtree(config.temp_dir)
            print(f"[INFO] Временная директория очищена: {config.temp_dir}")


if __name__ == "__main__":
    main()
