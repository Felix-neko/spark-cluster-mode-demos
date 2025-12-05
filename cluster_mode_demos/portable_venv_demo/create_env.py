#!/usr/bin/env python3
"""
Скрипт для создания портабельного виртуального окружения.

Поддерживает разные методы:
1. venv + tar.gz (встроен в Python, простейший)
2. venv + conda-pack (требует conda)
3. Целое conda окружение + conda-pack (самое надежное для conda пользователей)
4. UV-based venv (современный подход)

Использование:
    # Метод 1: вроде venv с tar.gz (РЕКОМЕНДУЕТСЯ)
    python create_portable_env.py --method venv --output pyspark_env.tar.gz
    
    # Метод 2: conda-pack (если используете conda)
    python create_portable_env.py --method conda-pack --output pyspark_env.tar.gz
    
    # Метод 3: Conda окружение целиком
    python create_portable_env.py --method conda --output pyspark_env.tar.gz
    
    # Метод 4: UV-based (если используете UV)
    python create_portable_env.py --method uv --output pyspark_env.tar.gz
"""

import os
import sys
import shutil
import tarfile
import argparse
import subprocess
import tempfile
from pathlib import Path
from typing import Optional, List


# ============================================================================
# Конфигурация
# ============================================================================

METHODS = {
    'venv': 'Python venv + tar.gz (встроен, простой, универсальный)',
    'venv-zip': 'Python venv + zip архив (для Windows-совместимости)',
    'conda-pack': 'Conda окружение + conda-pack (требует: conda, conda-pack)',
    'conda': 'Целое conda окружение + tar.gz (требует: conda)',
    'uv': 'UV-managed venv + tar.gz (требует: UV)',
}


# ============================================================================
# Вспомогательные функции
# ============================================================================

def run_command(cmd: List[str], description: str = "", check: bool = True) -> bool:
    """
    Выполнить команду.
    
    Args:
        cmd: Список аргументов для subprocess
        description: Описание операции для логирования
        check: Выбросить исключение при ошибке
    
    Returns:
        True если успешно, False если ошибка
    """
    if description:
        print(f"  • {description}...")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        if check:
            print(f"    ✗ Ошибка:")
            print(f"    {result.stderr}")
            raise RuntimeError(f"Command failed: {' '.join(cmd)}")
        else:
            print(f"    ⚠ Предупреждение: Команда завершилась с кодом {result.returncode}")
            return False
    
    return True


def check_command_exists(cmd: str) -> bool:
    """Проверить, доступна ли команда."""
    result = subprocess.run(
        ["which" if os.name != "nt" else "where", cmd],
        capture_output=True
    )
    return result.returncode == 0


def get_python_executable() -> str:
    """Получить путь к исполняемому файлу Python."""
    return sys.executable


# ============================================================================
# Метод 1: venv + tar.gz (РЕКОМЕНДУЕТСЯ)
# ============================================================================

def create_venv_portable(
    venv_dir: Path,
    requirements_file: Path,
    output_archive: Path,
    compress_format: str = "gz"
) -> None:
    """
    Создать портабельное venv с помощью встроенного venv.
    
    ПЛЮСЫ:
    - Встроен в Python 3.3+
    - Нет зависимостей
    - Простой и надежный
    
    МИНУСЫ:
    - Может не работать если Python скомпилирован с очень специфичными опциями
    - Требует хотя бы Python 3 на целевой системе
    
    Args:
        venv_dir: Директория для создания venv
        requirements_file: Файл с зависимостями
        output_archive: Путь для архива
        compress_format: Формат сжатия (gz, bz2, xz)
    """
    print("\n[METHOD: venv + tar.gz]")
    print("ПЛЮСЫ: встроен, универсальный, без зависимостей")
    print("МИНУСЫ: может быть нетранспортируемым на некоторых системах")
    print()
    
    try:
        # Создать venv
        print(f"1. Создание venv в: {venv_dir}")
        run_command(
            [sys.executable, "-m", "venv", str(venv_dir)],
            "Создание виртуального окружения"
        )
        
        # Найти pip
        if os.name == "nt":
            pip_cmd = venv_dir / "Scripts" / "pip.exe"
            python_cmd = venv_dir / "Scripts" / "python.exe"
        else:
            pip_cmd = venv_dir / "bin" / "pip"
            python_cmd = venv_dir / "bin" / "python"
        
        # Обновить pip
        print("\n2. Обновление pip")
        run_command([str(pip_cmd), "install", "--upgrade", "pip", "setuptools", "wheel"])
        
        # Установить зависимости
        print("\n3. Установка зависимостей")
        if requirements_file.exists():
            run_command(
                [str(pip_cmd), "install", "-r", str(requirements_file)],
                f"Установка из {requirements_file.name}"
            )
        
        # Оптимизация размера (удалить .pyc и другой мусор)
        print("\n4. Оптимизация размера")
        _cleanup_venv(venv_dir)
        
        # Упаковать
        print("\n5. Упаковка архива")
        _pack_to_archive(venv_dir, output_archive, compress_format)
        
        print(f"\n✓ Портабельное venv создано: {output_archive}")
        print(f"  Размер: {output_archive.stat().st_size / (1024*1024):.1f} MB")
        
    except Exception as e:
        print(f"\n✗ Ошибка: {e}")
        raise


# ============================================================================
# Метод 2: conda-pack (РЕКОМЕНДУЕТСЯ если используете conda)
# ============================================================================

def create_conda_pack_portable(
    env_name: str,
    requirements_file: Path,
    output_archive: Path,
    python_version: str = "3.9"
) -> None:
    """
    Создать портабельное conda окружение с помощью conda-pack.
    
    ПЛЮСЫ:
    - Самый надежный способ для conda пользователей
    - Работает с любыми C-расширениями и native код
    - Полная гарантия воспроизводимости
    
    МИНУСЫ:
    - Требует conda и conda-pack
    - Может быть очень большим (содержит весь conda окружение)
    - Обычно архив 200MB-1GB
    
    Args:
        env_name: Имя conda окружения
        requirements_file: Файл с зависимостями
        output_archive: Путь для архива
        python_version: Версия Python
    """
    print("\n[METHOD: conda-pack]")
    print("ПЛЮСЫ: очень надежный, работает со всеми C-расширениями")
    print("МИНУСЫ: требует conda/conda-pack, большой размер архива")
    print()
    
    # Проверить наличие conda-pack
    if not check_command_exists("conda-pack"):
        print("✗ Ошибка: conda-pack не установлен")
        print("  Установите: conda install conda-pack")
        print("  Или через pip: pip install conda-pack")
        raise RuntimeError("conda-pack not found")
    
    try:
        # Создать conda окружение
        print(f"1. Создание conda окружения '{env_name}'")
        run_command(
            ["conda", "create", "-y", "-n", env_name, f"python={python_version}"],
            "Создание conda окружения"
        )
        
        # Установить зависимости
        print("\n2. Установка зависимостей")
        if requirements_file.exists():
            run_command(
                ["conda", "run", "-n", env_name, "pip", "install", "-r", str(requirements_file)],
                "Установка зависимостей"
            )
        
        # Использовать conda-pack
        print("\n3. Упаковка с conda-pack")
        run_command(
            ["conda-pack", "-n", env_name, "-o", str(output_archive)],
            "Упаковка окружения"
        )
        
        print(f"\n✓ Портабельное conda окружение создано: {output_archive}")
        print(f"  Размер: {output_archive.stat().st_size / (1024*1024):.1f} MB")
        
        # Подсказка по использованию
        print("\n  Использование на целевом сервере:")
        print(f"    tar -xzf {output_archive.name} -C /путь/к/вашему/окружению")
        print(f"    source /путь/к/вашему/окружению/bin/activate")
        
    except Exception as e:
        print(f"\n✗ Ошибка: {e}")
        print("  Убедитесь что conda-pack установлен: pip install conda-pack")
        raise


# ============================================================================
# Метод 3: Целое conda окружение
# ============================================================================

def create_conda_portable(
    env_name: str,
    requirements_file: Path,
    output_archive: Path,
    python_version: str = "3.9"
) -> None:
    """
    Создать портабельное conda окружение целиком (без conda-pack).
    
    ПЛЮСЫ:
    - Не требует conda-pack
    - Полный контроль над окружением
    
    МИНУСЫ:
    - Может быть очень большим
    - Более сложный процесс
    
    Args:
        env_name: Имя conda окружения
        requirements_file: Файл с зависимостями
        output_archive: Путь для архива
        python_version: Версия Python
    """
    print("\n[METHOD: conda (без conda-pack)]")
    print("ПЛЮСЫ: не требует conda-pack")
    print("МИНУСЫ: может быть очень большим, более сложный процесс")
    print()
    
    try:
        # Получить путь к conda окружению
        print(f"1. Получение информации о conda")
        result = subprocess.run(
            ["conda", "info", "--json"],
            capture_output=True,
            text=True,
            check=True
        )
        
        import json
        conda_info = json.loads(result.stdout)
        envs_dirs = conda_info.get("envs_dirs", [])
        
        if not envs_dirs:
            raise RuntimeError("Cannot determine conda envs directory")
        
        conda_env_path = Path(envs_dirs[0]) / env_name
        
        # Создать окружение
        print(f"2. Создание conda окружения")
        run_command(
            ["conda", "create", "-y", "-n", env_name, f"python={python_version}"],
            "Создание окружения"
        )
        
        # Установить зависимости
        print("\n3. Установка зависимостей")
        if requirements_file.exists():
            run_command(
                ["conda", "run", "-n", env_name, "pip", "install", "-r", str(requirements_file)],
                "Установка зависимостей"
            )
        
        # Упаковать окружение
        print("\n4. Упаковка окружения")
        _pack_to_archive(conda_env_path, output_archive, "gz")
        
        print(f"\n✓ Портабельное conda окружение создано: {output_archive}")
        print(f"  Размер: {output_archive.stat().st_size / (1024*1024):.1f} MB")
        
    except Exception as e:
        print(f"\n✗ Ошибка: {e}")
        raise


# ============================================================================
# Метод 4: UV-based venv
# ============================================================================

def create_uv_portable(
    venv_dir: Path,
    requirements_file: Path,
    output_archive: Path,
    compress_format: str = "gz"
) -> None:
    """
    Создать портабельное venv с помощью UV.
    
    ПЛЮСЫ:
    - Очень быстрый процесс установки (UV супер быстрый)
    - Современный подход
    - Можно использовать pyproject.toml
    
    МИНУСЫ:
    - Требует UV установленный
    - Относительно новый инструмент
    
    Args:
        venv_dir: Директория для создания venv
        requirements_file: Файл с зависимостями
        output_archive: Путь для архива
        compress_format: Формат сжатия
    """
    print("\n[METHOD: UV-based venv]")
    print("ПЛЮСЫ: очень быстрый, современный подход")
    print("МИНУСЫ: требует UV установленный")
    print()
    
    if not check_command_exists("uv"):
        print("✗ Ошибка: UV не установлен")
        print("  Установите: curl -LsSf https://astral.sh/uv/install.sh | sh")
        print("  Или через pip: pip install uv")
        raise RuntimeError("uv not found")
    
    try:
        # Создать venv с UV
        print(f"1. Создание venv с UV")
        run_command(
            ["uv", "venv", str(venv_dir)],
            "Создание виртуального окружения"
        )
        
        # Найти pip из UV окружения
        if os.name == "nt":
            pip_cmd = venv_dir / "Scripts" / "pip.exe"
        else:
            pip_cmd = venv_dir / "bin" / "pip"
        
        # Установить зависимости через UV
        print("\n2. Установка зависимостей через UV")
        if requirements_file.exists():
            run_command(
                ["uv", "pip", "install", "-r", str(requirements_file)],
                "Установка зависимостей"
            )
        
        # Оптимизация размера
        print("\n3. Оптимизация размера")
        _cleanup_venv(venv_dir)
        
        # Упаковать
        print("\n4. Упаковка архива")
        _pack_to_archive(venv_dir, output_archive, compress_format)
        
        print(f"\n✓ Портабельное UV venv создано: {output_archive}")
        print(f"  Размер: {output_archive.stat().st_size / (1024*1024):.1f} MB")
        
    except Exception as e:
        print(f"\n✗ Ошибка: {e}")
        raise


# ============================================================================
# Вспомогательные функции для упаковки
# ============================================================================

def _cleanup_venv(venv_dir: Path) -> None:
    """Удалить ненужные файлы из venv для уменьшения размера."""
    print("    • Удаление .pyc файлов")
    subprocess.run(
        ["find", str(venv_dir), "-type", "f", "-name", "*.pyc", "-delete"],
        capture_output=True
    )
    
    print("    • Удаление __pycache__ директорий")
    subprocess.run(
        ["find", str(venv_dir), "-type", "d", "-name", "__pycache__", "-exec", "rm", "-rf", "{}", "+"],
        capture_output=True
    )
    
    print("    • Удаление .pytest_cache")
    subprocess.run(
        ["find", str(venv_dir), "-type", "d", "-name", ".pytest_cache", "-exec", "rm", "-rf", "{}", "+"],
        capture_output=True
    )


def _pack_to_archive(source_dir: Path, output_path: Path, compress_format: str) -> None:
    """Упаковать директорию в архив."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if compress_format == "zip":
        import zipfile
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, files in os.walk(source_dir):
                for file in files:
                    file_path = Path(root) / file
                    arcname = file_path.relative_to(source_dir.parent)
                    zf.write(file_path, arcname)
    else:
        tar_format = f"w:{compress_format}"
        with tarfile.open(output_path, tar_format) as tar:
            tar.add(source_dir, arcname=source_dir.name)


# ============================================================================
# Главная функция
# ============================================================================

def main():
    """Главная функция."""
    
    parser = argparse.ArgumentParser(
        description="Создать портабельное виртуальное окружение для PySpark"
    )
    parser.add_argument(
        "--method",
        choices=list(METHODS.keys()),
        default="venv",
        help=f"Метод создания окружения (default: venv)"
    )
    parser.add_argument(
        "--requirements",
        type=Path,
        default=Path("requirements.txt"),
        help="Путь к файлу с зависимостями (default: requirements.txt)"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=Path("pyspark_env.tar.gz"),
        help="Путь для выходного архива (default: pyspark_env.tar.gz)"
    )
    parser.add_argument(
        "--env-name",
        default="pyspark_env",
        help="Имя окружения для conda методов (default: pyspark_env)"
    )
    parser.add_argument(
        "--python-version", "-p",
        default="3.9",
        help="Версия Python для установки (default: 3.9)"
    )
    parser.add_argument(
        "--list-methods",
        action="store_true",
        help="Показать доступные методы и выход"
    )
    
    args = parser.parse_args()
    
    # Показать доступные методы
    if args.list_methods:
        print("\nДоступные методы создания портабельного окружения:\n")
        for method, description in METHODS.items():
            print(f"  {method:12} - {description}")
        print()
        return
    
    # Проверить requirements файл
    if not args.requirements.exists():
        print(f"✗ Файл не найдена: {args.requirements}")
        print(f"  Пожалуйста создайте файл с зависимостями")
        sys.exit(1)
    
    print("="*70)
    print("CREATE PORTABLE VIRTUALENV")
    print("="*70)
    print(f"Method: {args.method}")
    print(f"Requirements: {args.requirements}")
    print(f"Output: {args.output}")
    print(f"Python version: {args.python_version}")
    print("="*70)
    
    try:
        # Создать временную директорию
        temp_dir = Path(tempfile.gettempdir()) / "pyspark_venv_prep"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        venv_dir = temp_dir / "venv"
        
        # Выбрать метод
        if args.method == "venv":
            create_venv_portable(venv_dir, args.requirements, args.output)
        
        elif args.method == "venv-zip":
            create_venv_portable(venv_dir, args.requirements, args.output, compress_format="no")
            # Переупаковать в zip
            old_archive = args.output
            args.output = args.output.with_suffix(".zip")
            _pack_to_archive(venv_dir, args.output, "zip")
            old_archive.unlink()
        
        elif args.method == "conda-pack":
            create_conda_pack_portable(args.env_name, args.requirements, args.output, args.python_version)
        
        elif args.method == "conda":
            create_conda_portable(args.env_name, args.requirements, args.output, args.python_version)
        
        elif args.method == "uv":
            create_uv_portable(venv_dir, args.requirements, args.output)
        
        # Очистить временную директорию
        shutil.rmtree(temp_dir)
        
        print("\n" + "="*70)
        print("✓ УСПЕШНО")
        print("="*70 + "\n")
        
        print("Следующие шаги:")
        print(f"1. Загрузить архив на кластер:")
        print(f"   scp {args.output} user@cluster:/tmp/")
        print(f"\n2. На кластере распаковать:")
        print(f"   tar -xzf /tmp/{args.output.name} -C /opt/")
        print(f"\n3. Использовать в spark-submit:")
        print(f"   spark-submit --archives /opt/{args.output.name}#{args.env_name} \\")
        print(f"     --conf spark.pyspark.python=./{args.env_name}/bin/python \\")
        print(f"     your_script.py")
        
    except Exception as e:
        print(f"\n{'='*70}")
        print(f"✗ ОШИБКА: {e}")
        print(f"{'='*70}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
