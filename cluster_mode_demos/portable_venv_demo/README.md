# PySpark Cluster Mode - Полное руководство

## 📋 Содержание

1. [Структура проекта](#структура-проекта)
2. [Запуск в клиентском режиме](#запуск-в-клиентском-режиме)
3. [Запуск в кластерном режиме](#запуск-в-кластерном-режиме)
4. [Создание портабельного окружения](#создание-портабельного-окружения)
5. [Варианты методов](#варианты-методов-создания-окружения)
6. [Устранение неполадок](#устранение-неполадок)

---

## Структура проекта

```
my_project/
├── main.py                          # Основной скрипт (расширенная версия)
├── package_dir/
│   ├── __init__.py
│   └── subpackage_dir/
│       ├── __init__.py
│       └── utils.py                 # Пользовательские функции
├── hadoop_configs/
│   └── quickstart-bigdata/          # Конфиги Hadoop
├── requirements.txt                 # Зависимости проекта
├── submit_cluster.py                # Скрипт для автоматического запуска на кластере
├── create_portable_env.py           # Утилита для создания портабельного venv
└── venv/                            # Локальное окружение (для разработки)
```

---

## Запуск в клиентском режиме

Клиентский режим - это когда вы запускаете Python скрипт локально, а Spark вычисления идут на кластер.

### 1. Подготовка

```bash
# Установить Python 3.7+
python3 --version

# Создать локальное окружение
python3 -m venv venv

# Активировать
source venv/bin/activate  # на Linux/Mac
# или
venv\Scripts\activate      # на Windows

# Установить зависимости
pip install -r requirements.txt
```

### 2. Запуск в клиентском режиме

```bash
# Простой запуск (локально)
python main.py

# С указанием master для YARN кластера
export HADOOP_CONF_DIR=/path/to/hadoop/config
python main.py
```

### 3. Проверка режима

В выводе должно отобразиться:

```
============================================================
SPARK DEPLOYMENT MODE: CLIENT
============================================================
Master: yarn
App Name: ClientModeDemo
Spark Version: 3.5.0
```

---

## Запуск в кластерном режиме

Кластерный режим - это когда код выполняется полностью на кластере.

### 1. Подготовка окружения на локальной машине

```bash
# Создать портабельное venv (рекомендуется метод 'venv')
python create_portable_env.py \
  --method venv \
  --requirements requirements.txt \
  --output pyspark_env.tar.gz
```

Это создаст архив `pyspark_env.tar.gz` (~50-200 MB).

### 2. Запустить скрипт отправки на кластер

```bash
# Базовый запуск
python submit_cluster.py

# С указанием Python версии
python submit_cluster.py \
  --python-version 3.9 \
  --hdfs-base /user/spark/jobs

# Со скачиванием с HDFS (если нужно)
python submit_cluster.py \
  --master yarn \
  --deploy-mode cluster
```

### 3. Скрипт автоматически:

✓ Копирует все файлы проекта
✓ Создает портабельное venv
✓ Загружает на HDFS
✓ Запускает `spark-submit` с правильными параметрами

### 4. Вывод должен быть похож на:

```
======================================================================
PySpark CLUSTER SUBMIT
======================================================================
Master: yarn
Deploy Mode: cluster
Python Version: 3.9
HDFS Base: /user/spark/jobs
======================================================================

[INFO] Копирование файлов проекта...
  ✓ Директория скопирована: package_dir
  ✓ Файл скопирован: main.py

[INFO] Создание venv с pip...
  ✓ venv создан
  • Обновление pip...
  • Установка зависимостей...
  ✓ Зависимости установлены

[INFO] Упаковка venv в архив...
  ✓ Архив создан (125.5 MB)

[INFO] Загрузка на HDFS...
  ✓ Загруженно на HDFS

[INFO] Запуск spark-submit
  Команда: spark-submit --master yarn --deploy-mode cluster ...

...вывод от Spark...

✓ УСПЕШНО ЗАВЕРШЕНО
```

---

## Создание портабельного окружения

### Просмотр доступных методов

```bash
python create_portable_env.py --list-methods
```

Вывод:

```
Доступные методы создания портабельного окружения:

  venv         - Python venv + tar.gz (встроен, простой, универсальный)
  venv-zip     - Python venv + zip архив (для Windows-совместимости)
  conda-pack   - Conda окружение + conda-pack (требует: conda, conda-pack)
  conda        - Целое conda окружение + tar.gz (требует: conda)
  uv           - UV-managed venv + tar.gz (требует: UV)
```

---

## Варианты методов создания окружения

### Метод 1: venv + tar.gz ⭐ РЕКОМЕНДУЕТСЯ

**Лучше для:** Большинство случаев, если используете UV или pip

```bash
python create_portable_env.py \
  --method venv \
  --output pyspark_env.tar.gz
```

**ПЛЮСЫ:**
- ✅ Встроен в Python 3.3+
- ✅ Нет никаких зависимостей
- ✅ Просто и надежно
- ✅ Размер архива: 30-150 MB

**МИНУСЫ:**
- ⚠️ Может быть нетранспортируемым на некоторых очень специфичных системах

**Когда использовать:** В 95% случаев - это ваш выбор

---

### Метод 2: conda-pack ⭐ РЕКОМЕНДУЕТСЯ если используете conda

**Лучше для:** Если вы работаете в conda экосистеме

```bash
# Сначала установить conda-pack
pip install conda-pack
# или
conda install conda-pack

# Затем создать окружение
python create_portable_env.py \
  --method conda-pack \
  --env-name pyspark_env \
  --output pyspark_env.tar.gz
```

**ПЛЮСЫ:**
- ✅ Специально разработано для этого
- ✅ Очень надежно для conda пользователей
- ✅ Работает со всеми C-расширениями и native кодом
- ✅ Полная гарантия воспроизводимости

**МИНУСЫ:**
- ⚠️ Требует conda и conda-pack
- ⚠️ Может быть очень большим (200 MB - 1 GB)
- ⚠️ На целевом сервере нужен GLIBC совместимый

**Когда использовать:** Если используете conda с научными пакетами (numpy, scipy, etc.)

---

### Метод 3: UV-based venv (современный подход)

**Лучше для:** Проектов с современным Python инструментарием

```bash
# Установить UV (если еще не установлен)
curl -LsSf https://astral.sh/uv/install.sh | sh
# или
pip install uv

# Создать окружение
python create_portable_env.py \
  --method uv \
  --output pyspark_env.tar.gz
```

**ПЛЮСЫ:**
- ✅ Супер быстро (в 100+ раз быстрее pip)
- ✅ Можно использовать pyproject.toml
- ✅ Современный инструмент
- ✅ Все еще встроено в venv

**МИНУСЫ:**
- ⚠️ Требует UV установленный
- ⚠️ Относительно новый инструмент

**Когда использовать:** Если вы уже используете UV в проекте

---

### Метод 4: Целое conda окружение (не рекомендуется)

```bash
python create_portable_env.py \
  --method conda \
  --output pyspark_env.tar.gz
```

**ПЛЮСЫ:**
- Полный контроль над окружением

**МИНУСЫ:**
- ⚠️ Очень большой размер архива (500 MB - 2 GB)
- ⚠️ Более сложный процесс
- ⚠️ Использовать conda-pack вместо этого

**Когда использовать:** Редко, только если conda-pack не работает

---

## Сравнительная таблица методов

| Метод | Размер | Скорость | Надежность | Зависимости | Совместимость |
|-------|--------|----------|-----------|-------------|--------------|
| **venv** | 30-150 MB | ⭐⭐⭐ | ⭐⭐⭐ | Нет | ⭐⭐⭐ |
| **conda-pack** | 200 MB - 1 GB | ⭐⭐ | ⭐⭐⭐⭐ | conda, conda-pack | ⭐⭐⭐ |
| **conda** | 500 MB - 2 GB | ⭐ | ⭐⭐⭐ | conda | ⭐⭐ |
| **uv** | 30-150 MB | ⭐⭐⭐⭐ | ⭐⭐⭐ | uv | ⭐⭐⭐ |

---

## UV с conda-pack

Если вы используете **UV** и хотите установить **conda-pack**:

```bash
# Вариант 1: Через pip (рекомендуется)
uv pip install conda-pack

# Вариант 2: Если у вас есть conda окружение
conda install conda-pack

# Вариант 3: Через pip напрямую (если UV не работает)
pip install conda-pack

# Проверить установку
conda-pack --version
```

---

## Режимы запуска PySpark

### 1. Local Mode

```python
spark = SparkSession.builder.master("local[*]").getOrCreate()
```

- Выполнение: полностью локально
- Использование: разработка и тестирование
- Режим: `local`

### 2. Client Mode (YARN)

```python
spark = SparkSession.builder.master("yarn").getOrCreate()
```

- Выполнение: Driver на локальной машине, Executors на YARN кластере
- Использование: интерактивное использование
- Режим: `client`

```bash
# Запуск
python main.py
```

### 3. Cluster Mode (YARN)

```bash
# Запуск
python submit_cluster.py --deploy-mode cluster
```

- Выполнение: Driver и Executors оба на кластере
- Использование: production jobs
- Режим: `cluster`

---

## Проверка режима в коде

```python
from pyspark.sql import SparkSession

def get_spark_deployment_mode(spark):
    """Определить режим запуска."""
    master = spark.sparkContext.master
    
    if master.startswith("local"):
        return "local"
    
    if master == "yarn":
        deploy_mode = os.environ.get("SPARK_SUBMIT_DEPLOY_MODE", "client")
        return deploy_mode if deploy_mode == "cluster" else "client"
    
    return "unknown"

spark = SparkSession.builder.master("yarn").getOrCreate()
mode = get_spark_deployment_mode(spark)
print(f"Running in {mode} mode")
```

---

## Устранение неполадок

### Проблема: "module 'package_dir' not found"

**Причина:** Проект не добавлен в PYTHONPATH на Spark executors

**Решение:**

```bash
# Вариант 1: Добавить --py-files
python submit_cluster.py --py-files package_dir.zip

# Вариант 2: Убедитесь что проект загружается на HDFS
python submit_cluster.py --hdfs-base /user/spark/jobs
```

### Проблема: "Python version mismatch"

**Причина:** Разные версии Python на Driver и Executor

**Решение:**

```bash
# Проверить текущую версию Python
python --version

# Установить нужную версию
export PYSPARK_PYTHON=/usr/bin/python3.9
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.9

python main.py
```

### Проблема: "HADOOP_CONF_DIR not set"

**Причина:** Путь к Hadoop конфигам не найден

**Решение:**

```bash
# Установить переменную окружения
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

# Или в коде
import os
os.environ["HADOOP_CONF_DIR"] = "/path/to/hadoop/config"
```

### Проблема: venv архив слишком большой (> 500 MB)

**Решение:** Уменьшить зависимости или использовать другой метод

```bash
# 1. Удалить ненужные зависимости из requirements.txt
# 2. Использовать более легкие альтернативы

# Пример оптимизированного requirements.txt:
pyspark>=3.5.0
more-itertools>=10.0.0
# Удалить: numpy, pandas, matplotlib если не нужны
```

### Проблема: "Permission denied" при загрузке на HDFS

**Решение:**

```bash
# Убедитесь что у вас есть права на HDFS директорию
hdfs dfs -ls /user/spark/

# Или используйте другую директорию
python submit_cluster.py --hdfs-base /tmp/spark_jobs
```

---

## Пример полного workflow

### 1. Развернуть проект

```bash
mkdir my_pyspark_project
cd my_pyspark_project

# Создать структуру
mkdir -p package_dir/subpackage_dir
mkdir hadoop_configs/quickstart-bigdata

# Скопировать файлы
cp main.py .
cp submit_cluster.py .
cp create_portable_env.py .
cp requirements.txt .
```

### 2. Разработка локально

```bash
# Создать venv
python -m venv venv
source venv/bin/activate

# Установить зависимости
pip install -r requirements.txt

# Тестировать в local mode (изменить в main.py)
# spark = SparkSession.builder.master("local[*]")
python main.py
```

### 3. Тестировать в client mode

```bash
# Убедитесь что HADOOP доступен
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

# Запустить
python main.py
```

### 4. Deploy на кластер

```bash
# Создать портабельное окружение
python create_portable_env.py --method venv

# Отправить на кластер
python submit_cluster.py

# Отслеживать в YARN UI
# http://yarn-master:8088
```

---

## Полезные команды

```bash
# Список окружений conda
conda env list

# Информация о conda-pack
conda-pack --help

# Проверить версию UV
uv --version

# Список пакетов в venv
pip freeze

# Проверить HADOOP
hadoop version
hdfs dfs -ls /
```

---

## Документация

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark Deployment](https://spark.apache.org/docs/latest/submitting-applications.html)
- [conda-pack Documentation](https://conda.pack.readthedocs.io/)
- [UV Documentation](https://docs.astral.sh/uv/)
