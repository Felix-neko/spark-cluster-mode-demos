# Структура проекта PySpark

```
my_project/
├── main.py                          # Основной скрипт
├── package_dir/
│   ├── __init__.py
│   └── subpackage_dir/
│       ├── __init__.py
│       └── utils.py                 # Функция hello_world
├── hadoop_configs/
│   └── quickstart-bigdata/          # Конфиги Hadoop
├── requirements.txt                 # Зависимости
├── submit_cluster.py                # Скрипт для клаустера
├── create_portable_env.py           # Создание портабельного venv
└── venv/                            # Локальное окружение (для dev)
```

## Подготовка

### 1. Создать файлы пакета

**package_dir/\_\_init\_\_.py:**
```python
# Empty init
```

**package_dir/subpackage_dir/\_\_init\_\_.py:**
```python
# Empty init
```

**package_dir/subpackage_dir/utils.py:**
```python
def hello_world():
    return "Hello from package_dir.subpackage_dir!"
```

### 2. requirements.txt

```
pyspark>=3.5.0
more-itertools>=10.0.0
conda-pack>=0.7.1  # Для создания портабельного окружения
```
