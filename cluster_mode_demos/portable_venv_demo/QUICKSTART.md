# Быстрый справочник команд

## UV + conda-pack

```bash
# Установить conda-pack через UV
uv pip install conda-pack

# Проверить установку
conda-pack --version

# Или альтернативно, если используете pip
pip install conda-pack
```

---

## Создание портабельного окружения

### Метод 1: venv (РЕКОМЕНДУЕТСЯ) - встроен в Python

```bash
# Создать портабельное venv
python create_portable_env.py \
  --method venv \
  --requirements requirements.txt \
  --output pyspark_env.tar.gz

# Результат: архив ~50-150 MB
ls -lh pyspark_env.tar.gz
```

### Метод 2: conda-pack (если используете conda)

```bash
# Установить conda-pack
uv pip install conda-pack
# или
pip install conda-pack

# Создать портабельное окружение
python create_portable_env.py \
  --method conda-pack \
  --env-name pyspark_env \
  --output pyspark_env.tar.gz

# Результат: архив ~200MB-1GB
ls -lh pyspark_env.tar.gz
```

### Метод 3: UV-based venv (современный подход)

```bash
# Установить UV
curl -LsSf https://astral.sh/uv/install.sh | sh
# или
pip install uv

# Создать портабельное venv с UV
python create_portable_env.py \
  --method uv \
  --output pyspark_env.tar.gz

# Результат: архив ~50-150 MB (создается ОЧЕНЬ быстро)
ls -lh pyspark_env.tar.gz
```

### Просмотреть все доступные методы

```bash
python create_portable_env.py --list-methods
```

---

## Запуск в клиентском режиме (локально)

```bash
# Подготовка
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Запуск
python main.py

# Вывод будет содержать:
# SPARK DEPLOYMENT MODE: CLIENT
```

---

## Запуск в кластерном режиме (на Hadoop/YARN)

### Способ 1: Автоматический (РЕКОМЕНДУЕТСЯ)

```bash
# Скрипт автоматически:
# 1. Создает venv
# 2. Упаковывает его
# 3. Загружает на HDFS
# 4. Запускает spark-submit

python submit_cluster.py

# С опциями:
python submit_cluster.py \
  --master yarn \
  --deploy-mode cluster \
  --python-version 3.9 \
  --hdfs-base /user/spark/jobs

# Если пропустить venv (использовать окружение кластера):
python submit_cluster.py --skip-venv
```

### Способ 2: Вручную (если нужен контроль)

```bash
# 1. Создать портабельное venv
python create_portable_env.py --method venv --output pyspark_env.tar.gz

# 2. Загрузить на HDFS
hdfs dfs -put pyspark_env.tar.gz /user/spark/jobs/

# 3. Запустить spark-submit
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --archives hdfs:///user/spark/jobs/pyspark_env.tar.gz#pyspark_env \
  --conf spark.pyspark.python=./pyspark_env/bin/python \
  main.py
```

---

## Проверка режима запуска

```bash
# В выводе main.py будет строка:
# SPARK DEPLOYMENT MODE: [local|client|cluster]

# Local mode (локально):
# SPARK DEPLOYMENT MODE: LOCAL

# Client mode (с удаленным кластером):
# SPARK DEPLOYMENT MODE: CLIENT

# Cluster mode (полностью на кластере):
# SPARK DEPLOYMENT MODE: CLUSTER
```

---

## Проверка версий Python

```bash
# Локально
python --version

# На Hadoop кластере (через hdfs)
spark-submit \
  --master yarn \
  --deploy-mode client \
  -c "import sys; print(sys.version)"
```

---

## Содержимое requirements.txt (пример)

```
pyspark>=3.5.0
more-itertools>=10.0.0

# Опционально для разработки:
# numpy>=1.20.0
# pandas>=1.3.0
# pytest>=6.0.0
```

---

## Структура проекта - быстрая подготовка

```bash
# Создать структуру
mkdir my_pyspark_project
cd my_pyspark_project

# Структура директорий
mkdir -p package_dir/subpackage_dir
mkdir hadoop_configs/quickstart-bigdata

# Создать __init__.py файлы
touch package_dir/__init__.py
touch package_dir/subpackage_dir/__init__.py

# Создать файл с утилитой
cat > package_dir/subpackage_dir/utils.py << 'EOF'
def hello_world():
    return "Hello from package_dir.subpackage_dir!"
EOF

# Создать requirements.txt
cat > requirements.txt << 'EOF'
pyspark>=3.5.0
more-itertools>=10.0.0
EOF

# Скопировать основные скрипты
# (скопировать main.py, submit_cluster.py, create_portable_env.py)
```

---

## Полезные HDFS команды

```bash
# Список файлов
hdfs dfs -ls /user/spark/jobs/

# Загрузить файл
hdfs dfs -put local_file.tar.gz /user/spark/jobs/

# Скачать файл
hdfs dfs -get /user/spark/jobs/file.tar.gz .

# Удалить файл/директорию
hdfs dfs -rm -r /user/spark/jobs/pyspark_env

# Размер директории
hdfs dfs -du -h /user/spark/jobs/
```

---

## Отслеживание выполнения

### YARN UI (веб-интерфейс)

```bash
# Открыть в браузере
http://yarn-master:8088

# Там можно увидеть:
# - Running / Completed applications
# - Logs и output
# - Resource usage
```

### Логи Spark

```bash
# В client mode логи печатаются в консоль

# В cluster mode смотрите YARN UI или:
spark-submit --verbose main.py

# Или через yarn logs
yarn logs -applicationId application_1234567890_0001
```

---

## Переменные окружения

```bash
# Python версия для Spark
export PYSPARK_PYTHON=/usr/bin/python3.9
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.9

# Hadoop конфигурация
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

# Spark Home
export SPARK_HOME=/opt/spark

# Java Home (часто нужен для Hadoop)
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

---

## Типичные ошибки и решения

| Ошибка | Причина | Решение |
|--------|---------|---------|
| `ModuleNotFoundError: No module named 'package_dir'` | Проект не в PYTHONPATH | Убедитесь что проект загружается на HDFS или используйте `--py-files` |
| `Python version mismatch` | Разные версии на Driver и Executor | Установите `PYSPARK_PYTHON` и `PYSPARK_DRIVER_PYTHON` |
| `HADOOP_CONF_DIR not set` | Путь к Hadoop конфигам не найден | Установите `HADOOP_CONF_DIR` перед запуском |
| `Connection refused` | YARN/Hadoop не доступен | Проверьте что кластер запущен и доступен |
| `venv: command not found` | Python venv модуль не установлен | Установите `python3-venv` пакет |
| `conda-pack: command not found` | conda-pack не установлен | `pip install conda-pack` или `uv pip install conda-pack` |

---

## Команда одной строкой

```bash
# Все сразу: подготовка и запуск в cluster mode
python -m venv venv && \
source venv/bin/activate && \
pip install -r requirements.txt && \
python create_portable_env.py --method venv && \
python submit_cluster.py --master yarn --deploy-mode cluster
```

---

## Развертывание на AWS EMR

```bash
# 1. Создать EMR кластер с PySpark
aws emr create-cluster \
  --name my-spark-cluster \
  --release-label emr-7.0.0 \
  --instance-type m5.xlarge \
  --instance-count 3

# 2. SSH в master ноду
aws emr ssh --cluster-id j-XXXXXXXXXXXXX

# 3. Загрузить проект и запустить
scp -i key.pem -r my_pyspark_project ec2-user@master:/home/ec2-user/
ssh -i key.pem ec2-user@master
cd my_pyspark_project
python submit_cluster.py
```

---

## Развертывание на Kubernetes (Spark operator)

```bash
# Требует: Kubernetes кластер с Spark Operator

# 1. Создать Docker образ с проектом
docker build -t my-spark-app:latest .
docker push my-registry/my-spark-app:latest

# 2. Создать SparkApplication CR
kubectl apply -f sparkapp.yaml

# 3. Отслеживать
kubectl get sparkapplications
kubectl logs spark-app-driver
```
