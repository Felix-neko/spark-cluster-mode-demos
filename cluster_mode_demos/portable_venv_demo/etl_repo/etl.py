import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper


# ============================================================================
# Функции для проверки режима запуска
# ============================================================================

def get_spark_deployment_mode(spark):
    """
    Определить режим запуска PySpark: local, client, или cluster.

    Returns:
        str: Один из 'local', 'client', 'cluster'
    """
    master = spark.sparkContext.master

    # local[*] или local[n]
    if master.startswith("local"):
        return "local"

    # yarn в client режиме
    if master == "yarn":
        # В YARN-client режиме SPARK_SUBMIT_DEPLOY_MODE=client
        deploy_mode = os.environ.get("SPARK_SUBMIT_DEPLOY_MODE", "client")
        if deploy_mode == "cluster":
            return "cluster"
        return "client"

    # spark://host:port
    if master.startswith("spark://"):
        deploy_mode = os.environ.get("SPARK_SUBMIT_DEPLOY_MODE", "client")
        if deploy_mode == "cluster":
            return "cluster"
        return "client"

    return "unknown"


def print_deployment_info(spark):
    """Вывести информацию о режиме запуска."""
    mode = get_spark_deployment_mode(spark)
    print(f"\n{'='*60}")
    print(f"SPARK DEPLOYMENT MODE: {mode.upper()}")
    print(f"{'='*60}")
    print(f"Master: {spark.sparkContext.master}")
    print(f"App Name: {spark.sparkContext.appName}")
    print(f"Spark Version: {spark.version}")
    print()


# ============================================================================
# Функции для проверки environment на executor
# ============================================================================

def get_executor_python_info(iterator):
    """Получить информацию о Python на executor."""
    import sys
    import os
    executor_info = {
        'python_version': sys.version,
        'python_executable': sys.executable,
        'pyspark_python': os.environ.get('PYSPARK_PYTHON', 'NOT SET')
    }
    yield executor_info


# ============================================================================
# Основная логика
# ============================================================================

def setup_environment():
    """Настроить переменные окружения."""
    project_root = Path(__file__).parent

    # hadoop_conf_dir = project_root / "hadoop_configs" / "quickstart-bigdata"
    # os.environ["HADOOP_CONF_DIR"] = str(hadoop_conf_dir.resolve())
    # os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.7"

    # Добавить проект в sys.path для импорта package_dir
    sys.path.insert(0, str(project_root))


def create_spark_session():
    """Создать SparkSession."""
    return (SparkSession.builder
            .appName("ClientModeDemo")
            .master("yarn")
            .enableHiveSupport()
            .getOrCreate())


def test_package_import():
    """Тест: импорт функции из пакета на Driver."""
    print("\n" + "="*60)
    print("ПРОВЕРКА 1: Импорт из package_dir на Driver")
    print("="*60)

    try:
        from package_dir.subpackage_dir.utils import hello_world
        result = hello_world()
        print(f"✓ Успешный импорт: {result}")
        return True
    except ImportError as e:
        print(f"✗ ОШИБКА: Не удалось импортировать пакет!")
        print(f"  Детали: {e}")
        raise RuntimeError(f"Import failed: {e}")


def test_more_itertools():
    """Тест: использование more_itertools на Driver."""
    print("\n" + "="*60)
    print("ПРОВЕРКА 2: Использование more_itertools на Driver")
    print("="*60)

    try:
        from more_itertools import chunked
        data = list(range(10))
        chunks = list(chunked(data, 3))
        print(f"✓ Успешный импорт more_itertools")
        print(f"  Разбиение [0-9] на части по 3: {chunks}")
        return True
    except ImportError as e:
        print(f"✗ ОШИБКА: Не удалось импортировать more_itertools!")
        print(f"  Детали: {e}")
        raise RuntimeError(f"more_itertools import failed: {e}")


def test_executor_python_version(spark):
    """Проверить совпадение версий Python на Driver и Executor."""
    print("\n" + "="*60)
    print("ПРОВЕРКА 3: Версии Python на Driver и Executor")
    print("="*60)

    # Создаем простой RDD и проверяем информацию с executor
    test_rdd = spark.sparkContext.parallelize(range(1), 1)
    executor_info = test_rdd.mapPartitions(get_executor_python_info).collect()

    print(f"[DRIVER] Python version: {sys.version}")
    print(f"[EXECUTOR] Python version: {executor_info[0]['python_version']}")
    print(f"[EXECUTOR] Python executable: {executor_info[0]['python_executable']}")
    print(f"[EXECUTOR] PYSPARK_PYTHON env: {executor_info[0]['pyspark_python']}")

    # Сравниваем major и minor версии Python
    driver_version = sys.version_info
    executor_version_str = executor_info[0]['python_version'].split()[0]
    executor_version = tuple(map(int, executor_version_str.split('.')[:2]))

    if (driver_version.major, driver_version.minor) != executor_version:
        raise RuntimeError(
            f"ОШИБКА: Несовпадение версий Python!\n"
            f"Driver: Python {driver_version.major}.{driver_version.minor}\n"
            f"Executor: Python {executor_version[0]}.{executor_version[1]}\n"
            f"Проверьте переменные окружения PYSPARK_PYTHON и PYSPARK_DRIVER_PYTHON"
        )

    print("✓ Проверка пройдена: версии Python совпадают\n")


def test_dataframe_operations(spark):
    """Тест базовых операций с DataFrame."""
    print("="*60)
    print("ПРОВЕРКА 4: Базовый функционал DataFrame")
    print("="*60)

    # Создание тестового DataFrame
    data = [
        (1, "Alice", 25, "Engineering"),
        (2, "Bob", 30, "Sales"),
        (3, "Charlie", 35, "Engineering"),
        (4, "Diana", 28, "HR")
    ]
    columns = ["id", "name", "age", "department"]

    df = spark.createDataFrame(data, columns)
    print("\nИсходный DataFrame:")
    df.show()

    # Базовые операции с DataFrame
    # 1. Фильтрация
    filtered_df = df.filter(col("age") > 28)
    print("\nФильтр (age > 28):")
    filtered_df.show()

    # 2. Преобразование
    transformed_df = df.withColumn("name_upper", upper(col("name")))
    print("\nДобавлен столбец с именем в верхнем регистре:")
    transformed_df.show()

    # 3. Группировка и агрегация
    agg_df = df.groupBy("department").count()
    print("\nГруппировка по department:")
    agg_df.show()

    # 4. Сортировка
    sorted_df = df.orderBy(col("age").desc())
    print("\nСортировка по возрасту (убывание):")
    sorted_df.show()

    # 5. Select и alias
    selected_df = df.select(
        col("name"),
        col("age").alias("employee_age")
    )
    print("\nВыбор столбцов с alias:")
    selected_df.show()

    # Проверка количества записей
    total_count = df.count()
    filtered_count = filtered_df.count()

    assert total_count == 4, f"Ожидалось 4 записи, получено {total_count}"
    assert filtered_count == 2, f"После фильтрации ожидалось 2 записи, получено {filtered_count}"

    print("\n✓ Все проверки DataFrame пройдены успешно!")


def main():
    """Главная функция."""
    # Подготовка окружения
    setup_environment()

    # Создание SparkSession
    spark = create_spark_session()

    try:
        # Вывод информации о режиме запуска
        print_deployment_info(spark)

        # Проверка подключения к Hive (если доступно)
        print("Проверка доступа к Hive:")
        spark.sql("SHOW DATABASES").show()
        print()

        # ===== ТЕСТЫ =====

        # Тест 1: Импорт пакета
        test_package_import()

        # Тест 2: more_itertools
        test_more_itertools()

        # Тест 3: Версии Python
        test_executor_python_version(spark)

        # Тест 4: DataFrame операции
        test_dataframe_operations(spark)

        # Финальная информация
        print("\n" + "="*60)
        print("✓ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ УСПЕШНО!")
        print("="*60 + "\n")

    except Exception as e:
        print(f"\n{'='*60}")
        print(f"✗ ОШИБКА ПРИ ВЫПОЛНЕНИИ!")
        print(f"{'='*60}")
        print(f"Тип: {type(e).__name__}")
        print(f"Сообщение: {e}")
        print()
        spark.stop()
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
