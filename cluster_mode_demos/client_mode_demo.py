import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper


# Проверяем путь к Python на executor-нодах
# Используем mapPartitions для выполнения кода на executors
def get_executor_python_info(iterator):
    import sys
    import os
    executor_info = {
        'python_version': sys.version,
        'python_executable': sys.executable,
        'pyspark_python': os.environ.get('PYSPARK_PYTHON', 'NOT SET')
    }
    yield executor_info

if __name__ == "__main__":
    os.environ["HADOOP_CONF_DIR"] = str((Path(__file__).parent.parent / "hadoop_configs/quickstart-bigdata").resolve())
    os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.7"
    # os.environ["HADOOP_USER_NAME"] = "osboxes"
    spark = SparkSession.builder.appName("ClientModeDemo").master("yarn").enableHiveSupport().getOrCreate()
    spark.sql("SHOW DATABASES").show()


    # Создаем простой RDD и проверяем информацию с executor
    test_rdd = spark.sparkContext.parallelize(range(1), 1)
    executor_info = test_rdd.mapPartitions(get_executor_python_info).collect()

    print(f"\n[EXECUTOR] Python version: {executor_info[0]['python_version']}")
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

    print("\n✓ Проверка пройдена: версии Python на driver и executor совпадают")

    print("\n=== Проверка 2: Базовый функционал DataFrame ===")

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

    # Информация о Spark конфигурации
    print("\n=== Информация о Spark-конфигурации ===")
    print(f"Spark Version: {spark.version}")
    print(f"Master: {spark.sparkContext.master}")
    print(f"App Name: {spark.sparkContext.appName}")