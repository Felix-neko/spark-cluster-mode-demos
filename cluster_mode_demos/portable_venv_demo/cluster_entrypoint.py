"""
Точка входа Spark-приложения в cluster-режиме.
Поддерживает отправку логов на удалённый сокет для real-time мониторинга.

Переменные окружения:
    LOG_HOST - IP-адрес хоста для отправки логов (опционально)
    LOG_PORT - порт для отправки логов (опционально, по умолчанию 9999)
"""

import os
import socket
import sys
from contextlib import contextmanager
from pathlib import Path


class SocketWriter:
    """Писатель, который отправляет данные на сокет и дублирует в оригинальный поток."""
    
    def __init__(self, sock: socket.socket, original_stream):
        self.sock = sock
        self.original = original_stream
        self._encoding = "utf-8"
    
    @property
    def encoding(self):
        return self._encoding
    
    def write(self, s: str) -> int:
        if s:
            # Пишем в оригинальный поток
            if self.original:
                self.original.write(s)
                self.original.flush()
            
            # Отправляем на сокет
            try:
                self.sock.sendall(s.encode(self._encoding))
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass  # Сокет закрылся - игнорируем
        return len(s) if s else 0
    
    def flush(self):
        if self.original:
            self.original.flush()
    
    def fileno(self):
        if self.original:
            return self.original.fileno()
        raise OSError("No file descriptor")
    
    def isatty(self):
        return False


@contextmanager
def socket_logging(host: str, port: int):
    """Контекстный менеджер для перенаправления stdout/stderr на сокет."""
    sock = None
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    
    try:
        # Подключаемся к слушателю
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        
        # Перенаправляем stdout и stderr
        sys.stdout = SocketWriter(sock, original_stdout)
        sys.stderr = SocketWriter(sock, original_stderr)
        
        print(f"[SOCKET] Connected to {host}:{port}")
        yield sock
        
    except (ConnectionRefusedError, OSError) as e:
        # Если не удалось подключиться - работаем без сокета
        print(f"[SOCKET] Failed to connect to {host}:{port}: {e}", file=original_stderr)
        yield None
        
    finally:
        # Восстанавливаем потоки
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        
        if sock:
            try:
                sock.close()
            except Exception:
                pass


def run_spark_job():
    """Основная логика Spark-приложения."""
    print("=" * 80)
    print("CLUSTER ENTRYPOINT STARTED")
    print("=" * 80)
    
    print(f"Current working dir: {os.getcwd()}")
    print(f"sys.path: {sys.path}")
    print(f"Python version: {sys.version}")
    print(f"Python executable path: {sys.executable}")
    
    print("\nDirectory contents:")
    for item in os.listdir('.'):
        print(f"  - {item}")
    
    print(f"\netl_repo exists: {Path('etl_repo').exists()}")
    print(f"etl_repo.zip exists: {Path('etl_repo.zip').exists()}")
    
    if Path('etl_repo').exists():
        print(f"etl_repo contents: {os.listdir('etl_repo')}")
        sys.path.insert(0, str(Path('etl_repo').resolve()))
        print("\n" + "=" * 80)
        print("ETL REPO IMPORT CHECKS...")
        print("=" * 80)

        try:
            from etl import test_package_import, test_more_itertools

            test_package_import()
            test_more_itertools()
        except Exception as ex:
            print(ex)

    
    print("\n" + "=" * 80)
    print("CREATING SPARK SESSION")
    print("=" * 80)
    
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("Portable Venv Demo") \
        .enableHiveSupport().getOrCreate()
    
    print(f"Spark version: {spark.version}")
    print(f"Spark master: {spark.sparkContext.master}")
    print(f"App ID: {spark.sparkContext.applicationId}")
    
    print("\n" + "=" * 80)
    print("TESTING BASIC SPARK OPERATIONS")
    print("=" * 80)
    
    data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    df = spark.createDataFrame(data, ["id", "name"])
    
    print("\nCreated DataFrame:")
    df.show()
    
    print(f"Row count: {df.count()}")
    
    print("\n" + "=" * 80)
    print("CLUSTER ENTRYPOINT FINISHED SUCCESSFULLY")
    print("=" * 80)

    
    spark.stop()


if __name__ == "__main__":
    # Получаем параметры для socket-логирования из переменных окружения
    log_host = os.environ.get("LOG_HOST")
    log_port = int(os.environ.get("LOG_PORT", "9999"))
    
    if log_host:
        # Запускаем с перенаправлением логов на сокет
        with socket_logging(log_host, log_port):
            run_spark_job()
    else:
        # Запускаем без socket-логирования
        run_spark_job()