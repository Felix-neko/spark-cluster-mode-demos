#!/usr/bin/env python3
"""
Скрипт для запуска Spark-приложения в cluster-режиме с real-time логами драйвера.

Используется Socket-подход:
1. Запускается log_listener для приёма логов
2. spark-submit отправляет приложение с переменными LOG_HOST/LOG_PORT
3. Драйвер подключается к listener и стримит логи в реальном времени
"""

import os
import re
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import json
import urllib.request
import urllib.error


# =============================================================================
# Конфигурация
# =============================================================================

BASEDIR = Path(__file__).parent.resolve()
PROJECT_ROOT = BASEDIR.parent.parent
VENV_PATH = PROJECT_ROOT / ".venv"

# Настройки Hadoop/YARN
HADOOP_CONF_DIR = BASEDIR / "hadoop_configs" / "quickstart-bigdata"
HADOOP_USER_NAME = "osboxes"
YARN_RM_URL = "http://quickstart-bigdata:8088"

# Настройки для socket-логирования
LOG_PORT = 9999

# SSH для получения логов (fallback)
SSH_PASS = "BaseUser@123"
SSH_HOST = "osboxes@quickstart-bigdata"


# =============================================================================
# Утилиты
# =============================================================================

def get_host_ip() -> str:
    """Получает IP хост-машины, видимый из кластера."""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


def timestamp() -> str:
    """Возвращает текущее время в формате HH:MM:SS."""
    from datetime import datetime
    return datetime.now().strftime("%H:%M:%S")


def log_info(msg: str):
    """Выводит информационное сообщение."""
    print(f"[{timestamp()}] [INFO] {msg}", flush=True)


def log_error(msg: str):
    """Выводит сообщение об ошибке."""
    print(f"[{timestamp()}] [ERROR] {msg}", file=sys.stderr, flush=True)


# =============================================================================
# YARN REST API
# =============================================================================

def yarn_get_app_info(app_id: str) -> Optional[dict]:
    """Получает информацию о приложении через YARN REST API."""
    url = f"{YARN_RM_URL}/ws/v1/cluster/apps/{app_id}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            return data.get("app", {})
    except Exception:
        return None


def yarn_get_app_state(app_id: str) -> Optional[str]:
    """Получает состояние приложения (ACCEPTED, RUNNING, FINISHED, FAILED, KILLED)."""
    info = yarn_get_app_info(app_id)
    return info.get("state") if info else None


def yarn_get_app_final_status(app_id: str) -> Optional[str]:
    """Получает финальный статус приложения (UNDEFINED, SUCCEEDED, FAILED, KILLED)."""
    info = yarn_get_app_info(app_id)
    return info.get("finalStatus") if info else None


def yarn_get_am_container_id(app_id: str) -> Optional[str]:
    """Получает Container ID Application Master."""
    url = f"{YARN_RM_URL}/ws/v1/cluster/apps/{app_id}/appattempts"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            attempts = data.get("appAttempts", {}).get("appAttempt", [])
            if attempts:
                return attempts[0].get("containerId")
    except Exception:
        pass
    return None


def yarn_fetch_driver_logs_via_ssh(app_id: str) -> str:
    """Получает логи драйвера через SSH + yarn logs."""
    cmd = f"sshpass -p {SSH_PASS} ssh -o StrictHostKeyChecking=no {SSH_HOST} 'yarn logs -applicationId {app_id} 2>/dev/null'"
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        return result.stdout
    except Exception as e:
        return f"[ERROR] Failed to fetch logs: {e}"


# =============================================================================
# Статус приложения
# =============================================================================

@dataclass
class AppStatus:
    """Состояние Spark-приложения."""
    app_id: Optional[str] = None
    state: Optional[str] = None       # ACCEPTED, RUNNING, FINISHED, FAILED, KILLED
    final_status: Optional[str] = None  # UNDEFINED, SUCCEEDED, FAILED, KILLED
    error: Optional[str] = None
    finished: bool = False
    success: bool = False


# =============================================================================
# Поток spark-submit
# =============================================================================

class SparkSubmitThread(threading.Thread):
    """Поток для запуска spark-submit и мониторинга статуса приложения."""
    
    def __init__(self, app_name: str, log_host: str, log_port: int):
        super().__init__(daemon=True)
        self.app_name = app_name
        self.log_host = log_host
        self.log_port = log_port
        
        self.status = AppStatus()
        self.status_lock = threading.Lock()
        self.finished_event = threading.Event()
        self._stop_event = threading.Event()
    
    def get_status(self) -> AppStatus:
        with self.status_lock:
            return AppStatus(
                app_id=self.status.app_id,
                state=self.status.state,
                final_status=self.status.final_status,
                error=self.status.error,
                finished=self.status.finished,
                success=self.status.success,
            )
    
    def stop(self):
        self._stop_event.set()
    
    def run(self):
        try:
            self._run_spark_submit()
        except Exception as e:
            with self.status_lock:
                self.status.error = str(e)
                self.status.finished = True
            self.finished_event.set()
    
    def _run_spark_submit(self):
        # Формируем команду spark-submit
        cmd = [
            "spark-submit",
            "--master", "yarn",
            "--deploy-mode", "cluster",
            "--name", self.app_name,
            "--conf", "spark.eventLog.enabled=true",
            "--conf", "spark.eventLog.dir=hdfs://quickstart-bigdata:8020/user/spark/applicationHistory",
            "--conf", "spark.yarn.submit.waitAppCompletion=true",
            "--conf", "spark.yarn.maxAppAttempts=1",
            "--conf", f"spark.yarn.appMasterEnv.LOG_HOST={self.log_host}",
            "--conf", f"spark.yarn.appMasterEnv.LOG_PORT={self.log_port}",
            "--conf", "spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python3.7",
            "--conf", "spark.yarn.appMasterEnv.LD_LIBRARY_PATH=./environment/lib",
            "--archives", "etl_repo.zip#etl_repo,venv_py37.zip#environment",
            "cluster_entrypoint.py",
        ]
        
        env = os.environ.copy()
        env["HADOOP_USER_NAME"] = HADOOP_USER_NAME
        env["HADOOP_CONF_DIR"] = str(HADOOP_CONF_DIR)
        
        log_info(f"Запуск spark-submit: {self.app_name}")
        
        # Запускаем spark-submit
        process = subprocess.Popen(
            cmd,
            cwd=str(BASEDIR),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        
        app_id_pattern = re.compile(r"application_\d+_\d+")
        accepted_report_shown = False  # Флаг: уже показали первый "state: ACCEPTED"
        running_report_shown = False   # Флаг: уже показали первый "state: RUNNING"
        
        # Читаем вывод и ищем Application ID
        for line in process.stdout:
            line = line.rstrip()
            
            # Фильтруем повторяющиеся сообщения о статусах
            if "Application report for" in line:
                if "state: ACCEPTED" in line:
                    if accepted_report_shown:
                        continue
                    accepted_report_shown = True
                elif "state: RUNNING" in line:
                    if running_report_shown:
                        continue
                    running_report_shown = True
            
            print(f"[spark-submit] {line}", flush=True)
            
            # Ищем Application ID
            if not self.status.app_id:
                match = app_id_pattern.search(line)
                if match:
                    with self.status_lock:
                        self.status.app_id = match.group()
                    log_info(f"Application ID: {self.status.app_id}")
            
            # Проверяем статус приложения раз в секунду
            if self.status.app_id:
                state = yarn_get_app_state(self.status.app_id)
                final_status = yarn_get_app_final_status(self.status.app_id)
                
                with self.status_lock:
                    self.status.state = state
                    self.status.final_status = final_status
                
                # Проверяем на ошибку
                if state in ("FAILED", "KILLED") or final_status in ("FAILED", "KILLED"):
                    with self.status_lock:
                        self.status.error = f"Application {state}/{final_status}"
                        self.status.finished = True
                        self.status.success = False
                    self.finished_event.set()
            
            if self._stop_event.is_set():
                process.terminate()
                break
        
        process.wait()
        
        # Финальная проверка статуса
        if self.status.app_id:
            for _ in range(10):
                state = yarn_get_app_state(self.status.app_id)
                final_status = yarn_get_app_final_status(self.status.app_id)
                
                with self.status_lock:
                    self.status.state = state
                    self.status.final_status = final_status
                
                if state in ("FINISHED", "FAILED", "KILLED"):
                    break
                time.sleep(1)
        
        with self.status_lock:
            self.status.finished = True
            self.status.success = self.status.final_status == "SUCCEEDED"
        
        self.finished_event.set()


# =============================================================================
# Поток log_listener
# =============================================================================

class LogListenerThread(threading.Thread):
    """Поток для запуска log_listener.py."""
    
    def __init__(self, port: int, timeout: int = 120):
        super().__init__(daemon=True)
        self.port = port
        self.timeout = timeout
        self.process: Optional[subprocess.Popen] = None
        self._stop_event = threading.Event()
        self.exit_code: Optional[int] = None
        self.connected = threading.Event()
    
    def stop(self, delay: float = 0):
        """Останавливает listener с опциональной задержкой."""
        if delay > 0:
            time.sleep(delay)
        self._stop_event.set()
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
    
    def run(self):
        cmd = [
            sys.executable,
            str(BASEDIR / "log_listener.py"),
            "--port", str(self.port),
            "--timeout", str(self.timeout),
            "--exit-on-disconnect",
        ]
        
        log_info(f"Запуск log_listener на порту {self.port}")
        
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        
        for line in self.process.stdout:
            line = line.rstrip()
            print(line, flush=True)
            
            # Детектируем подключение драйвера
            if "ДРАЙВЕР ПОДКЛЮЧИЛСЯ" in line or "DRIVER CONNECTED" in line:
                self.connected.set()
            
            if self._stop_event.is_set():
                break
        
        self.exit_code = self.process.wait()


# =============================================================================
# Основная логика
# =============================================================================

def kill_existing_listener():
    """Убивает существующий log_listener, если есть."""
    kill_script = BASEDIR / "kill_listener.sh"
    if kill_script.exists():
        subprocess.run(["bash", str(kill_script)], capture_output=True)


def main():
    # Активируем venv
    activate_script = VENV_PATH / "bin" / "activate_this.py"
    if activate_script.exists():
        exec(open(activate_script).read(), {"__file__": str(activate_script)})
    
    # Устанавливаем переменные окружения
    os.environ["HADOOP_USER_NAME"] = HADOOP_USER_NAME
    os.environ["HADOOP_CONF_DIR"] = str(HADOOP_CONF_DIR)
    
    # Получаем IP хоста
    log_host = get_host_ip()
    app_name = f"portable-venv-demo-{int(time.time())}"
    
    print("=" * 60)
    print("=== Запуск Spark-приложения в cluster-режиме ===")
    print("=" * 60)
    log_info(f"Application Name: {app_name}")
    log_info(f"Log Host: {log_host}:{LOG_PORT}")
    print()
    
    # 0. Убиваем старый listener
    kill_existing_listener()
    
    # 1. Запускаем log_listener в отдельном потоке
    listener_thread = LogListenerThread(port=LOG_PORT, timeout=120)
    listener_thread.start()
    time.sleep(1)
    
    if not listener_thread.is_alive():
        log_error("Не удалось запустить log_listener")
        return 1
    
    log_info("log_listener запущен")
    print()
    
    # 2. Запускаем spark-submit в отдельном потоке
    spark_thread = SparkSubmitThread(app_name=app_name, log_host=log_host, log_port=LOG_PORT)
    spark_thread.start()
    
    # 3. Ждём завершения spark-submit или ошибки
    while True:
        spark_thread.finished_event.wait(timeout=1)
        
        status = spark_thread.get_status()
        
        if status.finished:
            break
        
        # Проверяем, не упал ли listener без подключения
        if not listener_thread.is_alive() and not listener_thread.connected.is_set():
            log_error("log_listener завершился без подключения драйвера")
            # Ждём немного, может приложение ещё работает
            time.sleep(5)
    
    # 4. Получаем финальный статус
    status = spark_thread.get_status()
    
    print()
    print("=" * 60)
    print("РЕЗУЛЬТАТ:")
    print(f"  Application ID: {status.app_id}")
    print(f"  State: {status.state}")
    print(f"  Final Status: {status.final_status}")
    print(f"  Listener Exit Code: {listener_thread.exit_code}")
    print("=" * 60)
    
    # 5. Если ошибка - получаем логи из YARN
    if status.error or status.final_status == "FAILED":
        print()
        log_info(">>> Получение логов драйвера из YARN...")
        
        # Убиваем listener с задержкой 5 секунд
        if listener_thread.is_alive():
            threading.Thread(target=listener_thread.stop, args=(5,), daemon=True).start()
        
        # Получаем логи
        if status.app_id:
            logs = yarn_fetch_driver_logs_via_ssh(status.app_id)
            print()
            print("=" * 60)
            print(f"ЛОГИ ДРАЙВЕРА ({status.app_id})")
            print("=" * 60)
            print(logs)
            print("=" * 60)
    
    # Ждём завершения потоков
    if listener_thread.is_alive():
        listener_thread.stop(delay=0)
    
    spark_thread.join(timeout=5)
    listener_thread.join(timeout=5)
    
    # 6. Выводим результат
    if status.success:
        print()
        log_info("✓ Приложение успешно завершено!")
        if status.app_id:
            print(f"  YARN UI: {YARN_RM_URL}/cluster/app/{status.app_id}")
            print(f"  Spark History: http://quickstart-bigdata:18088/history/{status.app_id}/jobs/")
        return 0
    else:
        print()
        log_error("✗ Приложение завершилось с ошибкой")
        return 1


if __name__ == "__main__":
    sys.exit(main())
