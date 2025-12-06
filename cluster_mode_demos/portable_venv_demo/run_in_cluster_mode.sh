#!/usr/bin/env bash
# Скрипт для запуска Spark-приложения в cluster-режиме с real-time логами драйвера
# Используем Socket-подход: драйвер подключается к слушателю и отправляет логи напрямую

set -e

BASEDIR=$(dirname "$0")
cd "$BASEDIR"

source $BASEDIR/../../.venv/bin/activate
export HADOOP_USER_NAME=osboxes
export HADOOP_CONF_DIR=/home/felix/Projects/spark-cluster-mode-demos/cluster_mode_demos/portable_venv_demo/hadoop_configs/quickstart-bigdata

# Настройки для socket-логирования
LOG_PORT=9999
LOG_HOST=$(hostname -I | awk '{print $1}')  # IP хост-машины, видимый из VM

# Настройки приложения
APP_NAME="portable-venv-demo-$(date +%s)"
LOG_FILE="/tmp/spark-submit-${APP_NAME}.log"

echo "=== Запуск Spark-приложения в cluster-режиме с Socket-логами ==="
echo "Application Name: $APP_NAME"
echo "Log Host: $LOG_HOST:$LOG_PORT"
echo ""

# Функция для получения статуса приложения через REST API
get_app_state() {
    curl -s "http://quickstart-bigdata:8088/ws/v1/cluster/apps/$1" 2>/dev/null | \
        grep -oP '"state"\s*:\s*"[^"]+' | head -1 | grep -oP ':\s*"\K[^"]+'
}

# Функция для получения finalStatus приложения
get_app_final_status() {
    curl -s "http://quickstart-bigdata:8088/ws/v1/cluster/apps/$1" 2>/dev/null | \
        grep -oP '"finalStatus"\s*:\s*"[^"]+' | head -1 | grep -oP ':\s*"\K[^"]+'
}

# 0. Убиваем старый слушатель, если есть
bash "$BASEDIR/kill_listener.sh" 2>/dev/null || true

# 1. Запускаем слушатель логов в фоне
echo ">>> Запуск слушателя логов на порту $LOG_PORT..."
python3 "$BASEDIR/log_listener.py" --port "$LOG_PORT" --timeout 120 --exit-on-disconnect &
LISTENER_PID=$!
sleep 1

# Проверяем, что слушатель запустился
if ! kill -0 $LISTENER_PID 2>/dev/null; then
    echo "✗ Не удалось запустить слушатель логов"
    exit 1
fi
echo "✓ Слушатель запущен (PID: $LISTENER_PID)"
echo ""

# 2. Запускаем spark-submit БЕЗ ожидания завершения
#export PYSPARK_PYTHON=./environment/bin/python
echo ">>> Отправка Spark-приложения..."
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --name "$APP_NAME" \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://quickstart-bigdata:8020/user/spark/applicationHistory \
    --conf spark.yarn.submit.waitAppCompletion=false \
    --conf spark.yarn.maxAppAttempts=1 \
    --conf "spark.yarn.appMasterEnv.LOG_HOST=$LOG_HOST" \
    --conf "spark.yarn.appMasterEnv.LOG_PORT=$LOG_PORT" \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
    --archives etl_repo.zip#etl_repo,venv_py37.zip#environment \
    cluster_entrypoint.py 2>&1 | tee "$LOG_FILE"

# Извлекаем Application ID
APP_ID=$(grep -oP 'application_\d+_\d+' "$LOG_FILE" | head -1)

if [ -z "$APP_ID" ]; then
    echo "✗ Не удалось получить Application ID"
    kill $LISTENER_PID 2>/dev/null || true
    exit 1
fi

echo ""
echo "✓ Application ID: $APP_ID"
echo "  YARN UI: http://quickstart-bigdata:8088/cluster/app/$APP_ID"
echo "  Spark History: http://quickstart-bigdata:18088/history/$APP_ID/jobs/"
echo ""

# 3. Ждём завершения слушателя (драйвер подключился, отработал и отключился)
echo ">>> Ожидание логов от драйвера (таймаут: 120 сек)..."
echo ""

wait $LISTENER_PID
LISTENER_EXIT_CODE=$?

echo ""

# 4. Проверяем статус приложения через YARN
echo ">>> Проверка статуса приложения..."

# Ждём финального статуса (может быть небольшая задержка)
for i in {1..10}; do
    STATE=$(get_app_state "$APP_ID")
    FINAL_STATUS=$(get_app_final_status "$APP_ID")
    
    if [[ "$STATE" == "FINISHED" || "$STATE" == "FAILED" || "$STATE" == "KILLED" ]]; then
        break
    fi
    sleep 1
done

echo ""
echo "=============================================="
echo "РЕЗУЛЬТАТ:"
echo "  Application ID: $APP_ID"
echo "  State: $STATE"
echo "  Final Status: $FINAL_STATUS"
echo "  Listener Exit Code: $LISTENER_EXIT_CODE"
echo "=============================================="

# Определяем итоговый код выхода
if [[ "$FINAL_STATUS" == "SUCCEEDED" && "$LISTENER_EXIT_CODE" == "0" ]]; then
    echo ""
    echo "✓ Приложение успешно завершено!"
    exit 0
else
    echo ""
    echo "✗ Приложение завершилось с ошибкой"
    exit 1
fi
