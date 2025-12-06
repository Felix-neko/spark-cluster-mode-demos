#!/usr/bin/env bash
set -e  # Прервать выполнение при ошибке

BASEDIR=$(dirname "$0")
cd "$BASEDIR"  # Перейти в директорию скрипта

source $BASEDIR/../../.venv/bin/activate
export HADOOP_USER_NAME=osboxes
export HADOOP_CONF_DIR=/home/felix/Projects/spark-cluster-mode-demos/cluster_mode_demos/portable_venv_demo/hadoop_configs/quickstart-bigdata

# Настройки для логирования и мониторинга
APP_NAME="portable-venv-demo-$(date +%s)"

echo "=== Запуск Spark-приложения в cluster-режиме ==="
echo "Application Name: $APP_NAME"
echo "HADOOP_CONF_DIR: $HADOOP_CONF_DIR"
echo ""

# Запуск spark-submit с настройками для логирования
# Перенаправляем вывод в файл и одновременно показываем на экран
LOG_FILE="/tmp/spark-submit-${APP_NAME}.log"

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --name "$APP_NAME" \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://quickstart-bigdata:8020/user/spark/applicationHistory \
    --conf spark.yarn.submit.waitAppCompletion=true \
    --conf spark.yarn.maxAppAttempts=1 \
    --py-files etl_repo.zip \
    cluster_entrypoint.py 2>&1 | tee "$LOG_FILE"

SUBMIT_EXIT_CODE=${PIPESTATUS[0]}

# Извлекаем Application ID из лог-файла
APP_ID=$(grep -oP 'application_\d+_\d+' "$LOG_FILE" | head -1)

echo ""
echo "=== Приложение завершено с кодом: $SUBMIT_EXIT_CODE ==="
echo "Application ID: $APP_ID"
echo "Логи сохранены в: $LOG_FILE"

if [ -n "$APP_ID" ]; then
    echo ""
    echo "=== Получение логов драйвера из YARN ==="
    echo "Получение stdout драйвера..."
    ssh -o StrictHostKeyChecking=no osboxes@quickstart-bigdata "yarn logs -applicationId $APP_ID" 2>&1 | grep -A 200 "LogType:stdout" | head -150
    
    echo ""
    echo "=== Ссылки для просмотра логов ==="
    echo "YARN Application UI: http://quickstart-bigdata:8088/cluster/app/$APP_ID"
    echo "Spark History Server: http://quickstart-bigdata:18088/history/$APP_ID/jobs/"
    echo ""
    echo "Для просмотра логов экзекутора откройте Spark History Server и перейдите в раздел 'Executors'"
fi
