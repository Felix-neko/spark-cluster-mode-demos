# Руководство по запуску Spark в кластерном режиме

## Обзор

Этот гайд описывает, как запускать PySpark-приложения в кластерном режиме (cluster mode) на YARN с полным логированием.

## Что было исправлено

### Проблема 1: "User did not initialize spark context"
**Причина**: `cluster_entrypoint.py` не создавал SparkSession/SparkContext  
**Решение**: Добавлено создание SparkSession в `cluster_entrypoint.py`

### Проблема 2: Отсутствие логов драйвера
**Причина**: В cluster mode логи драйвера находятся на кластере, а не на клиентской машине  
**Решение**: Добавлено получение логов через `yarn logs -applicationId <APP_ID>`

### Проблема 3: Логи не попадают в Spark History Server
**Причина**: Не была настроена директория Event Log  
**Решение**: Добавлены конфигурации:
```bash
--conf spark.eventLog.enabled=true
--conf spark.eventLog.dir=hdfs://quickstart-bigdata:8020/user/spark/applicationHistory
```

## Как использовать

### Запуск приложения

```bash
cd /home/felix/Projects/spark-cluster-mode-demos/cluster_mode_demos/portable_venv_demo
bash run_in_cluster_mode.sh
```

### Что делает скрипт

1. **Отправляет приложение на кластер** в cluster mode
2. **Ждёт завершения** приложения (`--conf spark.yarn.submit.waitAppCompletion=true`)
3. **Сохраняет логи** в `/tmp/spark-submit-<APP_NAME>.log`
4. **Получает stdout драйвера** через `yarn logs`
5. **Выводит ссылки** на YARN UI и Spark History Server

### Просмотр логов

#### 1. Логи драйвера (stdout/stderr)

Автоматически выводятся после завершения приложения:
```bash
=== Получение логов драйвера из YARN ===
```

Или вручную:
```bash
ssh osboxes@quickstart-bigdata "yarn logs -applicationId <APP_ID>"
```

#### 2. Логи экзекутора

**Вариант A: Через Spark History Server** (рекомендуется)
1. Откройте: `http://quickstart-bigdata:18088/history/<APP_ID>/jobs/`
2. Перейдите в раздел **Executors**
3. Нажмите на ссылку **stderr** или **stdout** для нужного экзекутора

**Вариант B: Через YARN CLI**
```bash
ssh osboxes@quickstart-bigdata "yarn logs -applicationId <APP_ID>" | grep -A 100 "container.*executor"
```

#### 3. Логи в реальном времени

Во время выполнения приложения:
```bash
# Откройте YARN Application UI
http://quickstart-bigdata:8088/cluster/app/<APP_ID>

# Нажмите на "ApplicationMaster" -> "logs"
```

## Структура проекта

```
portable_venv_demo/
├── run_in_cluster_mode.sh          # Скрипт запуска
├── cluster_entrypoint.py           # Точка входа приложения
├── etl_repo.zip                    # Архив с Python-кодом
└── hadoop_configs/                 # Конфигурация подключения к кластеру
    └── quickstart-bigdata/
        ├── core-site.xml
        ├── yarn-site.xml
        └── ...
```

## Ключевые параметры spark-submit

```bash
spark-submit \
    --master yarn \                                    # Использовать YARN
    --deploy-mode cluster \                            # Драйвер на кластере
    --name "portable-venv-demo" \                      # Имя приложения
    --conf spark.eventLog.enabled=true \               # Включить Event Log
    --conf spark.eventLog.dir=hdfs://.../applicationHistory \  # Директория для логов
    --conf spark.yarn.submit.waitAppCompletion=true \  # Ждать завершения
    --conf spark.yarn.maxAppAttempts=1 \               # Не перезапускать при ошибке
    --py-files etl_repo.zip \                          # Python-зависимости
    cluster_entrypoint.py                              # Точка входа
```

## Отличия cluster mode от client mode

| Параметр | Client Mode | Cluster Mode |
|----------|-------------|--------------|
| Где запускается драйвер | На клиентской машине | На кластере (в YARN AM) |
| Логи драйвера | В терминале клиента | На кластере (через `yarn logs`) |
| Сетевое подключение | Требуется постоянное | Не требуется после отправки |
| Использование | Интерактивная работа | Продакшн-задачи |

## Полезные команды

### Проверка статуса приложения
```bash
ssh osboxes@quickstart-bigdata "yarn application -status <APP_ID>"
```

### Остановка приложения
```bash
ssh osboxes@quickstart-bigdata "yarn application -kill <APP_ID>"
```

### Просмотр всех приложений
```bash
ssh osboxes@quickstart-bigdata "yarn application -list"
```

### Проверка Event Log в HDFS
```bash
ssh osboxes@quickstart-bigdata "hdfs dfs -ls /user/spark/applicationHistory"
```

## Troubleshooting

### Приложение падает с "User did not initialize spark context"
- Убедитесь, что в `cluster_entrypoint.py` создаётся SparkSession
- Проверьте, что `spark.stop()` вызывается в конце

### Логи не появляются в Spark History Server
- Проверьте, что директория `/user/spark/applicationHistory` существует в HDFS
- Убедитесь, что Spark History Server запущен и настроен на эту директорию
- Проверьте права доступа к директории

### SSH-подключение не работает
- Добавьте хост в `~/.ssh/known_hosts`:
  ```bash
  ssh-keyscan quickstart-bigdata >> ~/.ssh/known_hosts
  ```
- Настройте SSH-ключи или используйте пароль

## Ссылки

- **YARN ResourceManager UI**: http://quickstart-bigdata:8088/
- **Spark History Server**: http://quickstart-bigdata:18088/
- **HDFS NameNode UI**: http://quickstart-bigdata:50070/
