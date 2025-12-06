#!/usr/bin/env bash
# Скрипт для исправления virtualenv: замена симлинков на реальные файлы
# Это необходимо для переносимости архива на кластер, т.к. Spark не распаковывает симлинки корректно

set -e

if [ -z "$1" ]; then
    echo "Использование: $0 <путь_к_venv.zip> [выходной_файл.zip]"
    echo ""
    echo "Примеры:"
    echo "  $0 venv_py37.zip                    # Перезаписать исходный файл"
    echo "  $0 venv_py37.zip venv_py37_fixed.zip # Создать новый файл"
    exit 1
fi

INPUT_ZIP="$1"
OUTPUT_ZIP="${2:-$1}"  # Если не указан выходной файл - перезаписываем исходный

if [ ! -f "$INPUT_ZIP" ]; then
    echo "Ошибка: файл $INPUT_ZIP не найден"
    exit 1
fi

# Создаём временную директорию
TEMP_DIR=$(mktemp -d)
echo ">>> Распаковка $INPUT_ZIP в $TEMP_DIR..."
unzip -q "$INPUT_ZIP" -d "$TEMP_DIR"

# Считаем симлинки
SYMLINK_COUNT=$(find "$TEMP_DIR" -type l | wc -l)
echo ">>> Найдено симлинков: $SYMLINK_COUNT"

if [ "$SYMLINK_COUNT" -eq 0 ]; then
    echo ">>> Симлинков нет, архив уже исправлен"
    rm -rf "$TEMP_DIR"
    exit 0
fi

# Заменяем симлинки на реальные файлы
echo ">>> Замена симлинков на реальные файлы..."
REPLACED=0
find "$TEMP_DIR" -type l | while read link; do
    # Получаем абсолютный путь к целевому файлу
    target=$(readlink -f "$link" 2>/dev/null)
    
    if [ -f "$target" ]; then
        # Удаляем симлинк и копируем реальный файл
        rm "$link"
        cp "$target" "$link"
        ((REPLACED++)) || true
    elif [ -d "$target" ]; then
        # Для директорий - копируем рекурсивно
        rm "$link"
        cp -r "$target" "$link"
        ((REPLACED++)) || true
    else
        echo "  [WARN] Не удалось разрешить симлинк: $link -> $(readlink "$link")"
    fi
done

# Проверяем, что симлинков больше нет
REMAINING=$(find "$TEMP_DIR" -type l | wc -l)
echo ">>> Осталось симлинков: $REMAINING"

# Создаём новый архив
echo ">>> Создание $OUTPUT_ZIP..."
if [ "$INPUT_ZIP" = "$OUTPUT_ZIP" ]; then
    # Если перезаписываем - сначала удаляем старый
    rm -f "$INPUT_ZIP"
fi

cd "$TEMP_DIR"
zip -rq "$OUTPUT_ZIP" .
cd - > /dev/null

# Очищаем временную директорию
rm -rf "$TEMP_DIR"

# Показываем результат
OUTPUT_SIZE=$(ls -lh "$OUTPUT_ZIP" | awk '{print $5}')
echo ""
echo "✓ Готово: $OUTPUT_ZIP ($OUTPUT_SIZE)"
echo "  Симлинков заменено: $SYMLINK_COUNT"
