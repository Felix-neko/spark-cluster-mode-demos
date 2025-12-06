#!/bin/bash
set -e

# Деактивируем окружение, если оно активировано
if command -v deactivate &> /dev/null; then
    deactivate || true
fi

# Удаляем старые окружения, если существуют
rm -rf mega_portable_venv mega_portable_venv_ported

# Показываем версию Python
python3.7 --version
which python3.7

# Создаём виртуальное окружение с помощью стандартного venv (без pip)
echo "Создание виртуального окружения..."
python3.7 -m venv --without-pip mega_portable_venv

# Устанавливаем pip вручную
echo "Установка pip..."
curl -sS https://bootstrap.pypa.io/pip/3.7/get-pip.py | mega_portable_venv/bin/python3

# Устанавливаем пакеты
echo "Установка пакетов..."
mega_portable_venv/bin/pip3 install pyspark==3.4.3 more_itertools dill==0.3.5.1 boto3

# Преобразуем симлинки Python в реальные файлы для переносимости
echo "Преобразование симлинков Python в реальные файлы..."
for pybin in python python3 python3.7; do
    if [ -L "mega_portable_venv/bin/$pybin" ]; then
        echo "  Копирование: $pybin"
        cp -L "mega_portable_venv/bin/$pybin" "mega_portable_venv/bin/${pybin}.real"
        rm "mega_portable_venv/bin/$pybin"
        mv "mega_portable_venv/bin/${pybin}.real" "mega_portable_venv/bin/$pybin"
        chmod +x "mega_portable_venv/bin/$pybin"
    fi
done

# Проверяем, что boto3 импортируется
echo "Проверка импорта boto3..."
mega_portable_venv/bin/python3 -c "import boto3; print('✓ boto3 успешно импортирован, версия:', boto3.__version__)"

# Проверка 1: Проверяем, что в bin/ нет симлинков
echo ""
echo "Проверка 1: Поиск симлинков в bin/..."
SYMLINKS=$(find mega_portable_venv/bin -type l)
if [ -z "$SYMLINKS" ]; then
    echo "✓ В bin/ нет симлинков"
else
    echo "✗ Найдены симлинки в bin/:"
    echo "$SYMLINKS"
    exit 1
fi

# Проверка 2: Тест переносимости
echo ""
echo "Проверка 2: Тест переносимости виртуального окружения..."
rm -rf mega_portable_venv_ported
cp -r mega_portable_venv mega_portable_venv_ported
rm -rf mega_portable_venv

# Исправляем shebang-и в скриптах перенесённого окружения
echo "Исправление shebang-ов в перенесённом окружении..."
CURRENT_DIR=$(pwd)
for script in mega_portable_venv_ported/bin/*; do
    if [ -f "$script" ] && [ -x "$script" ]; then
        # Проверяем, есть ли shebang с абсолютным путём к старому окружению
        if head -1 "$script" 2>/dev/null | grep -q "^#!.*mega_portable_venv/bin/python"; then
            # Заменяем старый путь на новый абсолютный путь
            sed -i "1s|^#!.*/mega_portable_venv/bin/python.*|#!${CURRENT_DIR}/mega_portable_venv_ported/bin/python3|" "$script"
            echo "  Исправлен: $(basename $script)"
        fi
    fi
done

# Устанавливаем дополнительный пакет в перенесённое окружение
echo "Установка дополнительного пакета (requests) в перенесённое окружение..."
mega_portable_venv_ported/bin/pip3 install requests

# Проверяем импорты в перенесённом окружении
echo "Проверка импортов в перенесённом окружении..."
mega_portable_venv_ported/bin/python3 -c "
import boto3
import requests
import pyspark
import more_itertools
import dill
print('✓ Все пакеты успешно импортированы:')
print('  - boto3:', boto3.__version__)
print('  - requests:', requests.__version__)
print('  - pyspark:', pyspark.__version__)
print('  - more_itertools:', more_itertools.__version__)
print('  - dill:', dill.__version__)
"

echo ""
echo "✓ Виртуальное окружение создано и успешно протестировано на переносимость"
echo "✓ Результат: mega_portable_venv_ported/"