# export_data.py
import os
import sys
import psycopg2
import csv
from datetime import datetime

print("🚀 Начинаем экспорт данных...")

# Получаем DATABASE_URL
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    print("❌ ОШИБКА: DATABASE_URL не найден!")
    sys.exit(1)

try:
    # Подключаемся
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # 1. Получаем данные
    print("📊 Получаем токены из базы...")
    cursor.execute("SELECT * FROM tokens ORDER BY liquidity_usd DESC")
    tokens = cursor.fetchall()
    
    print(f"✅ Найдено токенов: {len(tokens)}")
    
    # 2. Получаем названия столбцов
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'tokens' 
        ORDER BY ordinal_position
    """)
    columns = [row[0] for row in cursor.fetchall()]
    
    print(f"📋 Столбцы: {', '.join(columns)}")
    
    # 3. Выводим в консоль (можно скопировать)
    print("\n" + "=" * 100)
    print("ДАННЫЕ ДЛЯ КОПИРОВАНИЯ:")
    print("=" * 100)
    
    # Заголовки
    print(" | ".join(columns))
    print("-" * 100)
    
    # Данные (первые 20 строк)
    for i, row in enumerate(tokens[:20]):
        print(f"{i+1:3} | " + " | ".join(str(x) for x in row))
    
    # 4. Сохраняем в файл CSV
    filename = f"tokens_export_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)  # Заголовки
        writer.writerows(tokens)  # Данные
    
    print(f"\n💾 Сохранено в файл: {filename}")
    print(f"📁 Файл создан в той же папке что и скрипт")
    
    cursor.close()
    conn.close()
    
    print("\n✅ Экспорт завершён успешно!")
    print("📋 Скопируйте данные из логов выше или откройте CSV файл в Excel")
    
except Exception as e:
    print(f"❌ Ошибка: {e}")
    import traceback
    traceback.print_exc()