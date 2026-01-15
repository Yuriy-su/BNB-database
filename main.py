# diagnostic.py - ПОЛНАЯ ДИАГНОСТИКА
import os
import sys
import psycopg2
import requests

print("=" * 80)
print("🔴 ЭКСТРЕННАЯ ДИАГНОСТИКА ПРОБЛЕМЫ")
print("=" * 80)

print("1️⃣ БАЗОВАЯ ИНФОРМАЦИЯ:")
print(f"   Python: {sys.version}")
print(f"   Рабочая директория: {os.getcwd()}")
print(f"   Файлы в директории: {os.listdir('.')}")

print("\n2️⃣ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ:")
print("   Все переменные:")
for key in sorted(os.environ.keys()):
    value = os.environ[key]
    # Показываем только важные или все
    if any(x in key.lower() for x in ['db', 'pg', 'sql', 'api', 'key', 'url', 'pass']):
        masked = value[:30] + '...' if len(value) > 30 else value
        print(f"   {key:25} = {masked}")

print("\n3️⃣ КРИТИЧЕСКИЕ ПЕРЕМЕННЫЕ:")
DATABASE_URL = os.environ.get('DATABASE_URL')
COINGECKO_API_KEY = os.environ.get('COINGECKO_API_KEY')

print(f"   DATABASE_URL: {'✅ НАЙДЕН' if DATABASE_URL else '❌ НЕ НАЙДЕН'}")
if DATABASE_URL:
    print(f"      Пример: {DATABASE_URL[:50]}...")

print(f"   COINGECKO_API_KEY: {'✅ НАЙДЕН' if COINGECKO_API_KEY else '❌ НЕ НАЙДЕН'}")
if COINGECKO_API_KEY:
    print(f"      Длина: {len(COINGECKO_API_KEY)} символов")

print("\n4️⃣ ПРОВЕРКА ПОДКЛЮЧЕНИЯ К БАЗЕ:")
if DATABASE_URL:
    try:
        print(f"   Пробуем подключиться к: {DATABASE_URL.split('@')[-1] if '@' in DATABASE_URL else DATABASE_URL[:50]}...")
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        cur = conn.cursor()
        
        # Простой запрос
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
        print(f"   ✅ PostgreSQL подключен: {version.split(',')[0]}")
        
        # Проверяем таблицу tokens
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tokens')")
        exists = cur.fetchone()[0]
        print(f"   📊 Таблица 'tokens': {'✅ СУЩЕСТВУЕТ' if exists else '❌ НЕ СУЩЕСТВУЕТ'}")
        
        if exists:
            cur.execute("SELECT COUNT(*) FROM tokens")
            count = cur.fetchone()[0]
            print(f"   📊 Записей в таблице: {count}")
            
            # Структура таблицы
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'tokens' 
                ORDER BY ordinal_position
            """)
            columns = cur.fetchall()
            print(f"   📋 Столбцы ({len(columns)}):")
            for name, dtype in columns:
                print(f"      - {name} ({dtype})")
        
        cur.close()
        conn.close()
        print("   ✅ Подключение к базе - УСПЕХ")
        
    except psycopg2.OperationalError as e:
        print(f"   ❌ ОШИБКА ПОДКЛЮЧЕНИЯ: {e}")
        print(f"   Проблема с DATABASE_URL: {DATABASE_URL[:100]}")
    except Exception as e:
        print(f"   ❌ ДРУГАЯ ОШИБКА: {type(e).__name__}: {e}")
else:
    print("   ⚠️ Пропускаем - нет DATABASE_URL")

print("\n5️⃣ ПРОВЕРКА COINGECKO API:")
if COINGECKO_API_KEY:
    try:
        print("   Тестируем подключение к CoinGecko...")
        response = requests.get("https://api.coingecko.com/api/v3/ping", timeout=10)
        print(f"   📡 Статус подключения: {response.status_code} ({'✅ OK' if response.status_code == 200 else '❌ ERROR'})")
        
        if response.status_code == 200:
            # Тестируем ключ
            test_url = "https://api.coingecko.com/api/v3/coins/bitcoin"
            test_response = requests.get(test_url, params={'x_cg_demo_api_key': COINGECKO_API_KEY}, timeout=10)
            print(f"   🔑 API Key статус: {test_response.status_code} ({'✅ РАБОТАЕТ' if test_response.status_code == 200 else '❌ НЕ РАБОТАЕТ'})")
            
            if test_response.status_code != 200:
                print(f"   ❗ Ответ от CoinGecko: {test_response.text[:200]}")
    except Exception as e:
        print(f"   ❌ Ошибка проверки API: {type(e).__name__}: {e}")
else:
    print("   ⚠️ Пропускаем - нет COINGECKO_API_KEY")

print("\n6️⃣ ПРОБНЫЙ ТЕСТ - СОЗДАЕМ ТАБЛИЦУ:")
if DATABASE_URL:
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        print("   Шаг 1: Удаляем старую таблицу...")
        cur.execute('DROP TABLE IF EXISTS tokens;')
        
        print("   Шаг 2: Создаем новую таблицу...")
        cur.execute('''
            CREATE TABLE tokens (
                id SERIAL PRIMARY KEY,
                network VARCHAR(20) NOT NULL,
                name VARCHAR(200),
                symbol VARCHAR(50),
                liquidity_usd DECIMAL,
                token_address VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        print("   Шаг 3: Добавляем тестовую запись...")
        cur.execute('''
            INSERT INTO tokens (network, name, symbol, liquidity_usd, token_address)
            VALUES (%s, %s, %s, %s, %s)
        ''', ('BSC', 'Wrapped BNB', 'WBNB', 2500000.50, '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'))
        
        conn.commit()
        
        print("   Шаг 4: Проверяем...")
        cur.execute("SELECT COUNT(*) FROM tokens")
        count = cur.fetchone()[0]
        
        if count > 0:
            print(f"   ✅ ТЕСТ ПРОЙДЕН! В таблице {count} записей")
            cur.execute("SELECT id, network, symbol, name FROM tokens")
            for row in cur.fetchall():
                print(f"      📍 {row}")
        else:
            print("   ❌ ТЕСТ ПРОВАЛЕН - таблица пустая")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"   ❌ ОШИБКА ТЕСТА: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
else:
    print("   ⚠️ Пропускаем - нет DATABASE_URL")

print("\n" + "=" * 80)
print("📊 ИТОГИ ДИАГНОСТИКИ:")
print("=" * 80)

if not DATABASE_URL:
    print("❌ ПРОБЛЕМА: DATABASE_URL не найден")
    print("   Решение: Добавьте в Railway Variables")

if not COINGECKO_API_KEY:
    print("❌ ПРОБЛЕМА: COINGECKO_API_KEY не найден")
    print("   Решение: Добавьте ваш ключ CoinGecko")

if DATABASE_URL and COINGECKO_API_KEY:
    print("✅ Все переменные найдены")
    print("   Проблема в коде или подключении")

print("\n🔧 Рекомендации:")
print("1. Проверьте Railway Logs для детальных ошибок")
print("2. Убедитесь что DATABASE_URL начинается с 'postgresql://'")
print("3. Проверьте что COINGECKO_API_KEY активен")
print("4. Перезапустите deployment в Railway")

print("=" * 80)
print("🏁 ДИАГНОСТИКА ЗАВЕРШЕНА")
print("=" * 80)