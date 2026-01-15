# main.py - АБСОЛЮТНО ПРОСТОЙ СКРИПТ
import os
import psycopg2

print("=" * 60)
print("🚀 НАЧАЛО РАБОТЫ СКРИПТА")
print("=" * 60)

# 1. Проверяем переменные окружения
print("1️⃣ Проверяем переменные окружения...")
DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    print("❌ ОШИБКА: DATABASE_URL не найден!")
    print("   Проверьте, что файл .env существует и содержит:")
    print("   DATABASE_URL=postgresql://user:password@host:port/database")
    exit(1)

print(f"✅ DATABASE_URL найден: {DATABASE_URL[:50]}...")

# 2. Подключаемся к базе
print("\n2️⃣ Подключаемся к базе данных...")
try:
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    print("✅ Подключение успешно!")
except Exception as e:
    print(f"❌ Ошибка подключения: {e}")
    exit(1)

# 3. Создаем таблицу (если не существует)
print("\n3️⃣ Создаем таблицу tokens...")
try:
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            id SERIAL PRIMARY KEY,
            network VARCHAR(20) NOT NULL,
            name VARCHAR(200),
            symbol VARCHAR(50),
            liquidity_usd DECIMAL,
            token_address VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    ''')
    conn.commit()
    print("✅ Таблица создана/проверена")
except Exception as e:
    print(f"❌ Ошибка создания таблицы: {e}")
    cursor.close()
    conn.close()
    exit(1)

# 4. Очищаем таблицу перед заполнением
print("\n4️⃣ Очищаем таблицу...")
try:
    cursor.execute("DELETE FROM tokens")
    conn.commit()
    print("✅ Таблица очищена")
except Exception as e:
    print(f"❌ Ошибка очистки таблицы: {e}")
    conn.rollback()

# 5. Вставляем тестовые данные
print("\n5️⃣ Вставляем тестовые данные...")
test_tokens = [
    ("BSC", "Wrapped BNB", "WBNB", 2500000000, "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"),
    ("BSC", "Tether USD", "USDT", 1500000000, "0x55d398326f99059ff775485246999027b3197955"),
    ("BSC", "USD Coin", "USDC", 1200000000, "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"),
    ("BSC", "PancakeSwap", "CAKE", 800000000, "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82"),
    ("BSC", "Dai Stablecoin", "DAI", 600000000, "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3"),
    ("BSC", "Polkadot", "DOT", 400000000, "0x7083609fce4d1d8dc0c979aab8c869ea2c873402"),
    ("BSC", "Ethereum", "ETH", 350000000, "0x2170ed0880ac9a755fd29b2688956bd959f933f8"),
    ("BSC", "Polygon", "MATIC", 300000000, "0xcc42724c6683b7e57334c4e856f4c9965ed682bd"),
    ("BSC", "Cardano", "ADA", 180000000, "0x3ee2200efb3400fabb9aacf31297cbdd1d435d47"),
    ("BSC", "Chainlink", "LINK", 120000000, "0xf8a0bf9cf54bb92f17374d9e9a321e6a111a51bd"),
]

inserted_count = 0
for token in test_tokens:
    try:
        cursor.execute('''
            INSERT INTO tokens (network, name, symbol, liquidity_usd, token_address)
            VALUES (%s, %s, %s, %s, %s)
        ''', token)
        inserted_count += 1
    except Exception as e:
        print(f"⚠️ Ошибка вставки {token[2]}: {e}")

conn.commit()
print(f"✅ Вставлено записей: {inserted_count}")

# 6. Проверяем результат
print("\n6️⃣ Проверяем результат...")
try:
    cursor.execute("SELECT COUNT(*) FROM tokens")
    total_count = cursor.fetchone()[0]
    print(f"✅ Всего записей в таблице: {total_count}")
    
    if total_count > 0:
        print("\n📋 Первые 5 записей:")
        cursor.execute("SELECT id, network, symbol, name, liquidity_usd FROM tokens ORDER BY id LIMIT 5")
        for row in cursor.fetchall():
            print(f"   {row[0]}. {row[1]}:{row[2]} - {row[3]} (${row[4]:,.0f})")
    else:
        print("❌ Таблица пустая!")
        
except Exception as e:
    print(f"❌ Ошибка проверки: {e}")

# 7. Закрываем соединение
print("\n7️⃣ Закрываем соединение...")
cursor.close()
conn.close()
print("✅ Соединение закрыто")

print("\n" + "=" * 60)
print("🎯 СКРИПТ ВЫПОЛНЕН УСПЕШНО!")
print("=" * 60)