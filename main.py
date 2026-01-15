# main.py - ФИНАЛЬНЫЙ КОД ДЛЯ RAILWAY
import os
import time
import requests
import psycopg2
from datetime import datetime

# ========== КОНФИГУРАЦИЯ ==========
# Railway автоматически загружает переменные из Variables
DATABASE_URL = os.environ.get('DATABASE_URL')
BIRDEYE_API_KEY = os.environ.get('BIRDEYE_API_KEY')  # ДОБАВЬТЕ ЭТУ ПЕРЕМЕННУЮ В RAILWAY
NETWORK = "BSC"
# ==================================

def log(message):
    """Логирование с временем"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")

def setup_database():
    """Создаем таблицу в базе данных"""
    try:
        log("Подключаемся к базе данных...")
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        log("Проверяем подключение...")
        cur.execute("SELECT version()")
        db_version = cur.fetchone()[0]
        log(f"✅ Подключено: {db_version.split(',')[0]}")
        
        log("Создаем таблицу 'tokens'...")
        cur.execute('''
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
        
        log("Очищаем таблицу...")
        cur.execute("TRUNCATE tokens RESTART IDENTITY")
        
        conn.commit()
        cur.close()
        conn.close()
        log("✅ Таблица создана и очищена")
        return True
        
    except Exception as e:
        log(f"❌ Ошибка базы данных: {e}")
        return False

def get_birdeye_tokens():
    """Получаем токены через BirdEye API"""
    if not BIRDEYE_API_KEY:
        log("❌ BIRDEYE_API_KEY не найден!")
        log("   Добавьте переменную BIRDEYE_API_KEY в Railway Variables")
        return []
    
    log(f"Используем BirdEye API Key: {BIRDEYE_API_KEY[:8]}...")
    
    url = "https://public-api.birdeye.so/defi/token_list"
    headers = {"X-API-KEY": BIRDEYE_API_KEY}
    
    all_tokens = []
    
    # Получаем несколько страниц
    for page in range(5):  # 5 страниц по 100 токенов = 500 токенов
        params = {
            "sort_by": "liquidity",
            "sort_type": "desc",
            "offset": page * 100,
            "limit": 100,
            "chain": "bsc"
        }
        
        try:
            log(f"Запрос страницы {page + 1}...")
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get("success"):
                    tokens_data = data.get("data", {}).get("tokens", [])
                    
                    for token in tokens_data:
                        address = token.get("address", "").lower()
                        symbol = token.get("symbol", "UNKNOWN").upper()
                        name = token.get("name", symbol)
                        liquidity = float(token.get("liquidity", 0))
                        
                        if address and address.startswith("0x") and len(address) == 42:
                            all_tokens.append({
                                "token_address": address,
                                "symbol": symbol[:50],
                                "name": name[:200],
                                "liquidity_usd": liquidity
                            })
                    
                    log(f"   Получено: {len(tokens_data)} токенов")
                    
                    if len(tokens_data) < 100:
                        log("   Достигнут конец списка")
                        break
                        
                else:
                    log(f"❌ Ошибка API: {data.get('message', 'Unknown error')}")
                    break
            else:
                log(f"❌ HTTP ошибка: {response.status_code}")
                break
                
        except Exception as e:
            log(f"❌ Ошибка запроса: {e}")
            break
        
        # Небольшая задержка между запросами
        time.sleep(0.5)
    
    log(f"📊 Всего получено токенов: {len(all_tokens)}")
    return all_tokens[:1000]  # Ограничиваем 1000 токенов

def save_tokens(tokens):
    """Сохраняем токены в базу данных"""
    if not tokens:
        log("❌ Нет токенов для сохранения")
        return 0
    
    try:
        log("Подключаемся к базе для сохранения...")
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        saved_count = 0
        batch_size = 50
        
        # Сохраняем пачками
        for i in range(0, len(tokens), batch_size):
            batch = tokens[i:i + batch_size]
            values = []
            
            for token in batch:
                values.append((
                    NETWORK,
                    token['name'],
                    token['symbol'],
                    token['liquidity_usd'],
                    token['token_address']
                ))
            
            try:
                cur.executemany('''
                    INSERT INTO tokens (network, name, symbol, liquidity_usd, token_address)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (token_address) DO NOTHING
                ''', values)
                
                saved_count += cur.rowcount
                conn.commit()
                
                log(f"   Пакет {i//batch_size + 1}: {cur.rowcount} токенов")
                
            except Exception as e:
                log(f"   ⚠️ Ошибка пакета: {e}")
                conn.rollback()
                continue
        
        # Итоговая проверка
        cur.execute("SELECT COUNT(*) FROM tokens")
        total_in_db = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        log(f"✅ Сохранено новых токенов: {saved_count}")
        log(f"📊 Всего токенов в базе: {total_in_db}")
        
        return saved_count
        
    except Exception as e:
        log(f"❌ Ошибка сохранения: {e}")
        return 0

def show_sample_data():
    """Показываем пример данных из таблицы"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM tokens")
        total = cur.fetchone()[0]
        
        log(f"\n📈 ИТОГОВАЯ СТАТИСТИКА:")
        log(f"   Всего токенов в таблице: {total}")
        
        if total > 0:
            log("\n🏆 ТОП-5 токенов по ликвидности:")
            cur.execute("""
                SELECT symbol, name, liquidity_usd, token_address 
                FROM tokens 
                ORDER BY liquidity_usd DESC 
                LIMIT 5
            """)
            
            for i, row in enumerate(cur.fetchall(), 1):
                log(f"   {i}. {row[0]} ({row[1]})")
                log(f"      Ликвидность: ${row[2]:,.0f}")
                log(f"      Адрес: {row[3][:20]}...")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        log(f"❌ Ошибка при выводе данных: {e}")

def main():
    """Основная функция"""
    print("\n" + "=" * 70)
    log("🚀 ЗАПУСК BSC TOKEN COLLECTOR")
    print("=" * 70)
    
    # Проверяем наличие необходимых переменных
    if not DATABASE_URL:
        log("❌ КРИТИЧЕСКАЯ ОШИБКА: DATABASE_URL не найден!")
        log("   Убедитесь что переменная добавлена в Railway Variables")
        return
    
    if not BIRDEYE_API_KEY:
        log("❌ КРИТИЧЕСКАЯ ОШИБКА: BIRDEYE_API_KEY не найден!")
        log("   Добавьте переменную BIRDEYE_API_KEY в Railway Variables")
        log("   Значение: ваш API ключ от BirdEye")
        return
    
    start_time = time.time()
    
    try:
        # 1. Подготавливаем базу данных
        if not setup_database():
            return
        
        # 2. Получаем токены с BirdEye
        log("\n" + "=" * 50)
        log("🦅 ПОЛУЧЕНИЕ ДАННЫХ С BIRDЕYE")
        print("=" * 50)
        
        tokens = get_birdeye_tokens()
        
        if not tokens:
            log("❌ Не удалось получить токены")
            log("   Возможные причины:")
            log("   1. Неверный BIRDEYE_API_KEY")
            log("   2. Лимит запросов исчерпан")
            log("   3. Проблемы с сетью")
            return
        
        # 3. Сохраняем токены в базу
        log("\n" + "=" * 50)
        log("💾 СОХРАНЕНИЕ В БАЗУ ДАННЫХ")
        print("=" * 50)
        
        saved_count = save_tokens(tokens)
        
        if saved_count == 0:
            log("⚠️  Токены не были сохранены")
            log("   Возможно таблица уже содержит эти токены")
        
        # 4. Показываем результат
        show_sample_data()
        
        total_time = time.time() - start_time
        
        log("\n" + "=" * 70)
        if saved_count > 0:
            log(f"✅ УСПЕХ! За {total_time:.1f} секунд сохранено {saved_count} токенов")
        else:
            log(f"⚠️  ВНИМАНИЕ: Токены не сохранены (возможно уже есть в базе)")
        log("=" * 70)
        
    except Exception as e:
        log(f"💥 НЕОБРАБОТАННАЯ ОШИБКА: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()