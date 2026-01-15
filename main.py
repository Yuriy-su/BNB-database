import os
import sys
import time
import requests
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Конфигурация
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY')
DATABASE_URL = os.getenv('DATABASE_URL')
NETWORK = "BSC"  # Сеть Binance Smart Chain

# Инициализация пула соединений БД
db_pool = None

# ========== ОСНОВНЫЕ ФУНКЦИИ ==========

def init_database():
    """Инициализация соединения с БД и создание таблицы для токенов"""
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, DATABASE_URL)
        print("✅ Database connection pool created")
        
        # Создаем таблицу для токенов
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tokens (
                id SERIAL PRIMARY KEY,
                network VARCHAR(20) NOT NULL,
                name VARCHAR(200),
                symbol VARCHAR(50),
                liquidity_usd DECIMAL,
                token_address VARCHAR(255) UNIQUE NOT NULL,
                current_price DECIMAL,
                market_cap DECIMAL,
                total_volume DECIMAL,
                coin_id VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        conn.commit()
        cursor.close()
        db_pool.putconn(conn)
        print("✅ Table 'tokens' is ready")
        return True
        
    except Exception as e:
        print(f"❌ Database initialization error: {e}")
        return False

def get_liquid_tokens_from_coingecko(limit=1000):
    """
    Получает список ликвидных токенов BSC через CoinGecko API.
    Использует два запроса: сначала список, потом детали с адресами.
    """
    if not COINGECKO_API_KEY:
        print("❌ COINGECKO_API_KEY not set!")
        return []
    
    print(f"🔄 Starting data collection from CoinGecko...")
    
    # ШАГ 1: Получаем список токенов с основными метриками
    page = 1
    per_page = 250
    all_tokens = []
    
    while len(all_tokens) < limit:
        try:
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                'vs_currency': 'usd',
                'category': 'binance-smart-chain',
                'order': 'volume_desc',
                'per_page': per_page,
                'page': page,
                'sparkline': 'false',
                'x_cg_demo_api_key': COINGECKO_API_KEY
            }
            
            print(f"📥 Requesting page {page}...")
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 429:
                print("⚠️ Rate limit exceeded. Waiting 60 seconds...")
                time.sleep(60)
                continue
                
            if response.status_code != 200:
                print(f"❌ CoinGecko API error: {response.status_code}")
                break
                
            tokens = response.json()
            
            if not tokens:
                print("ℹ️ No more tokens available")
                break
            
            # Фильтруем только токены с объёмом торгов > 1000 USD
            for token in tokens:
                if token.get('total_volume', 0) > 1000:
                    all_tokens.append(token)
            
            print(f"✅ Received {len(tokens)} tokens (page {page})")
            
            time.sleep(7)  # Пауза для лимитов API
            page += 1
            
            if len(all_tokens) >= limit:
                all_tokens = all_tokens[:limit]
                break
                
        except Exception as e:
            print(f"❌ Error: {e}")
            break
    
    print(f"🎯 Got {len(all_tokens)} tokens. Now getting contract addresses...")
    
    # ШАГ 2: Для каждого токена получаем детали с адресом контракта
    tokens_with_addresses = []
    
    for i, token in enumerate(all_tokens):
        try:
            token_id = token.get('id')
            if not token_id:
                continue
            
            # Получаем детали токена
            details_url = f"https://api.coingecko.com/api/v3/coins/{token_id}"
            details_params = {
                'localization': 'false',
                'tickers': 'false',
                'market_data': 'false',
                'community_data': 'false',
                'developer_data': 'false',
                'sparkline': 'false',
                'x_cg_demo_api_key': COINGECKO_API_KEY
            }
            
            # Выводим прогресс каждые 50 токенов
            if i % 50 == 0:
                print(f"🔍 Getting contract addresses: {i}/{len(all_tokens)}...")
            
            response = requests.get(details_url, params=details_params, timeout=30)
            
            if response.status_code == 200:
                details = response.json()
                
                # Ищем адрес контракта для BSC
                platforms = details.get('platforms', {})
                contract_address = platforms.get('binance-smart-chain', '') or platforms.get('bsc', '')
                
                if contract_address:
                    # Добавляем адрес в данные токена
                    token['contract_address'] = contract_address.lower()
                    token['coin_id'] = token_id  # Сохраняем ID CoinGecko
                    tokens_with_addresses.append(token)
                else:
                    # Проверяем другие возможные ключи
                    for key in platforms:
                        if 'binance' in key.lower() or 'bsc' in key.lower():
                            token['contract_address'] = platforms[key].lower()
                            token['coin_id'] = token_id
                            tokens_with_addresses.append(token)
                            break
                    else:
                        print(f"  ⚠️ No BSC address found for {token.get('symbol')}")
            
            # Пауза между запросами (чтобы не превысить лимиты)
            time.sleep(0.7)  # 0.7 секунд между запросами
            
        except Exception as e:
            print(f"  ⚠️ Error getting details for {token.get('symbol')}: {e}")
            continue
    
    print(f"✅ Got contract addresses for {len(tokens_with_addresses)} tokens")
    return tokens_with_addresses

def save_tokens_to_db(tokens_data):
    """
    Сохраняет токены в базу данных.
    Возвращает количество успешно сохранённых токенов.
    """
    if not db_pool or not tokens_data:
        return 0
    
    saved_count = 0
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        for token in tokens_data:
            try:
                # Адрес контракта
                token_address = token.get('contract_address', '')
                
                if not token_address:
                    continue
                
                name = token.get('name', '')
                symbol = token.get('symbol', '')
                liquidity_usd = token.get('total_volume', 0)
                current_price = token.get('current_price', 0)
                market_cap = token.get('market_cap', 0)
                coin_id = token.get('coin_id', '')
                
                # Вставка или обновление записи
                cursor.execute('''
                    INSERT INTO tokens 
                    (network, name, symbol, liquidity_usd, token_address, 
                     current_price, market_cap, total_volume, coin_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (token_address) 
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        symbol = EXCLUDED.symbol,
                        liquidity_usd = EXCLUDED.liquidity_usd,
                        current_price = EXCLUDED.current_price,
                        market_cap = EXCLUDED.market_cap,
                        total_volume = EXCLUDED.total_volume,
                        coin_id = EXCLUDED.coin_id,
                        updated_at = NOW()
                ''', (
                    NETWORK, name, symbol, liquidity_usd, token_address,
                    current_price, market_cap, liquidity_usd, coin_id
                ))
                
                saved_count += 1
                
                if saved_count % 50 == 0:
                    print(f"  💾 Saved {saved_count} tokens...")
                    
            except Exception as e:
                print(f"  ⚠️ Error saving token {token.get('symbol')}: {e}")
                continue
        
        conn.commit()
        cursor.close()
        db_pool.putconn(conn)
        
    except Exception as e:
        print(f"❌ Database error: {e}")
    
    return saved_count

def display_token_stats(tokens):
    """Выводит статистику собранных токенов"""
    if not tokens:
        print("📊 No data for statistics")
        return
    
    print("\n📊 COLLECTED TOKEN STATISTICS:")
    print("-" * 50)
    
    # Топ-5 по ликвидности
    sorted_tokens = sorted(tokens, key=lambda x: x.get('total_volume', 0), reverse=True)
    
    print("Top 5 most liquid tokens:")
    for i, token in enumerate(sorted_tokens[:5], 1):
        symbol = token.get('symbol', 'N/A').upper()
        volume = token.get('total_volume', 0)
        price = token.get('current_price', 0)
        address = token.get('contract_address', 'N/A')[:20] + "..."
        print(f"  {i}. {symbol:8} - Volume: ${volume:,.0f} | Address: {address}")
    
    # Статистика
    total_volume = sum(t.get('total_volume', 0) for t in tokens)
    avg_volume = total_volume / len(tokens) if tokens else 0
    tokens_with_address = len([t for t in tokens if t.get('contract_address')])
    
    print(f"\n📈 General statistics:")
    print(f"   • Total tokens: {len(tokens)}")
    print(f"   • Tokens with BSC address: {tokens_with_address}")
    print(f"   • Total trading volume: ${total_volume:,.0f}")
    print(f"   • Average volume per token: ${avg_volume:,.0f}")
    print("-" * 50)

# ========== ОСНОВНАЯ ФУНКЦИЯ ==========

def main():
    """Основной рабочий процесс"""
    print("🚀 Starting BSC Token Collector via CoinGecko")
    print("=" * 60)
    
    # Проверяем обязательные переменные
    if not COINGECKO_API_KEY:
        print("❌ COINGECKO_API_KEY not found in Variables!")
        print("   Add it to Railway Variables")
        return
    
    if not DATABASE_URL:
        print("❌ DATABASE_URL not found!")
        return
    
    # Инициализируем БД
    if not init_database():
        print("❌ Failed to initialize database")
        return
    
    # Получаем токены с CoinGecko
    tokens = get_liquid_tokens_from_coingecko(limit=1000)
    
    if not tokens:
        print("❌ No tokens received")
        return
    
    # Показываем статистику
    display_token_stats(tokens)
    
    # Сохраняем в БД
    print("\n💾 Saving tokens to database...")
    saved_count = save_tokens_to_db(tokens)
    
    print(f"\n✅ COMPLETED!")
    print(f"   • Received tokens: {len(tokens)}")
    print(f"   • Saved to DB: {saved_count}")
    
    # Проверяем запись
    if db_pool and saved_count > 0:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM tokens")
            count = cursor.fetchone()[0]
            cursor.close()
            db_pool.putconn(conn)
            print(f"   • Total in database: {count} tokens")
            
            # Показываем топ-5 из базы
            cursor = conn.cursor()
            cursor.execute('''
                SELECT symbol, name, liquidity_usd, token_address 
                FROM tokens 
                ORDER BY liquidity_usd DESC 
                LIMIT 5
            ''')
            top_tokens = cursor.fetchall()
            cursor.close()
            
            print("\n🏆 Top 5 from database:")
            for i, (symbol, name, liquidity, address) in enumerate(top_tokens, 1):
                print(f"  {i}. {symbol} ({name[:20]}...): ${liquidity:,.0f}")
                
        except Exception as e:
            print(f"  ⚠️ Error checking database: {e}")
    
    print("\n🎯 Done! Check data in Postgres:")
    print("   SELECT * FROM tokens ORDER BY liquidity_usd DESC LIMIT 10;")
    print("\n⏳ Container will stay alive for 10 minutes...")

# ========== ЗАПУСК СКРИПТА ==========

print("=" * 60)
print("🔄 SCRIPT STARTING")
print("=" * 60)

try:
    main()
except KeyboardInterrupt:
    print("\n🛑 Script interrupted by user")
except Exception as e:
    print(f"\n❌ Unexpected error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("✅ SCRIPT FINISHED")
print("=" * 60)

# Держим контейнер живым для проверки логов
print("\n⏳ Container alive for 10 minutes...")
time.sleep(600)