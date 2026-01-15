import os
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
NETWORK = "BSC"

# Инициализация пула соединений БД
db_pool = None

# ========== БАЗА ДАННЫХ ==========

def init_database():
    """Инициализация соединения с БД"""
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, DATABASE_URL)
        print("✅ Database connection pool created")
        
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        # Просто проверяем что таблица существует
        cursor.execute("SELECT COUNT(*) FROM tokens")
        count = cursor.fetchone()[0]
        
        print(f"📊 Table 'tokens' exists with {count} records")
        
        cursor.close()
        db_pool.putconn(conn)
        print("✅ Database is ready")
        return True
        
    except Exception as e:
        print(f"❌ Database initialization error: {e}")
        return False

# ========== ПОЛУЧЕНИЕ ТОКЕНОВ ==========

def get_tokens_with_contract_addresses(limit=30):
    """Получает список токенов BSC с адресами контрактов"""
    if not COINGECKO_API_KEY:
        print("❌ COINGECKO_API_KEY not set!")
        return []
    
    print(f"🔄 Getting top {limit} BSC tokens...")
    
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            'vs_currency': 'usd',
            'category': 'binance-smart-chain',
            'order': 'volume_desc',
            'per_page': limit,
            'page': 1,
            'sparkline': 'false',
            'x_cg_demo_api_key': COINGECKO_API_KEY
        }
        
        print("📥 Requesting tokens list from CoinGecko...")
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ CoinGecko API error: {response.status_code}")
            return []
        
        tokens = response.json()
        print(f"✅ Received {len(tokens)} tokens")
        
    except Exception as e:
        print(f"❌ Error getting tokens list: {e}")
        return []
    
    # Получаем адреса контрактов
    tokens_with_addresses = []
    
    print(f"🔍 Getting contract addresses...")
    
    for i, token in enumerate(tokens):
        try:
            token_id = token.get('id')
            symbol = token.get('symbol', 'UNKNOWN').upper()
            
            if not token_id:
                continue
            
            if i % 5 == 0:
                print(f"  Processing {i+1}/{len(tokens)}...")
            
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
            
            details_response = requests.get(details_url, params=details_params, timeout=20)
            
            if details_response.status_code == 200:
                details = details_response.json()
                platforms = details.get('platforms', {})
                
                contract_address = None
                
                # Ищем BSC адрес
                bsc_keys = ['binance-smart-chain', 'bsc', 'binance']
                for key in bsc_keys:
                    if key in platforms and platforms[key]:
                        contract_address = platforms[key]
                        break
                
                if contract_address and isinstance(contract_address, str) and contract_address.startswith('0x'):
                    contract_address = contract_address.lower().strip()
                    token['contract_address'] = contract_address
                    tokens_with_addresses.append(token)
                    print(f"    ✓ {symbol}: found BSC address")
                else:
                    for key, address in platforms.items():
                        if address and isinstance(address, str) and address.startswith('0x'):
                            token['contract_address'] = address.lower().strip()
                            tokens_with_addresses.append(token)
                            print(f"    ⚠️ {symbol}: using {key} address")
                            break
                    else:
                        print(f"    ✗ {symbol}: no valid contract address found")
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"    ⚠️ Error processing {token.get('symbol', 'UNKNOWN')}: {e}")
    
    print(f"✅ Found {len(tokens_with_addresses)} tokens with valid contract addresses")
    return tokens_with_addresses

def save_tokens_to_database(tokens_data):
    """Сохраняет токены в базу данных"""
    if not db_pool or not tokens_data:
        print("⚠️ No tokens to save or no database connection")
        return 0
    
    saved_count = 0
    error_count = 0
    
    print(f"💾 Saving {len(tokens_data)} tokens to database...")
    
    for i, token in enumerate(tokens_data):
        try:
            token_address = token.get('contract_address', '').strip()
            symbol = token.get('symbol', 'UNKNOWN').upper()
            name = token.get('name', '')
            
            if not token_address or not token_address.startswith('0x'):
                print(f"  ⚠️ [{i+1}] {symbol}: Invalid address")
                error_count += 1
                continue
            
            liquidity_usd = float(token.get('total_volume', 0) or 0)
            
            conn = db_pool.getconn()
            cursor = conn.cursor()
            
            try:
                cursor.execute('''
                    INSERT INTO tokens 
                    (network, name, symbol, liquidity_usd, token_address)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (token_address) 
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        symbol = EXCLUDED.symbol,
                        liquidity_usd = EXCLUDED.liquidity_usd,
                        updated_at = NOW()
                ''', (
                    NETWORK,
                    name[:200],
                    symbol[:50],
                    liquidity_usd,
                    token_address
                ))
                
                conn.commit()
                saved_count += 1
                
                if saved_count % 5 == 0:
                    print(f"  ✅ Saved {saved_count} tokens...")
                
            except Exception as e:
                print(f"  ❌ [{i+1}] {symbol}: Database error - {e}")
                conn.rollback()
                error_count += 1
            finally:
                cursor.close()
                db_pool.putconn(conn)
                
        except Exception as e:
            print(f"  ⚠️ [{i+1}] Error processing token: {e}")
            error_count += 1
    
    print(f"📊 Save completed: {saved_count} saved, {error_count} failed")
    return saved_count

# ========== ЭКСПОРТ В CSV ==========

def export_tokens_to_csv():
    """Экспортирует все токены в CSV формат"""
    print("\n" + "=" * 80)
    print("📤 CSV ЭКСПОРТ ДАННЫХ")
    print("=" * 80)
    
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, network, name, symbol, 
                   liquidity_usd::numeric(30,2), 
                   token_address 
            FROM tokens 
            ORDER BY liquidity_usd DESC
        """)
        
        tokens = cursor.fetchall()
        
        print(f"📊 Всего токенов в базе: {len(tokens)}")
        print("\n" + "=" * 80)
        print("СКОПИРУЙТЕ ВСЁ НИЖЕ И СОХРАНИТЕ КАК tokens.csv")
        print("=" * 80)
        
        # Заголовки CSV
        print("id,network,name,symbol,liquidity_usd,token_address")
        
        # Данные
        for token in tokens:
            id_val, network, name, symbol, liquidity, address = token
            
            if ',' in str(name):
                name = f'"{name}"'
            if ',' in str(symbol):
                symbol = f'"{symbol}"'
            
            print(f"{id_val},{network},{name},{symbol},{liquidity},{address}")
        
        print("=" * 80)
        print(f"✅ Экспортировано {len(tokens)} токенов")
        
        cursor.close()
        db_pool.putconn(conn)
        
        return len(tokens)
        
    except Exception as e:
        print(f"❌ Ошибка экспорта: {e}")
        return 0

# ========== ОСНОВНАЯ ФУНКЦИЯ ==========

def main():
    """Основной рабочий процесс"""
    print("=" * 60)
    print("🚀 BSC Token Collector + Экспорт")
    print("=" * 60)
    
    if not COINGECKO_API_KEY:
        print("❌ ERROR: COINGECKO_API_KEY not found!")
        return
    
    if not DATABASE_URL:
        print("❌ ERROR: DATABASE_URL not found!")
        return
    
    # Инициализация базы данных
    print("\n🔧 Initializing database connection...")
    if not init_database():
        print("❌ Failed to initialize database")
        return
    
    # Получение токенов
    print("\n🌐 Fetching BSC tokens from CoinGecko...")
    tokens = get_tokens_with_contract_addresses(limit=30)
    
    if not tokens:
        print("❌ No tokens retrieved from CoinGecko")
        return
    
    # Сохранение в базу данных
    print(f"\n💾 Saving {len(tokens)} tokens to PostgreSQL...")
    saved_count = save_tokens_to_database(tokens)
    
    # Вывод результатов
    print("\n" + "=" * 60)
    print(f"🎯 COLLECTION COMPLETE: {saved_count} tokens saved")
    print("=" * 60)
    
    # Экспорт данных
    print("\n📤 Starting data export...")
    export_count = export_tokens_to_csv()
    
    print(f"\n⏱️ Total execution time: {time.strftime('%M:%S')}")

# ========== ЗАПУСК ==========

if __name__ == "__main__":
    print("🔄 Starting script...")
    start_time = time.time()
    
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
    
    elapsed_time = time.time() - start_time
    print(f"\n⏱️ Script finished in {elapsed_time:.1f} seconds")
    
    # Ждём чтобы скопировать данные
    print("\n⏳ Container active for 5 minutes...")
    time.sleep(300)