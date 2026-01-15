import os
import sys
import time
import requests
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY')
DATABASE_URL = os.getenv('DATABASE_URL')
NETWORK = "BSC"

db_pool = None

def init_database():
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, DATABASE_URL)
        print("✅ Database connection pool created")
        
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
        print(f"❌ Database error: {e}")
        return False

def get_liquid_tokens_fast(limit=100):
    """Получает топ ликвидных токенов с адресами"""
    if not COINGECKO_API_KEY:
        print("❌ COINGECKO_API_KEY not set!")
        return []
    
    print(f"🔄 Collecting top {limit} liquid tokens...")
    
    # 1. Получаем список токенов
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
    
    try:
        print("📥 Requesting tokens list...")
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ API error: {response.status_code}")
            return []
        
        tokens = response.json()
        print(f"✅ Received {len(tokens)} tokens")
        
        # 2. Получаем адреса для каждого токена
        tokens_with_addresses = []
        
        for i, token in enumerate(tokens[:50]):  # Только первые 50 для скорости
            try:
                token_id = token.get('id')
                if not token_id:
                    continue
                
                if i % 10 == 0:
                    print(f"🔍 Getting address {i+1}/50...")
                
                # Быстрый запрос для адреса
                details_url = f"https://api.coingecko.com/api/v3/coins/{token_id}"
                details_params = {
                    'localization': 'false',
                    'market_data': 'false',
                    'x_cg_demo_api_key': COINGECKO_API_KEY
                }
                
                details_response = requests.get(details_url, params=details_params, timeout=15)
                
                if details_response.status_code == 200:
                    details = details_response.json()
                    platforms = details.get('platforms', {})
                    
                    # Ищем BSC адрес
                    contract_address = None
                    for key in platforms:
                        if 'binance' in key.lower() or 'bsc' in key.lower():
                            contract_address = platforms[key]
                            break
                    
                    if contract_address and contract_address.startswith('0x'):
                        token['contract_address'] = contract_address.lower()
                        token['coin_id'] = token_id
                        tokens_with_addresses.append(token)
                
                time.sleep(0.2)  # Маленькая пауза
                
            except Exception as e:
                print(f"  ⚠️ Error for {token.get('symbol')}: {e}")
                continue
        
        print(f"✅ Found {len(tokens_with_addresses)} tokens with valid BSC addresses")
        return tokens_with_addresses
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

def save_tokens_to_db(tokens_data):
    """Сохраняет токены - КАЖДЫЙ ОТДЕЛЬНОЙ ТРАНЗАКЦИЕЙ"""
    if not db_pool or not tokens_data:
        return 0
    
    saved_count = 0
    
    for i, token in enumerate(tokens_data):
        try:
            token_address = token.get('contract_address', '')
            if not token_address or not token_address.startswith('0x'):
                continue
            
            # ОТДЕЛЬНОЕ СОЕДИНЕНИЕ ДЛЯ КАЖДОГО ТОКЕНА
            conn = db_pool.getconn()
            cursor = conn.cursor()
            
            try:
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
                    NETWORK,
                    str(token.get('name', ''))[:190],
                    str(token.get('symbol', ''))[:45],
                    float(token.get('total_volume', 0) or 0),
                    token_address.lower(),
                    float(token.get('current_price', 0) or 0),
                    float(token.get('market_cap', 0) or 0),
                    float(token.get('total_volume', 0) or 0),
                    str(token.get('coin_id', ''))[:95]
                ))
                
                conn.commit()
                saved_count += 1
                
                if saved_count % 10 == 0:
                    print(f"  ✅ Saved {saved_count} tokens")
                    
            except psycopg2.Error as e:
                print(f"  ⚠️ DB error for {token.get('symbol')}: {e}")
                conn.rollback()
            except Exception as e:
                print(f"  ⚠️ Other error for {token.get('symbol')}: {e}")
                conn.rollback()
            finally:
                cursor.close()
                db_pool.putconn(conn)
                
        except Exception as e:
            print(f"  ⚠️ General error for token: {e}")
            continue
    
    return saved_count

def main():
    print("🚀 BSC Token Collector v2")
    print("=" * 60)
    
    if not COINGECKO_API_KEY:
        print("❌ COINGECKO_API_KEY not found!")
        return
    
    if not DATABASE_URL:
        print("❌ DATABASE_URL not found!")
        return
    
    if not init_database():
        return
    
    # Собираем токены
    tokens = get_liquid_tokens_fast(limit=100)
    
    if not tokens:
        print("❌ No tokens found")
        return
    
    print(f"\n💾 Saving {len(tokens)} tokens to database...")
    saved_count = save_tokens_to_db(tokens)
    
    print(f"\n" + "=" * 60)
    print(f"✅ FINAL RESULT:")
    print(f"   • Tokens collected: {len(tokens)}")
    print(f"   • Successfully saved: {saved_count}")
    print(f"   • Failed: {len(tokens) - saved_count}")
    
    # Показываем что сохранили
    if saved_count > 0:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT symbol, name, liquidity_usd 
                FROM tokens 
                ORDER BY liquidity_usd DESC 
                LIMIT 3
            ''')
            results = cursor.fetchall()
            cursor.close()
            db_pool.putconn(conn)
            
            print("\n🏆 Top 3 tokens in database:")
            for i, (symbol, name, liquidity) in enumerate(results, 1):
                print(f"  {i}. {symbol}: ${liquidity:,.0f}")
        except:
            pass
    
    print("\n📊 Check database with:")
    print("   SELECT COUNT(*) FROM tokens;")
    print("   SELECT * FROM tokens LIMIT 5;")

# ЗАПУСК
if __name__ == "__main__":
    print("🔄 Starting token collection...")
    start_time = time.time()
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
    
    elapsed = time.time() - start_time
    print(f"\n⏱️ Execution time: {elapsed:.1f} seconds")
    print("🎯 Script completed successfully!")