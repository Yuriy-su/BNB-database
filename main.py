import os
import time
import requests
import psycopg2
from psycopg2 import pool

# Railway автоматически загружает переменные
COINGECKO_API_KEY = os.environ.get('COINGECKO_API_KEY')
DATABASE_URL = os.environ.get('DATABASE_URL')
NETWORK = "BSC"

db_pool = None

def init_database():
    """Создаем таблицу с 6 столбцами"""
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, DATABASE_URL)
        print("✅ Database connection pool created")
        
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        # Удаляем старую таблицу и создаем новую с 6 столбцами
        cursor.execute('DROP TABLE IF EXISTS tokens;')
        
        cursor.execute('''
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
        
        conn.commit()
        cursor.close()
        db_pool.putconn(conn)
        
        print("✅ Table 'tokens' created with 6 columns")
        print("   - id (SERIAL PRIMARY KEY)")
        print("   - network (VARCHAR)")
        print("   - name (VARCHAR)")
        print("   - symbol (VARCHAR)")
        print("   - liquidity_usd (DECIMAL)")
        print("   - token_address (VARCHAR, UNIQUE)")
        print("   - created_at (TIMESTAMP, DEFAULT NOW())")
        
        return True
        
    except Exception as e:
        print(f"❌ Database initialization error: {e}")
        return False

def get_tokens_with_contract_addresses(limit=30):
    """Получаем 30 BSC токенов с адресами"""
    if not COINGECKO_API_KEY:
        print("❌ COINGECKO_API_KEY not set!")
        return []
    
    print(f"🔄 Getting top {limit} BSC tokens with contract addresses...")
    
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
    
    tokens_with_addresses = []
    
    print(f"🔍 Getting contract addresses for {len(tokens)} tokens...")
    
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
                bsc_keys = ['binance-smart-chain', 'bsc', 'binance']
                for key in bsc_keys:
                    if key in platforms and platforms[key]:
                        contract_address = platforms[key]
                        break
                
                if contract_address and isinstance(contract_address, str) and contract_address.startswith('0x'):
                    contract_address = contract_address.lower().strip()
                    
                    token['contract_address'] = contract_address
                    token['coin_id'] = token_id
                    
                    tokens_with_addresses.append(token)
                    print(f"    ✓ {symbol}: found BSC address")
                else:
                    for key, address in platforms.items():
                        if address and isinstance(address, str) and address.startswith('0x'):
                            token['contract_address'] = address.lower().strip()
                            token['coin_id'] = token_id
                            tokens_with_addresses.append(token)
                            print(f"    ⚠️ {symbol}: using {key} address")
                            break
                    else:
                        print(f"    ✗ {symbol}: no valid contract address found")
            
            time.sleep(0.5)
            
        except requests.exceptions.Timeout:
            print(f"    ⏱️ Timeout for {token.get('symbol', 'UNKNOWN')}, skipping...")
        except Exception as e:
            print(f"    ⚠️ Error processing {token.get('symbol', 'UNKNOWN')}: {e}")
    
    print(f"✅ Found {len(tokens_with_addresses)} tokens with valid contract addresses")
    return tokens_with_addresses

def save_tokens_to_database(tokens_data):
    """Сохраняем токены в 6 столбцов таблицы"""
    if not db_pool or not tokens_data:
        print("⚠️ No tokens to save or no database connection")
        return 0
    
    saved_count = 0
    error_count = 0
    
    print(f"💾 Saving {len(tokens_data)} tokens to 6-column table...")
    
    for i, token in enumerate(tokens_data):
        try:
            # Извлекаем данные для 6 столбцов
            token_address = token.get('contract_address', '').strip()
            symbol = token.get('symbol', 'UNKNOWN').upper()
            name = token.get('name', '')
            
            if not token_address or not token_address.startswith('0x'):
                error_count += 1
                continue
            
            # Вычисляем ликвидность для столбца liquidity_usd
            volume = token.get('total_volume', 0) or 0
            price = token.get('current_price', 0) or 0
            liquidity_usd = float(volume) * float(price)
            
            conn = db_pool.getconn()
            cursor = conn.cursor()
            
            try:
                # ВСТАВЛЯЕМ В 6 СТОЛБЦОВ
                cursor.execute('''
                    INSERT INTO tokens 
                    (network, name, symbol, liquidity_usd, token_address)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (token_address) DO NOTHING
                ''', (
                    NETWORK,           # network - столбец 2
                    name[:200],        # name - столбец 3
                    symbol[:50],       # symbol - столбец 4
                    liquidity_usd,     # liquidity_usd - столбец 5
                    token_address      # token_address - столбец 6
                    # created_at - столбец 7 добавится автоматически
                ))
                
                if cursor.rowcount > 0:
                    saved_count += 1
                
                conn.commit()
                
                if saved_count % 5 == 0:
                    print(f"  ✅ Saved {saved_count} tokens so far...")
                
            except Exception as e:
                conn.rollback()
                error_count += 1
            finally:
                cursor.close()
                db_pool.putconn(conn)
                
        except Exception as e:
            error_count += 1
    
    print(f"📊 Save completed: {saved_count} saved, {error_count} failed")
    return saved_count

def display_results(tokens_saved, total_tokens):
    """Показываем результаты"""
    print("\n" + "=" * 60)
    print("🎯 COLLECTION COMPLETE")
    print("=" * 60)
    
    print(f"📈 Results:")
    print(f"   • Tokens processed: {total_tokens}")
    print(f"   • Successfully saved: {tokens_saved}")
    
    if tokens_saved > 0:
        print(f"\n✅ SUCCESS! Table 'tokens' now has {tokens_saved} BSC tokens")
        
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            
            # Проверяем структуру таблицы
            cursor.execute('''
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'tokens' 
                ORDER BY ordinal_position
            ''')
            
            print("\n📊 Table structure (should have 7 columns):")
            columns = cursor.fetchall()
            for col_name, data_type in columns:
                print(f"   - {col_name} ({data_type})")
            
            # Показываем топ токенов
            cursor.execute('''
                SELECT symbol, name, liquidity_usd 
                FROM tokens 
                ORDER BY liquidity_usd DESC 
                LIMIT 5
            ''')
            
            print("\n🏆 Top 5 tokens by liquidity:")
            for i, (symbol, name, liquidity) in enumerate(cursor.fetchall(), 1):
                print(f"  {i}. {symbol:6} - {name[:20]:20} ${liquidity:12,.0f}")
            
            cursor.close()
            db_pool.putconn(conn)
            
        except Exception as e:
            print(f"  ⚠️ Could not fetch data: {e}")
        
        print("\n📋 To verify in PostgreSQL:")
        print("   SELECT COUNT(*) FROM tokens;")
        print("   SELECT * FROM tokens LIMIT 5;")
    else:
        print("\n❌ No tokens were saved")
    
    print("\n" + "=" * 60)

def main():
    """Основная функция"""
    print("=" * 60)
    print("🚀 BSC Token Collector - 30 tokens, 6 columns")
    print("=" * 60)
    
    if not COINGECKO_API_KEY:
        print("❌ ERROR: COINGECKO_API_KEY not found!")
        return
    
    if not DATABASE_URL:
        print("❌ ERROR: DATABASE_URL not found!")
        return
    
    print("\n🔧 Initializing database...")
    if not init_database():
        return
    
    print("\n🌐 Fetching 30 BSC tokens...")
    tokens = get_tokens_with_contract_addresses(limit=30)
    
    if not tokens:
        print("❌ No tokens retrieved")
        return
    
    print(f"\n💾 Saving to database...")
    saved_count = save_tokens_to_database(tokens)
    
    display_results(saved_count, len(tokens))
    
    print(f"\n⏱️ Execution time: {time.time() - start_time:.1f} seconds")

if __name__ == "__main__":
    print("🔄 Starting script...")
    start_time = time.time()
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Interrupted")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n✅ Script finished")