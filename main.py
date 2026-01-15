import os
import time
import requests
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
NETWORK = "BSC"

def setup_database():
    """Создаем таблицу для токенов"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
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
        print("✅ Created clean tokens table")
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

def get_tokens_from_pancakeswap():
    """Основная функция получения токенов"""
    print("🔄 Getting tokens from PancakeSwap API...")
    
    # Рабочий API PancakeSwap
    url = "https://api.pancakeswap.info/api/v2/tokens"
    
    try:
        response = requests.get(url, timeout=20)
        print(f"📡 API Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            if 'data' in data:
                tokens = []
                count = 0
                
                for token_address, token_data in data['data'].items():
                    if count >= 1000:  # Ограничиваем 1000 токенами
                        break
                    
                    # Получаем данные
                    name = token_data.get('name', 'Unknown Token')
                    symbol = token_data.get('symbol', 'UNKNOWN')
                    price = float(token_data.get('price', 0))
                    liquidity = float(token_data.get('liquidity', 0))
                    
                    # Расчет ликвидности в USD
                    liquidity_usd = price * liquidity if price and liquidity else 0
                    
                    # Фильтруем совсем уж мусорные токены
                    if liquidity_usd > 100:  # Хотя бы $100 ликвидности
                        tokens.append({
                            'token_address': token_address.lower(),
                            'symbol': symbol.upper()[:50],
                            'name': name[:200],
                            'liquidity_usd': liquidity_usd
                        })
                        count += 1
                
                print(f"✅ Found {len(tokens)} tokens with liquidity > $100")
                return tokens
            
            else:
                print("❌ No 'data' in response")
                print(f"Response: {data}")
                return []
        
        else:
            print(f"❌ API Error {response.status_code}: {response.text[:200]}")
            return []
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return []

def save_tokens_to_db(tokens):
    """Сохраняем токены в базу"""
    if not tokens:
        return 0
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        inserted = 0
        for token in tokens:
            try:
                cursor.execute('''
                    INSERT INTO tokens (network, name, symbol, liquidity_usd, token_address)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (token_address) DO NOTHING
                ''', (
                    NETWORK,
                    token['name'],
                    token['symbol'],
                    token['liquidity_usd'],
                    token['token_address']
                ))
                inserted += 1
            except Exception as e:
                print(f"⚠️ Failed to insert {token['symbol']}: {e}")
                continue
        
        conn.commit()
        print(f"💾 Inserted {inserted} tokens into database")
        
        # Проверяем
        cursor.execute("SELECT COUNT(*) FROM tokens")
        total = cursor.fetchone()[0]
        print(f"📊 Total tokens in DB: {total}")
        
        cursor.close()
        conn.close()
        return inserted
        
    except Exception as e:
        print(f"❌ Database save error: {e}")
        return 0

def main():
    print("=" * 60)
    print("🚀 BSC Token Collector v2.0")
    print("=" * 60)
    
    if not DATABASE_URL:
        print("❌ DATABASE_URL not found in .env")
        return
    
    # 1. Создаем таблицу
    if not setup_database():
        return
    
    # 2. Получаем токены
    start_time = time.time()
    tokens = get_tokens_from_pancakeswap()
    
    if not tokens:
        print("❌ No tokens received. Trying alternative API...")
        # Можно добавить fallback на другую API
        return
    
    # 3. Сохраняем
    saved = save_tokens_to_db(tokens)
    
    total_time = time.time() - start_time
    
    print(f"\n" + "=" * 60)
    print(f"✅ COMPLETED in {total_time:.1f} seconds")
    print(f"📈 Tokens saved: {saved}")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
    
    print("\nExiting...")