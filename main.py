import os
import time
import requests
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
NETWORK = "BSC"

db_pool = None

def setup_database():
    """УДАЛЯЕТ старую таблицу и создаёт новую только с нужными столбцами"""
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, DATABASE_URL)
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        # 1. УДАЛЯЕМ старую таблицу со всеми столбцами
        cursor.execute('DROP TABLE IF EXISTS tokens;')
        print("🗑️ Deleted old table with unnecessary columns")
        
        # 2. Создаём ЧИСТУЮ таблицу
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
        print("✅ Created clean table with columns:")
        print("   - network (сети)")
        print("   - name (название токена)")
        print("   - symbol (символ)")
        print("   - liquidity_usd (ликвидность)")
        print("   - token_address (адрес токена)")
        return True
        
    except Exception as e:
        print(f"❌ Database setup error: {e}")
        return False

def get_1000_liquid_tokens():
    """Получает 1000 самых ликвидных токенов из PancakeSwap"""
    print("🔄 Fetching 1000 most liquid BSC tokens from PancakeSwap...")
    
    url = "https://api.thegraph.com/subgraphs/name/pancakeswap/exchange-v3-bsc"
    
    query = """
    query {
      tokens(
        first: 1000
        orderBy: volumeUSD
        orderDirection: desc
        where: {volumeUSD_gt: 10000}
      ) {
        id
        symbol
        name
        volumeUSD
      }
    }
    """
    
    try:
        response = requests.post(url, json={'query': query}, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'data' in data and 'tokens' in data['data']:
                tokens = []
                for item in data['data']['tokens']:
                    tokens.append({
                        'token_address': item['id'].lower(),
                        'symbol': item['symbol'].upper(),
                        'name': item['name'],
                        'liquidity_usd': float(item['volumeUSD'])
                    })
                
                print(f"✅ Got {len(tokens)} liquid tokens")
                return tokens
    
        print("❌ Failed to get tokens from PancakeSwap")
        return []
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

def save_tokens_to_clean_table(tokens):
    """Сохраняет токены в чистую таблицу"""
    if not db_pool or not tokens:
        return 0
    
    print(f"💾 Saving {len(tokens)} tokens to clean table...")
    
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        # Вставляем данные
        for token in tokens:
            cursor.execute('''
                INSERT INTO tokens 
                (network, name, symbol, liquidity_usd, token_address)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (token_address) DO NOTHING
            ''', (
                NETWORK,
                token['name'][:200],  # Ограничиваем длину
                token['symbol'][:50],
                token['liquidity_usd'],
                token['token_address']
            ))
        
        conn.commit()
        saved = len(tokens)
        
        cursor.close()
        db_pool.putconn(conn)
        
        print(f"✅ Successfully saved {saved} tokens")
        return saved
        
    except Exception as e:
        print(f"❌ Save error: {e}")
        return 0

def main():
    print("=" * 60)
    print("🚀 CLEAN BSC TOKEN COLLECTOR")
    print("=" * 60)
    print("This will:")
    print("1. DELETE old table with unnecessary columns")
    print("2. CREATE new table with only: network, name, symbol, liquidity, address")
    print("3. FETCH 1000 liquid tokens from PancakeSwap")
    print("4. SAVE to database")
    print("=" * 60)
    
    if not DATABASE_URL:
        print("❌ DATABASE_URL not found!")
        return
    
    # 1. Создаём чистую таблицу
    if not setup_database():
        return
    
    start_time = time.time()
    
    # 2. Получаем токены
    tokens = get_1000_liquid_tokens()
    if not tokens:
        return
    
    # 3. Сохраняем
    saved = save_tokens_to_clean_table(tokens)
    
    total_time = time.time() - start_time
    
    print(f"\n" + "=" * 60)
    print(f"🎯 MISSION ACCOMPLISHED!")
    print(f"   • Time: {total_time:.1f} seconds")
    print(f"   • Tokens in database: {saved}")
    
    # Быстрая проверка
    if saved > 0:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) as count FROM tokens;")
            count = cursor.fetchone()[0]
            
            cursor.execute('''
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'tokens'
                ORDER BY ordinal_position
            ''')
            
            print(f"\n📊 TABLE STRUCTURE (only {count} columns):")
            for col_name, data_type in cursor.fetchall():
                print(f"   • {col_name} ({data_type})")
            
            cursor.close()
            db_pool.putconn(conn)
            
        except:
            pass
    
    print(f"\n✅ Database now contains {saved} BSC tokens")
    print("   Only necessary columns: network, name, symbol, liquidity_usd, token_address")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    
    print("\n⏳ Exiting in 5 seconds...")
    time.sleep(5)