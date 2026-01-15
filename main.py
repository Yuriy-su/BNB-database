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

# ===== ОСНОВНОЙ КОД ДЛЯ ТОКЕНОВ =====
def init_database():
    """ТОЛЬКО таблица для токенов"""
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, DATABASE_URL)
        print("✅ Database connection pool created")
        
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        # УДАЛИТЕ создание bsc_blocks или закомментируйте:
        # cursor.execute('CREATE TABLE IF NOT EXISTS bsc_blocks (...')
        
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

# ... (остальные функции get_liquid_tokens_from_coingecko, save_tokens_to_db и т.д.)
# ... (не изменяйте их)

def main():
    print("🚀 Starting BSC Token Collector via CoinGecko")
    print("=" * 60)
    
    if not COINGECKO_API_KEY:
        print("❌ COINGECKO_API_KEY not found in Variables!")
        return
    
    if not DATABASE_URL:
        print("❌ DATABASE_URL not found!")
        return
    
    if not init_database():
        return
    
    # Получаем токены
    tokens = get_liquid_tokens_from_coingecko(limit=1000)
    
    if not tokens:
        print("❌ No tokens received")
        return
    
    # Сохраняем
    saved = save_tokens_to_db(tokens)
    print(f"\n✅ Done! Saved {saved} tokens to database")
    
    # Показываем топ-5
    if saved > 0:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT symbol, name, liquidity_usd 
                FROM tokens 
                ORDER BY liquidity_usd DESC 
                LIMIT 5
            ''')
            top_tokens = cursor.fetchall()
            cursor.close()
            db_pool.putconn(conn)
            
            print("\n🏆 Top 5 most liquid tokens:")
            for i, (symbol, name, liquidity) in enumerate(top_tokens, 1):
                print(f"{i}. {symbol} ({name}): ${liquidity:,.0f}")
        except:
            pass

# ===== ВАЖНО: Уберите условие if __name__ == "__main__" =====
# ЗАКОММЕНТИРУЙТЕ ЭТО:
# if __name__ == "__main__":
#     main()

# И ДОБАВЬТЕ ПРЯМОЙ ВЫЗОВ:
print("🔄 Script starting...")
main()
print("✅ Script finished. Check logs for details.")
# Оставьте контейнер живым на 5 минут для проверки
time.sleep(300)