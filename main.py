import os
import time
import requests
from datetime import datetime
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Конфигурация
BSC_RPC_URL = os.getenv('BSC_RPC_URL', 'https://bsc-dataseed.binance.org/')
BSCSCAN_API_KEY = os.getenv('BSCSCAN_API_KEY', '')
DATABASE_URL = os.getenv('DATABASE_URL')

# Инициализация пула соединений БД
db_pool = None

def init_database():
    """Инициализация соединения с БД"""
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20, DATABASE_URL
        )
        print("✅ Database connection pool created")
        
        # Создаем таблицу если не существует
        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bsc_blocks (
                id SERIAL PRIMARY KEY,
                block_number BIGINT UNIQUE,
                timestamp TIMESTAMP,
                transaction_count INT,
                gas_used DECIMAL,
                gas_limit DECIMAL,
                miner VARCHAR(42),
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        conn.commit()
        cursor.close()
        db_pool.putconn(conn)
        print("✅ Database table ready")
        
    except Exception as e:
        print(f"❌ Database initialization error: {e}")

def get_bsc_block(block_number='latest'):
    """Получение данных блока из BSC"""
    try:
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [block_number, True],
            "id": 1
        }
        response = requests.post(BSC_RPC_URL, json=payload, timeout=10)
        data = response.json()
        
        if 'result' in data:
            return data['result']
        else:
            print(f"⚠️ Error getting block: {data}")
            return None
    except Exception as e:
        print(f"❌ BSC RPC error: {e}")
        return None

def save_block_to_db(block_data):
    """Сохранение данных блока в БД"""
    if not block_data:
        return False
    
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        # Конвертируем hex в числа
        block_number = int(block_data['number'], 16)
        timestamp = int(block_data['timestamp'], 16)
        tx_count = len(block_data['transactions'])
        gas_used = int(block_data['gasUsed'], 16)
        gas_limit = int(block_data['gasLimit'], 16)
        miner = block_data['miner']
        
        cursor.execute('''
            INSERT INTO bsc_blocks 
            (block_number, timestamp, transaction_count, gas_used, gas_limit, miner)
            VALUES (%s, to_timestamp(%s), %s, %s, %s, %s)
            ON CONFLICT (block_number) DO NOTHING
        ''', (block_number, timestamp, tx_count, gas_used, gas_limit, miner))
        
        conn.commit()
        cursor.close()
        db_pool.putconn(conn)
        
        print(f"✅ Block #{block_number} saved to DB")
        return True
        
    except Exception as e:
        print(f"❌ Error saving block to DB: {e}")
        return False

def main():
    """Основной цикл работы"""
    print("🚀 Starting BSC Database Service...")
    
    # Проверяем обязательные переменные
    if not DATABASE_URL:
        print("❌ DATABASE_URL not set!")
        return
    
    # Инициализируем БД
    init_database()
    
    if not db_pool:
        print("❌ Cannot start without database connection")
        return
    
    print("🔄 Starting block monitoring...")
    last_processed_block = None
    
    while True:
        try:
            # Получаем последний блок
            block = get_bsc_block('latest')
            
            if block:
                block_number = int(block['number'], 16)
                
                # Обрабатываем только новые блоки
                if last_processed_block != block_number:
                    print(f"📦 New block #{block_number} with {len(block['transactions'])} tx")
                    
                    # Сохраняем в БД
                    save_block_to_db(block)
                    
                    last_processed_block = block_number
                else:
                    print(f"⏳ Waiting for new block... (current: #{block_number})")
            else:
                print("⚠️ Failed to get block data")
            
            # Пауза 15 секунд между проверками (BSC ~3 сек/блок)
            time.sleep(60)  # Update
            
        except KeyboardInterrupt:
            print("\n🛑 Service stopped by user")
            break
        except Exception as e:
            print(f"❌ Main loop error: {e}")
            time.sleep(30)  # Пауза при ошибке

if __name__ == "__main__":
    main()