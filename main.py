import os
import time
import requests
import psycopg2
from datetime import datetime

def get_bsc_data(api_key):
    """Получаем данные с BSCScan API"""
    if not api_key:
        print("⚠️ BSCSCAN_API_KEY not set in environment variables")
        return None
    
    try:
        # Пример 1: Получить цену BNB
        params = {
            'module': 'stats',
            'action': 'bscprice',
            'apikey': api_key
        }
        
        response = requests.get('https://api.bscscan.com/api', params=params, timeout=10)
        response.raise_for_status()  # Проверка HTTP ошибок
        
        data = response.json()
        
        if data['status'] == '1':
            bnb_price = data['result']['ethusd']
            print(f"✅ BNB Price: ${bnb_price}")
            return {"bnb_price": bnb_price}
        else:
            print(f"❌ BSCScan API error: {data.get('message')}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

def save_to_database(db_url, data):
    """Сохраняем данные в PostgreSQL"""
    if not db_url:
        print("⚠️ DATABASE_URL not set")
        return False
    
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # Создаём таблицу если не существует
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bsc_prices (
                id SERIAL PRIMARY KEY,
                bnb_price_usd DECIMAL(10, 4),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Добавляем данные
        if data and 'bnb_price' in data:
            cur.execute("""
                INSERT INTO bsc_prices (bnb_price_usd)
                VALUES (%s)
            """, (float(data['bnb_price']),))
        
        conn.commit()
        
        # Сколько записей в таблице
        cur.execute("SELECT COUNT(*) FROM bsc_prices;")
        count = cur.fetchone()[0]
        print(f"🗄️ Total price records: {count}")
        
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

def main():
    print("=" * 50)
    print("🚀 BSC Data Collector Service")
    print("=" * 50)
    
    # Получаем ключи ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ
    api_key = os.getenv('BSCSCAN_API_KEY')
    db_url = os.getenv('DATABASE_URL')
    
    print(f"🔑 API Key loaded: {'✅' if api_key else '❌ Not set'}")
    print(f"🗄️ DB URL loaded: {'✅' if db_url else '❌ Not set'}")
    
    if not api_key:
        print("\n⚠️ Please set BSCSCAN_API_KEY in Railway Variables!")
        print("1. Go to Railway → Variables")
        print("2. Add: BSCSCAN_API_KEY = your_key_here")
        print("3. Redeploy service\n")
    
    cycle_count = 0
    
    while True:
        cycle_count += 1
        print(f"\n📊 Cycle #{cycle_count} at {datetime.now()}")
        
        # 1. Получаем данные с BSCScan
        data = get_bsc_data(api_key)
        
        # 2. Сохраняем в базу
        if data:
            save_to_database(db_url, data)
        
        # 3. Ждём 5 минут до следующего цикла
        print(f"⏳ Next update in 300 seconds (5 minutes)...")
        time.sleep(300)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Service stopped by user")
    except Exception as e:
        print(f"💥 Critical error: {e}")
        print("Restarting in 60 seconds...")
        time.sleep(60)"# Update" 
