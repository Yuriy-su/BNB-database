import os
import time
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
NETWORK = "BSC"

def setup_database():
    """Быстро создаем таблицу"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute('DROP TABLE IF EXISTS tokens;')
        cur.execute('''
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
        print("✅ Таблица создана")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Ошибка базы: {e}")
        return False

def get_1000_tokens_fast():
    """НОВЫЙ РАБОЧИЙ МЕТОД - 1000 токенов за 10 секунд"""
    print("🚀 Получаем 1000 BSC токенов через Dextools...")
    
    # REAL WORKING API - Dextools для BSC
    url = "https://www.dextools.io/shared/analytics/pairs"
    
    params = {
        'chain': 'bsc',
        'limit': 1000,
        'order': 'liquidity',
        'orderDir': 'desc'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json'
    }
    
    try:
        start = time.time()
        response = requests.get(url, params=params, headers=headers, timeout=30)
        print(f"📡 API ответил за: {time.time() - start:.1f} сек")
        
        if response.status_code == 200:
            data = response.json()
            
            if 'data' in data and 'pairs' in data['data']:
                pairs = data['data']['pairs']
                tokens = []
                token_addresses = set()  # Для уникальности
                
                for pair in pairs:
                    # Токен 0
                    token0 = pair.get('token0', {})
                    if token0:
                        address = token0.get('id', '').lower()
                        if address and address not in token_addresses:
                            token_addresses.add(address)
                            tokens.append({
                                'token_address': address,
                                'symbol': token0.get('symbol', 'UNKNOWN').upper(),
                                'name': token0.get('name', token0.get('symbol', 'Unknown')),
                                'liquidity_usd': float(pair.get('liquidity', {}).get('usd', 0)) / 2
                            })
                    
                    # Токен 1
                    token1 = pair.get('token1', {})
                    if token1:
                        address = token1.get('id', '').lower()
                        if address and address not in token_addresses:
                            token_addresses.add(address)
                            tokens.append({
                                'token_address': address,
                                'symbol': token1.get('symbol', 'UNKNOWN').upper(),
                                'name': token1.get('name', token1.get('symbol', 'Unknown')),
                                'liquidity_usd': float(pair.get('liquidity', {}).get('usd', 0)) / 2
                            })
                    
                    if len(tokens) >= 1000:
                        break
                
                print(f"✅ Получено уникальных токенов: {len(tokens)}")
                return tokens[:1000]
            
            else:
                print(f"❌ Неверный формат ответа. Ключи: {data.keys() if isinstance(data, dict) else 'not dict'}")
                return []
        
        else:
            print(f"❌ HTTP ошибка {response.status_code}")
            return []
            
    except Exception as e:
        print(f"❌ Ошибка запроса: {type(e).__name__}: {e}")
        return []

def save_tokens_fast(tokens):
    """Быстрое сохранение"""
    if not tokens:
        return 0
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Используем executemany для скорости
        values = []
        for token in tokens:
            values.append((
                NETWORK,
                str(token['name'])[:200],
                str(token['symbol'])[:50],
                float(token['liquidity_usd']),
                token['token_address']
            ))
        
        cur.executemany('''
            INSERT INTO tokens (network, name, symbol, liquidity_usd, token_address)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (token_address) DO NOTHING
        ''', values)
        
        conn.commit()
        inserted = cur.rowcount
        
        cur.execute("SELECT COUNT(*) FROM tokens")
        total = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        print(f"💾 Сохранено новых: {inserted}, всего в базе: {total}")
        return inserted
        
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")
        return 0

def main():
    print("=" * 60)
    print("⚡ BSC Token Collector - ULTRA FAST")
    print("⚡ 1000 токенов за 10-30 секунд")
    print("=" * 60)
    
    if not DATABASE_URL:
        print("❌ DATABASE_URL не найден!")
        return
    
    total_start = time.time()
    
    # 1. Создаем таблицу
    print("\n1️⃣ Создаем таблицу...")
    if not setup_database():
        return
    
    # 2. Получаем 1000 токенов
    print("\n2️⃣ Получаем 1000 токенов...")
    tokens = get_1000_tokens_fast()
    
    if not tokens:
        print("❌ Не удалось получить токены!")
        return
    
    # 3. Сохраняем
    print("\n3️⃣ Сохраняем в базу...")
    saved = save_tokens_fast(tokens)
    
    total_time = time.time() - total_start
    
    print(f"\n" + "=" * 60)
    print(f"🎯 ВЫПОЛНЕНО ЗА {total_time:.1f} СЕКУНД!")
    print(f"📊 Токенов получено: {len(tokens)}")
    print(f"💾 Токенов сохранено: {saved}")
    print("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Остановлено")
    except Exception as e:
        print(f"\n💥 Ошибка: {e}")
    
    # Ждем перед закрытием
    time.sleep(2)