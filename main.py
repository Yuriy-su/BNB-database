# main.py - РАБОЧИЙ КОД ДЛЯ 100 BSC ТОКЕНОВ
import os
import time
import requests
import psycopg2
from datetime import datetime

# Railway автоматически загружает переменные
COINGECKO_API_KEY = os.environ.get('COINGECKO_API_KEY')
DATABASE_URL = os.environ.get('DATABASE_URL')
NETWORK = "BSC"

def log(message):
    """Логирование с временем"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")

def setup_database():
    """Создаем таблицу с ТОЛЬКО нужными столбцами"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Удаляем старую таблицу если существует
        cur.execute('DROP TABLE IF EXISTS tokens;')
        
        # Создаем ТОЛЬКО с нужными столбцами
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
        log("✅ Таблица 'tokens' создана с нужными столбцами")
        return True
        
    except Exception as e:
        log(f"❌ Ошибка создания таблицы: {e}")
        return False

def get_bsc_address_from_token(token_id):
    """Получаем BSC адрес для токена"""
    try:
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
        
        response = requests.get(details_url, params=details_params, timeout=15)
        
        if response.status_code == 200:
            details = response.json()
            platforms = details.get('platforms', {})
            
            # Ищем BSC адрес в возможных ключах
            bsc_keys = ['binance-smart-chain', 'binancecoin', 'bsc', 'binance']
            for key in bsc_keys:
                if key in platforms and platforms[key]:
                    address = platforms[key].strip().lower()
                    if address.startswith('0x') and len(address) == 42:
                        return address
            
            # Если не нашли BSC, ищем любой адрес (но помечаем)
            for key, addr in platforms.items():
                if addr and isinstance(addr, str) and addr.startswith('0x'):
                    return addr.strip().lower()
                    
    except Exception as e:
        log(f"    ⚠️ Ошибка получения адреса: {e}")
    
    return None

def get_100_bsc_tokens():
    """Получаем 100 токенов С ГАРАНТИРОВАННЫМИ BSC АДРЕСАМИ"""
    if not COINGECKO_API_KEY:
        log("❌ COINGECKO_API_KEY не найден!")
        return []
    
    log("🔄 Получаем 100 BSC токенов с адресами...")
    
    # Шаг 1: Получаем список токенов (больше чем нужно, т.к. не у всех будут BSC адреса)
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            'vs_currency': 'usd',
            'category': 'binance-smart-chain',
            'order': 'volume_desc',  # Сортировка по объему = ликвидности
            'per_page': 150,  # Берем больше, т.к. не у всех будут BSC адреса
            'page': 1,
            'sparkline': 'false',
            'x_cg_demo_api_key': COINGECKO_API_KEY
        }
        
        log("📥 Запрос списка токенов...")
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            log(f"❌ Ошибка CoinGecko API: {response.status_code}")
            log(f"   Ответ: {response.text[:200]}")
            return []
        
        tokens = response.json()
        log(f"✅ Получено {len(tokens)} токенов")
        
    except Exception as e:
        log(f"❌ Ошибка получения списка: {e}")
        return []
    
    # Шаг 2: Фильтруем токены с BSC адресами
    bsc_tokens = []
    total_processed = 0
    
    log("🔍 Фильтруем токены с BSC адресами...")
    
    for token in tokens:
        try:
            total_processed += 1
            token_id = token.get('id')
            symbol = token.get('symbol', 'UNKNOWN').upper()
            name = token.get('name', '')
            
            if not token_id:
                continue
            
            # Показываем прогресс
            if total_processed % 20 == 0:
                log(f"   Обработано: {total_processed}, найдено BSC: {len(bsc_tokens)}")
            
            # Получаем BSC адрес
            bsc_address = get_bsc_address_from_token(token_id)
            
            if not bsc_address:
                continue  # Пропускаем токены без BSC адреса
            
            # Вычисляем ликвидность (volume * price)
            volume = token.get('total_volume', 0) or 0
            price = token.get('current_price', 0) or 0
            liquidity = float(volume) * float(price)
            
            # Добавляем токен с BSC адресом
            bsc_tokens.append({
                'token_address': bsc_address,
                'symbol': symbol,
                'name': name,
                'liquidity_usd': liquidity,
                'token_id': token_id
            })
            
            # Останавливаемся когда нашли 100 токенов
            if len(bsc_tokens) >= 100:
                break
            
            # Пауза между запросами
            time.sleep(0.3)
            
        except Exception as e:
            log(f"    ⚠️ Ошибка обработки токена: {e}")
            continue
    
    log(f"📊 Итог: обработано {total_processed}, найдено с BSC: {len(bsc_tokens)}")
    return bsc_tokens

def save_tokens_to_db(tokens):
    """Сохраняем токены в базу"""
    if not tokens:
        log("❌ Нет токенов для сохранения")
        return 0
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        saved_count = 0
        
        log(f"💾 Сохраняем {len(tokens)} токенов...")
        
        for i, token in enumerate(tokens, 1):
            try:
                cur.execute('''
                    INSERT INTO tokens (network, name, symbol, liquidity_usd, token_address)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (token_address) DO NOTHING
                ''', (
                    NETWORK,
                    str(token['name'])[:200],  # Ограничиваем длину
                    str(token['symbol'])[:50],
                    float(token['liquidity_usd']),
                    token['token_address']
                ))
                
                if cur.rowcount > 0:
                    saved_count += 1
                
                # Показываем прогресс
                if i % 20 == 0:
                    log(f"   Сохранено: {i}/{len(tokens)}")
                    
            except Exception as e:
                log(f"   ⚠️ Ошибка токена {token['symbol']}: {e}")
                continue
        
        conn.commit()
        
        # Проверяем итог
        cur.execute("SELECT COUNT(*) FROM tokens")
        total_in_db = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        log(f"✅ Сохранено новых токенов: {saved_count}")
        log(f"📊 Всего в базе: {total_in_db}")
        
        return saved_count
        
    except Exception as e:
        log(f"❌ Ошибка сохранения: {e}")
        return 0

def show_results():
    """Показываем результаты"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Считаем записи
        cur.execute("SELECT COUNT(*) FROM tokens")
        total = cur.fetchone()[0]
        
        if total == 0:
            log("❌ Таблица пустая!")
            return False
        
        log(f"\n📈 ИТОГОВАЯ СТАТИСТИКА:")
        log(f"   Всего токенов в таблице: {total}")
        
        # Показываем топ-5 по ликвидности
        log("\n🏆 ТОП-5 по ликвидности:")
        cur.execute("""
            SELECT symbol, name, liquidity_usd, token_address 
            FROM tokens 
            ORDER BY liquidity_usd DESC 
            LIMIT 5
        """)
        
        for i, row in enumerate(cur.fetchall(), 1):
            log(f"   {i}. {row[0]} ({row[1][:20]})")
            log(f"      Ликвидность: ${row[2]:,.0f}")
            log(f"      Адрес: {row[3][:20]}...")
        
        # Показываем структуру таблицы
        log("\n📋 СТРУКТУРА ТАБЛИЦЫ:")
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'tokens' 
            ORDER BY ordinal_position
        """)
        
        for col_name, data_type in cur.fetchall():
            log(f"   - {col_name} ({data_type})")
        
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        log(f"❌ Ошибка при выводе результатов: {e}")
        return False

def main():
    """Основная функция"""
    print("\n" + "=" * 70)
    log("🚀 COINGECKO BSC TOKEN COLLECTOR - 100 ТОКЕНОВ")
    print("=" * 70)
    
    # Проверяем наличие необходимых переменных
    if not DATABASE_URL:
        log("❌ DATABASE_URL не найден!")
        return
    
    if not COINGECKO_API_KEY:
        log("❌ COINGECKO_API_KEY не найден!")
        log("   Добавьте в Railway Variables:")
        log("   COINGECKO_API_KEY=ваш_ключ_от_coingecko")
        return
    
    log(f"🔑 Используется CoinGecko API Key: {COINGECKO_API_KEY[:8]}...")
    
    start_time = time.time()
    
    try:
        # 1. Создаем таблицу
        log("\n1️⃣ ПОДГОТОВКА БАЗЫ ДАННЫХ")
        if not setup_database():
            return
        
        # 2. Получаем токены с BSC адресами
        log("\n2️⃣ ПОЛУЧЕНИЕ BSC ТОКЕНОВ")
        tokens = get_100_bsc_tokens()
        
        if not tokens:
            log("❌ Не удалось получить токены с BSC адресами")
            log("   Возможные причины:")
            log("   1. CoinGecko API не отвечает")
            log("   2. У токенов нет BSC адресов")
            log("   3. Проблемы с сетью")
            return
        
        # 3. Сохраняем токены
        log("\n3️⃣ СОХРАНЕНИЕ В БАЗУ")
        saved_count = save_tokens_to_db(tokens)
        
        # 4. Показываем результаты
        log("\n4️⃣ РЕЗУЛЬТАТЫ")
        if saved_count > 0:
            show_results()
        
        total_time = time.time() - start_time
        
        print("\n" + "=" * 70)
        if saved_count > 0:
            log(f"✅ УСПЕХ! За {total_time:.1f} секунд сохранено {saved_count} BSC токенов")
        else:
            log(f"⚠️  Токены не были сохранены (возможно уже есть в базе)")
        print("=" * 70)
        
    except Exception as e:
        log(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()