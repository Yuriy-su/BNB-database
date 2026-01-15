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
NETWORK = "BSC"  # Сеть Binance Smart Chain

# Инициализация пула соединений БД
db_pool = None

# ========== ОСНОВНЫЕ ФУНКЦИИ ==========

def init_database():
    """Инициализация соединения с БД и проверка структуры таблицы"""
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, DATABASE_URL)
        print("✅ Database connection pool created")
        
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        # Проверяем наличие всех необходимых столбцов
        cursor.execute('''
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'tokens' 
            ORDER BY ordinal_position
        ''')
        
        columns = {row[0]: row[1] for row in cursor.fetchall()}
        print(f"📊 Table 'tokens' has {len(columns)} columns")
        
        # Проверяем обязательные столбцы
        required_columns = ['network', 'name', 'symbol', 'liquidity_usd', 
                          'token_address', 'current_price', 'market_cap', 'total_volume']
        
        missing_columns = [col for col in required_columns if col not in columns]
        
        if missing_columns:
            print(f"❌ Missing columns: {missing_columns}")
            return False
        
        # Проверяем наличие coin_id, добавляем если нет
        if 'coin_id' not in columns:
            print("⚠️ Column 'coin_id' not found, adding it...")
            try:
                cursor.execute('ALTER TABLE tokens ADD COLUMN coin_id VARCHAR(100);')
                conn.commit()
                print("✅ Column 'coin_id' added")
            except Exception as e:
                print(f"⚠️ Could not add coin_id: {e}")
        
        cursor.close()
        db_pool.putconn(conn)
        print("✅ Database is ready")
        return True
        
    except Exception as e:
        print(f"❌ Database initialization error: {e}")
        return False

def get_tokens_with_contract_addresses(limit=30):
    """
    Получает список токенов BSC с реальными адресами контрактов через CoinGecko API.
    Возвращает только токены с валидными BSC-адресами.
    """
    if not COINGECKO_API_KEY:
        print("❌ COINGECKO_API_KEY not set!")
        return []
    
    print(f"🔄 Getting top {limit} BSC tokens with contract addresses...")
    
    # Шаг 1: Получаем список токенов
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            'vs_currency': 'usd',
            'category': 'binance-smart-chain',
            'order': 'volume_desc',  # Сортировка по объёму (ликвидности)
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
    
    # Шаг 2: Для каждого токена получаем адрес контракта
    tokens_with_addresses = []
    
    print(f"🔍 Getting contract addresses for {len(tokens)} tokens...")
    
    for i, token in enumerate(tokens):
        try:
            token_id = token.get('id')
            symbol = token.get('symbol', 'UNKNOWN').upper()
            
            if not token_id:
                continue
            
            # Выводим прогресс каждые 5 токенов
            if i % 5 == 0:
                print(f"  Processing {i+1}/{len(tokens)}...")
            
            # Получаем детали токена для адреса контракта
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
                
                # Ищем BSC адрес контракта
                contract_address = None
                
                # Проверяем возможные ключи для BSC
                bsc_keys = ['binance-smart-chain', 'bsc', 'binance']
                for key in bsc_keys:
                    if key in platforms and platforms[key]:
                        contract_address = platforms[key]
                        break
                
                # Если нашли валидный адрес
                if contract_address and isinstance(contract_address, str) and contract_address.startswith('0x'):
                    # Очищаем адрес
                    contract_address = contract_address.lower().strip()
                    
                    # Добавляем адрес и coin_id в данные токена
                    token['contract_address'] = contract_address
                    token['coin_id'] = token_id
                    
                    tokens_with_addresses.append(token)
                    print(f"    ✓ {symbol}: found BSC address")
                else:
                    # Проверяем другие возможные адреса
                    for key, address in platforms.items():
                        if address and isinstance(address, str) and address.startswith('0x'):
                            token['contract_address'] = address.lower().strip()
                            token['coin_id'] = token_id
                            tokens_with_addresses.append(token)
                            print(f"    ⚠️ {symbol}: using {key} address")
                            break
                    else:
                        print(f"    ✗ {symbol}: no valid contract address found")
            
            # Пауза между запросами чтобы не превысить лимиты API
            time.sleep(0.5)
            
        except requests.exceptions.Timeout:
            print(f"    ⏱️ Timeout for {token.get('symbol', 'UNKNOWN')}, skipping...")
        except Exception as e:
            print(f"    ⚠️ Error processing {token.get('symbol', 'UNKNOWN')}: {e}")
    
    print(f"✅ Found {len(tokens_with_addresses)} tokens with valid contract addresses")
    return tokens_with_addresses

def save_tokens_to_database(tokens_data):
    """
    Сохраняет токены в базу данных.
    Каждый токен сохраняется отдельной транзакцией для надёжности.
    """
    if not db_pool or not tokens_data:
        print("⚠️ No tokens to save or no database connection")
        return 0
    
    saved_count = 0
    error_count = 0
    
    print(f"💾 Attempting to save {len(tokens_data)} tokens to database...")
    
    for i, token in enumerate(tokens_data):
        try:
            # Извлекаем данные
            token_address = token.get('contract_address', '').strip()
            coin_id = token.get('coin_id', '')
            symbol = token.get('symbol', 'UNKNOWN').upper()
            name = token.get('name', '')
            
            # Проверяем обязательные поля
            if not token_address or not token_address.startswith('0x'):
                print(f"  ⚠️ [{i+1}] {symbol}: Invalid contract address '{token_address}'")
                error_count += 1
                continue
            
            # Подготавливаем значения для базы данных
            liquidity_usd = float(token.get('total_volume', 0) or 0)
            current_price = float(token.get('current_price', 0) or 0)
            market_cap = float(token.get('market_cap', 0) or 0)
            total_volume = liquidity_usd  # Используем тот же показатель
            
            # Получаем соединение с БД
            conn = db_pool.getconn()
            cursor = conn.cursor()
            
            try:
                # SQL запрос для вставки или обновления
                # Используем все столбцы, которые есть в вашей таблице
                cursor.execute('''
                    INSERT INTO tokens 
                    (network, name, symbol, liquidity_usd, token_address, 
                     current_price, market_cap, total_volume, coin_id, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
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
                    name[:200],  # Ограничиваем длину
                    symbol[:50],
                    liquidity_usd,
                    token_address,
                    current_price,
                    market_cap,
                    total_volume,
                    coin_id[:100]
                ))
                
                conn.commit()
                saved_count += 1
                
                # Выводим прогресс
                if saved_count % 5 == 0:
                    print(f"  ✅ Saved {saved_count} tokens so far...")
                
            except psycopg2.Error as db_error:
                print(f"  ❌ [{i+1}] {symbol}: Database error - {db_error}")
                conn.rollback()
                error_count += 1
            except Exception as e:
                print(f"  ⚠️ [{i+1}] {symbol}: Unexpected error - {e}")
                conn.rollback()
                error_count += 1
            finally:
                cursor.close()
                db_pool.putconn(conn)
                
        except Exception as e:
            print(f"  ⚠️ [{i+1}] Error processing token: {e}")
            error_count += 1
    
    print(f"📊 Save operation completed: {saved_count} saved, {error_count} failed")
    return saved_count

def display_results(tokens_saved, total_tokens):
    """Выводит итоговые результаты и инструкции для проверки"""
    print("\n" + "=" * 60)
    print("🎯 COLLECTION COMPLETE")
    print("=" * 60)
    
    print(f"📈 Results:")
    print(f"   • Tokens processed: {total_tokens}")
    print(f"   • Successfully saved: {tokens_saved}")
    print(f"   • Success rate: {(tokens_saved/total_tokens*100 if total_tokens > 0 else 0):.1f}%")
    
    if tokens_saved > 0:
        print(f"\n✅ SUCCESS! Database now contains {tokens_saved} BSC tokens")
        
        # Показываем примеры сохранённых токенов
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT symbol, name, liquidity_usd, 
                       LEFT(token_address, 20) || '...' as short_address
                FROM tokens 
                ORDER BY liquidity_usd DESC 
                LIMIT 5
            ''')
            top_tokens = cursor.fetchall()
            cursor.close()
            db_pool.putconn(conn)
            
            print("\n🏆 Top 5 most liquid tokens in database:")
            for i, (symbol, name, liquidity, address) in enumerate(top_tokens, 1):
                print(f"  {i}. {symbol:6} - {name[:20]:20} ${liquidity:12,.0f}")
                print(f"     Address: {address}")
        except Exception as e:
            print(f"  ⚠️ Could not fetch top tokens: {e}")
        
        print("\n📊 To verify in PostgreSQL, run these queries:")
        print("   SELECT COUNT(*) FROM tokens;")
        print("   SELECT symbol, name, liquidity_usd FROM tokens ORDER BY liquidity_usd DESC LIMIT 10;")
        print("   SELECT symbol, token_address FROM tokens WHERE token_address LIKE '0x%' LIMIT 5;")
    else:
        print("\n❌ No tokens were saved to the database")
        print("   Check the logs above for errors")
    
    print("\n" + "=" * 60)

# ========== НОВАЯ ФУНКЦИЯ: ЭКСПОРТ В CSV ==========

def export_tokens_to_csv():
    """Экспортирует все токены в CSV формат (выводит в консоль)"""
    print("\n" + "=" * 80)
    print("📤 CSV ЭКСПОРТ ДАННЫХ ИЗ БАЗЫ")
    print("=" * 80)
    
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        # Получаем ВСЕ токены отсортированные по ликвидности
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
        print("А ЗАТЕМ ОТКРОЙТЕ В EXCEL ИЛИ GOOGLE SHEETS")
        print("=" * 80)
        
        # Заголовки CSV
        print("id,network,name,symbol,liquidity_usd,token_address")
        
        # Данные в CSV формате
        for token in tokens:
            id_val, network, name, symbol, liquidity, address = token
            
            # Экранируем запятые в названиях
            if ',' in str(name):
                name = f'"{name}"'
            if ',' in str(symbol):
                symbol = f'"{symbol}"'
            if ',' in str(address):
                address = f'"{address}"'
            
            print(f"{id_val},{network},{name},{symbol},{liquidity},{address}")
        
        print("=" * 80)
        print("✅ ЭКСПОРТ ЗАВЕРШЁН")
        print(f"📁 Экспортировано {len(tokens)} токенов")
        print("\n📋 ИНСТРУКЦИЯ:")
        print("1. Скопируйте ВЕСЬ текст выше (от = до =)")
        print("2. Вставьте в БЛОКНОТ (Notepad)")
        print("3. Сохраните как tokens.csv")
        print("4. Откройте в Microsoft Excel или Google Sheets")
        print("=" * 80)
        
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
    print("🚀 BSC Token Collector - CoinGecko + PostgreSQL")
    print("=" * 60)
    
    # Проверка обязательных переменных
    if not COINGECKO_API_KEY:
        print("❌ ERROR: COINGECKO_API_KEY not found!")
        print("   Add it to Railway Variables:")
        print("   Name: COINGECKO_API_KEY")
        print("   Value: Your_CoinGecko_API_Key_Here")
        return
    
    if not DATABASE_URL:
        print("❌ ERROR: DATABASE_URL not found!")
        return
    
    # Инициализация базы данных
    print("\n🔧 Initializing database connection...")
    if not init_database():
        print("❌ Failed to initialize database")
        return
    
    # Получение токенов с адресами контрактов
    print("\n🌐 Fetching BSC tokens from CoinGecko...")
    tokens = get_tokens_with_contract_addresses(limit=30)
    
    if not tokens:
        print("❌ No tokens retrieved from CoinGecko")
        return
    
    # Сохранение в базу данных
    print(f"\n💾 Saving {len(tokens)} tokens to PostgreSQL...")
    saved_count = save_tokens_to_database(tokens)
    
    # Вывод результатов
    display_results(saved_count, len(tokens))
    
    # ===== НОВОЕ: ЭКСПОРТ ДАННЫХ =====
    print("\n" + "=" * 60)
    print("📤 ЗАПУСК ЭКСПОРТА ДАННЫХ")
    print("=" * 60)
    
    export_count = export_tokens_to_csv()
    
    if export_count > 0:
        print(f"\n🎉 ВСЁ ГОТОВО! База данных обновлена и экспортирована.")
        print(f"   • Собрано токенов: {saved_count}")
        print(f"   • Экспортировано: {export_count}")
    else:
        print("\n⚠️ Экспорт не удался, но данные собраны успешно")
    
    print(f"\n⏱️ Total execution time: {time.strftime('%M:%S')}")

# ========== ЗАПУСК ПРОГРАММЫ ==========

if __name__ == "__main__":
    print("🔄 Starting BSC Token Collection Script...")
    start_time = time.time()
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Script interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    
    elapsed_time = time.time() - start_time
    print(f"\n⏱️ Script finished in {elapsed_time:.1f} seconds")
    print("📝 Check Railway logs for details")
    print("=" * 60)
    
    # Держим контейнер активным для проверки логов и копирования
    print("\n⏳ Container will stay alive for 10 minutes to copy data...")
    for i in range(600):
        if i % 60 == 0:
            print(f"[{i//60} min] Container is still running - copy CSV data from above")
        time.sleep(1)