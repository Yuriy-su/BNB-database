import os
import sys
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

def init_database():
    """Инициализация соединения с БД и создание таблицы для токенов"""
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, DATABASE_URL)
        print("✅ Database connection pool created")
        
        # Создаем таблицу для токенов
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
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        conn.commit()
        cursor.close()
        db_pool.putconn(conn)
        print("✅ Database table 'tokens' is ready")
        
    except Exception as e:
        print(f"❌ Database initialization error: {e}")
        return False
    return True

def get_liquid_tokens_from_coingecko(limit=1000):
    """
    Получает список ликвидных токенов BSC через CoinGecko API.
    Сортирует по объёму торгов (total_volume) - это показатель ликвидности.
    """
    if not COINGECKO_API_KEY:
        print("❌ COINGECKO_API_KEY not set!")
        return []
    
    all_tokens = []
    page = 1
    per_page = 250  # Максимум 250 токенов за запрос
    
    print(f"🔄 Начинаем сбор данных с CoinGecko...")
    
    while len(all_tokens) < limit:
        try:
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                'vs_currency': 'usd',
                'category': 'binance-smart-chain',
                'order': 'volume_desc',  # Сортировка по объёму (ликвидности)
                'per_page': per_page,
                'page': page,
                'sparkline': 'false',
                'x_cg_demo_api_key': COINGECKO_API_KEY
            }
            
            print(f"📥 Запрос страницы {page}...")
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 429:
                print("⚠️ Превышен лимит запросов. Ждём 60 секунд...")
                time.sleep(60)
                continue
                
            if response.status_code != 200:
                print(f"❌ Ошибка CoinGecko API: {response.status_code}")
                break
                
            tokens = response.json()
            
            if not tokens:
                print("ℹ️ Больше токенов нет")
                break
            
            # Фильтруем только токены с объёмом торгов > 1000 USD
            liquid_tokens = [
                token for token in tokens 
                if token.get('total_volume', 0) > 1000
            ]
            
            all_tokens.extend(liquid_tokens)
            print(f"✅ Получено {len(liquid_tokens)} ликвидных токенов (страница {page})")
            
            # CoinGecko API имеет лимиты, поэтому добавляем паузу
            time.sleep(7)  # Бесплатный тариф: 30 запросов/мин ≈ 1 запрос/2 сек
            page += 1
            
            if len(all_tokens) >= limit:
                all_tokens = all_tokens[:limit]
                break
                
        except requests.exceptions.Timeout:
            print("⏱️ Таймаут запроса. Пробуем снова через 10 секунд...")
            time.sleep(10)
        except Exception as e:
            print(f"❌ Ошибка при запросе к CoinGecko: {e}")
            break
    
    print(f"🎯 Итого собрано {len(all_tokens)} ликвидных токенов BSC")
    return all_tokens

def save_tokens_to_db(tokens_data):
    """
    Сохраняет токены в базу данных.
    Возвращает количество успешно сохранённых токенов.
    """
    if not db_pool or not tokens_data:
        return 0
    
    saved_count = 0
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        for token in tokens_data:
            try:
                # Извлекаем данные из ответа CoinGecko
                token_address = token.get('contract_address', '')
                
                # Если адреса нет, пропускаем (не контрактный токен BSC)
                if not token_address:
                    continue
                
                name = token.get('name', '')
                symbol = token.get('symbol', '')
                
                # Ликвидность (объём торгов за 24ч)
                liquidity_usd = token.get('total_volume', 0)
                
                # Дополнительные метрики
                current_price = token.get('current_price', 0)
                market_cap = token.get('market_cap', 0)
                
                # Вставка или обновление записи
                cursor.execute('''
                    INSERT INTO tokens 
                    (network, name, symbol, liquidity_usd, token_address, 
                     current_price, market_cap, total_volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (token_address) 
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        symbol = EXCLUDED.symbol,
                        liquidity_usd = EXCLUDED.liquidity_usd,
                        current_price = EXCLUDED.current_price,
                        market_cap = EXCLUDED.market_cap,
                        total_volume = EXCLUDED.total_volume,
                        updated_at = NOW()
                ''', (
                    NETWORK, name, symbol, liquidity_usd, token_address,
                    current_price, market_cap, liquidity_usd
                ))
                
                saved_count += 1
                
                # Выводим прогресс каждые 50 токенов
                if saved_count % 50 == 0:
                    print(f"  💾 Сохранено {saved_count} токенов...")
                    
            except Exception as e:
                print(f"  ⚠️ Ошибка при сохранении токена {token.get('symbol')}: {e}")
                continue
        
        conn.commit()
        cursor.close()
        db_pool.putconn(conn)
        
    except Exception as e:
        print(f"❌ Ошибка при работе с базой данных: {e}")
    
    return saved_count

def display_token_stats(tokens):
    """Выводит статистику собранных токенов"""
    if not tokens:
        print("📊 Нет данных для статистики")
        return
    
    print("\n📊 СТАТИСТИКА СОБРАННЫХ ТОКЕНОВ:")
    print("-" * 50)
    
    # Топ-5 по ликвидности
    sorted_tokens = sorted(tokens, key=lambda x: x.get('total_volume', 0), reverse=True)
    
    print("Топ-5 самых ликвидных токенов:")
    for i, token in enumerate(sorted_tokens[:5], 1):
        symbol = token.get('symbol', 'N/A').upper()
        volume = token.get('total_volume', 0)
        price = token.get('current_price', 0)
        print(f"  {i}. {symbol:8} - Объём: ${volume:,.0f} | Цена: ${price:.6f}")
    
    # Общая статистика
    total_volume = sum(t.get('total_volume', 0) for t in tokens)
    avg_volume = total_volume / len(tokens) if tokens else 0
    
    print(f"\n📈 Общая статистика:")
    print(f"   • Всего токенов: {len(tokens)}")
    print(f"   • Общий объём торгов: ${total_volume:,.0f}")
    print(f"   • Средний объём на токен: ${avg_volume:,.0f}")
    print("-" * 50)

def main():
    """Основной рабочий процесс"""
    print("🚀 Запуск сбора ликвидных токенов BSC через CoinGecko")
    print("=" * 60)
    
    # Проверяем обязательные переменные
    if not COINGECKO_API_KEY:
        print("❌ Ключ COINGECKO_API_KEY не найден!")
        print("   Добавьте его в Railway Variables")
        return
    
    if not DATABASE_URL:
        print("❌ DATABASE_URL не найден!")
        return
    
    # Инициализируем БД
    if not init_database():
        print("❌ Не удалось инициализировать базу данных")
        return
    
    # Получаем токены с CoinGecko
    tokens = get_liquid_tokens_from_coingecko(limit=1000)
    
    if not tokens:
        print("❌ Не удалось получить токены")
        return
    
    # Показываем статистику
    display_token_stats(tokens)
    
    # Сохраняем в БД
    print("\n💾 Сохранение токенов в базу данных...")
    saved_count = save_tokens_to_db(tokens)
    
    print(f"\n✅ ВЫПОЛНЕНО!")
    print(f"   • Получено токенов: {len(tokens)}")
    print(f"   • Сохранено в БД: {saved_count}")
    
    # Проверяем запись
    if db_pool and saved_count > 0:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM tokens")
            count = cursor.fetchone()[0]
            cursor.close()
            db_pool.putconn(conn)
            print(f"   • Всего в базе: {count} токенов")
        except:
            pass
    
    print("\n🎯 Готово! Проверьте данные командой в Postgres:")
    print("   SELECT * FROM tokens ORDER BY liquidity_usd DESC LIMIT 10;")

if __name__ == "__main__":
    main()