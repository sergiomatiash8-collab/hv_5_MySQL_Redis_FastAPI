import json
import redis

# Підключаємось до Redis
# decode_responses=True — означає що Redis повертатиме
# звичайні рядки замість байтів b"..."
client = redis.Redis(host="localhost", port=6379, decode_responses=True)

# TTL константи — час життя кешу в секундах
TTL_CAMPAIGN = 30        # 30 секунд — дані змінюються часто
TTL_ADVERTISER = 300     # 5 хвилин — змінюється рідше
TTL_USER = 60            # 1 хвилина


def get_cached(key: str):
    """
    Перевіряємо чи є дані в Redis по ключу.
    Якщо є — десеріалізуємо з JSON і повертаємо.
    Якщо немає — повертаємо None.
    """
    value = client.get(key)
    if value is None:
        return None
    return json.loads(value)


def set_cached(key: str, data, ttl: int):
    """
    Зберігаємо дані в Redis.
    Серіалізуємо в JSON бо Redis зберігає тільки рядки.
    ex=ttl означає що після TTL секунд Redis автоматично
    видалить цей запис.
    """
    client.set(key, json.dumps(data, default=str), ex=ttl)