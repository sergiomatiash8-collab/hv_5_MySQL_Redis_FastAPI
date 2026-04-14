import json
import redis


client = redis.Redis(host="localhost", port=6379, decode_responses=True)

# TTL 
TTL_CAMPAIGN = 30       
TTL_ADVERTISER = 300    
TTL_USER = 60            


def get_cached(key: str):
    """
   check data redis
    """
    value = client.get(key)
    if value is None:
        return None
    return json.loads(value)


def set_cached(key: str, data, ttl: int):
    """
   save
    """
    client.set(key, json.dumps(data, default=str), ex=ttl)