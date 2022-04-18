cache = {}
KEY_TEMPLATE = "user_account-{user_id}-{account_id}"

async def get_user_account(user: User, account_id: int):
    key = KEY_TEMPLATE.format(user_id=user.id, account_id=account_id)
    if key not in cache:
        cache[key] = await _get_user_account(user, account_id)
    return cache[key]


  
TEMPLATE = "user_account-{user_id}-{account_id}"
CACHE_TTL_SECONDS = 24 * 60 * 60 # 24 hours
cache = CacheStorage()

async def get_user_account(user: User, account_id: int):
    key = TEMPLATE.format(user_id=user.id, account_id=account_id)
    account = await cache.get(key)
    if account:
        return account
    account = await _get_user_account(user, account_id)
    await cache.set(key, account, ttl=CACHE_TTL_SECONDS)
    return account
  

# never use it
import asyncio
import json
from functools import wraps
CACHE_TTL_HOURS = 24
_cache = CacheStorage()

def cache(function):
    @wraps(function)
    async def _wrapped(*args, **kwargs):
        cache_key = function.__module__ + function.__name__ + json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True)
        result = await _cache.get(cache_key)
        if result:
            return result
        result = await function(*args, **kwargs)
        asyncio.create_task(_cache.set(cache_key, result, ttl=CACHE_TTL_HOURS))
        return result
    return _wrapped
 
