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
 
    
import asyncio
from functools import wraps
from datetime import timedelta

def cache(key_function: Callable, ttl: timedelta):
    def _decorator(function):
        @wraps(function)
        async def _function(*args, **kwargs):
            cache_key = key_function(*args, **kwargs)
            result = await _cache.get(cache_key)
            if result:
                return result
            result = await function(*args, **kwargs)
            asyncio.create_task(_cache.set(cache_key, result, ttl=ttl.total_seconds()))
            return result
        return _function
    return _decorator


def _key_function(user, account_id):
    return f"user-account-{user.id}-{account_id}"


@cache(_key_function, timedelta(hours=3))
async def get_user_account(*, user: User, account_id: int):
    ...
    
    
from cashews import cache

@app.middleware("http")
async def disable_cache_for_no_store(request: Request, call_next):
    if request.method.lower() != "get":
        return await call_next(request)
    if request.headers.get("Cache-Control") in ("no-store", "no-cache"):
        with cache.disabling("get", "set"):
            return await call_next(request)
    return await call_next(request)


from cashews import cache

@app.middleware("http")
async def add_from_cache_headers(request: Request, call_next):
    with cache.detect as detector:
        response = await call_next(request)
        if request.method.lower() != "get":
            return response
        if detector.calls:
            response.headers["X-From-Cache-keys"] = ";".join(detector.calls.keys())
    return response


from cashews import cache

@app.get("/friends")
@cache(ttl="10h")
async def get_fiends(user: User = Depends(get_current_user)):
    ...


@app.post("/friends")
@cache.invalidate(get_fiends)
async def create_friend(user: User = Depends(get_current_user)):
    ...

