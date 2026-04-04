from fastapi import FastAPI, HTTPException
from api.database import (
    get_campaign_performance,
    get_advertiser_spending,
    get_user_engagements,
)
from api.cache import get_cached, set_cached, TTL_CAMPAIGN, TTL_ADVERTISER, TTL_USER

app = FastAPI(title="AdTech Analytics API")


@app.get("/campaign/{campaign_id}/performance")
def campaign_performance(campaign_id: int):
    """
    Повертає CTR, clicks, impressions і ad_spend для кампанії.
    Спочатку перевіряємо Redis — якщо є, повертаємо одразу.
    Якщо немає — йдемо в MySQL, зберігаємо в Redis і повертаємо.
    TTL: 30 секунд.
    """
    key = f"campaign:{campaign_id}:performance"

    cached = get_cached(key)
    if cached:
        return {"source": "cache", "data": cached}

    data = get_campaign_performance(campaign_id)
    if not data:
        raise HTTPException(status_code=404, detail="Campaign not found")

    set_cached(key, data, TTL_CAMPAIGN)
    return {"source": "db", "data": data}


@app.get("/advertiser/{advertiser_id}/spending")
def advertiser_spending(advertiser_id: int):
    """
    Повертає загальні витрати рекламодавця.
    TTL: 5 хвилин.
    """
    key = f"advertiser:{advertiser_id}:spending"

    cached = get_cached(key)
    if cached:
        return {"source": "cache", "data": cached}

    data = get_advertiser_spending(advertiser_id)
    if not data:
        raise HTTPException(status_code=404, detail="Advertiser not found")

    set_cached(key, data, TTL_ADVERTISER)
    return {"source": "db", "data": data}


@app.get("/user/{user_id}/engagements")
def user_engagements(user_id: int):
    """
    Повертає останні 20 рекламних подій для юзера.
    TTL: 1 хвилина.
    """
    key = f"user:{user_id}:engagements"

    cached = get_cached(key)
    if cached:
        return {"source": "cache", "data": cached}

    data = get_user_engagements(user_id)
    if not data:
        raise HTTPException(status_code=404, detail="User not found")

    set_cached(key, data, TTL_USER)
    return {"source": "db", "data": data}