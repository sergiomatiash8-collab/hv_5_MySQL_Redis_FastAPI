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
    Return CTR, clicks, impressions і ad_spend 
    
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
    Over exp adv
    TTL: 5m
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
    20 adv ev for user
    TTL: 1m
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