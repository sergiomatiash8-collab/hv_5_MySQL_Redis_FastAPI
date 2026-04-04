import os
import pymysql
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_campaign_performance(campaign_id: int) -> dict:
    """
    Рахуємо CTR, clicks, impressions і ad_spend для кампанії.
    CTR = clicks / impressions — показує скільки людей клікнули
    на рекламу відносно тих хто її побачив.
    """
    sql = """
        SELECT
            e.campaign_id,
            COUNT(*)                              AS impressions,
            SUM(e.click_timestamp IS NOT NULL)    AS clicks,
            ROUND(
                SUM(e.click_timestamp IS NOT NULL) * 1.0 / COUNT(*), 4
            )                                     AS ctr,
            ROUND(SUM(e.ad_cost), 2)              AS ad_spend
        FROM events e
        WHERE e.campaign_id = %s
        GROUP BY e.campaign_id
    """
    conn = get_connection()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (campaign_id,))
            return cursor.fetchone()


def get_advertiser_spending(advertiser_id: int) -> dict:
    """
    Загальні витрати рекламодавця across всіх кампаній.
    Джойнимо events → campaigns → advertisers.
    """
    sql = """
        SELECT
            a.advertiser_id,
            a.advertiser_name,
            ROUND(SUM(e.ad_cost), 2) AS total_spend
        FROM events e
        JOIN campaigns c   ON e.campaign_id   = c.campaign_id
        JOIN advertisers a ON c.advertiser_id = a.advertiser_id
        WHERE a.advertiser_id = %s
        GROUP BY a.advertiser_id, a.advertiser_name
    """
    conn = get_connection()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (advertiser_id,))
            return cursor.fetchone()


def get_user_engagements(user_id: int) -> list:
    """
    Останні 20 рекламних подій для юзера —
    які оголошення бачив, чи клікав, коли.
    """
    sql = """
        SELECT
            e.event_id,
            e.campaign_id,
            c.campaign_name,
            a.advertiser_name,
            e.event_timestamp,
            e.ad_cost,
            (e.click_timestamp IS NOT NULL) AS clicked
        FROM events e
        JOIN campaigns c   ON e.campaign_id   = c.campaign_id
        JOIN advertisers a ON c.advertiser_id = a.advertiser_id
        WHERE e.user_id = %s
        ORDER BY e.event_timestamp DESC
        LIMIT 20
    """
    conn = get_connection()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (user_id,))
            return cursor.fetchall()