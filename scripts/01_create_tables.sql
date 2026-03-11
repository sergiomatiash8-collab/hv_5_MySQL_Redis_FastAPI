USE adtech_gold;

SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS campaigns;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS advertisers;
SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE advertisers (
    advertiser_id   INT UNSIGNED NOT NULL AUTO_INCREMENT,
    advertiser_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (advertiser_id),
    UNIQUE KEY uq_advertiser_name (advertiser_name)
);

CREATE TABLE users (
    user_id     INT UNSIGNED NOT NULL,
    age         TINYINT UNSIGNED,
    gender      VARCHAR(20),
    location    VARCHAR(100),
    interests   VARCHAR(255),
    signup_date DATE,
    PRIMARY KEY (user_id)
);

CREATE TABLE campaigns (
    campaign_id        INT UNSIGNED NOT NULL AUTO_INCREMENT,
    advertiser_id      INT UNSIGNED NOT NULL,
    campaign_name      VARCHAR(255) NOT NULL,
    start_date         DATE,
    end_date           DATE,
    targeting_age_min  TINYINT UNSIGNED,
    targeting_age_max  TINYINT UNSIGNED,
    targeting_interest VARCHAR(100),
    targeting_country  VARCHAR(100),
    ad_slot_size       VARCHAR(50),
    budget             DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    PRIMARY KEY (campaign_id),
    FOREIGN KEY (advertiser_id) REFERENCES advertisers(advertiser_id)
);

CREATE TABLE events (
    event_id        VARCHAR(36) NOT NULL,
    campaign_id     INT UNSIGNED NOT NULL,
    user_id         INT UNSIGNED NOT NULL,
    device          VARCHAR(50),
    location        VARCHAR(100),
    event_timestamp DATETIME NOT NULL,
    bid_amount      DECIMAL(10,4),
    ad_cost         DECIMAL(10,4),
    ad_revenue      DECIMAL(10,4),
    click_timestamp DATETIME NULL,
    PRIMARY KEY (event_id),
    FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id),
    FOREIGN KEY (user_id)     REFERENCES users(user_id)
);