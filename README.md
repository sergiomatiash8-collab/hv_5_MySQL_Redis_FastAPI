# AdTech Data Pipeline

ETL pipeline для обробки та завантаження AdTech даних у MySQL з використанням Medallion архітектури.

## Технології
- Python 3.13
- Pandas, Polars
- SQLAlchemy, mysql-connector-python
- MySQL 8.0 (Docker)
- Loguru

## Архітектура
```
RAW → Bronze (1NF) → Silver (2NF) → Gold (3NF) → MySQL
```

### Medallion шари
- **RAW** — оригінальні CSV файли без змін
- **Bronze** — 1NF: видалення дублів, типізація, зайві колонки
- **Silver** — 2NF: валідація ключів, очищення
- **Gold** — 3NF: нормалізована схема, готова до завантаження

## Схема бази даних
```
advertisers (advertiser_id PK, advertiser_name)
     ↓
campaigns (campaign_id PK, advertiser_id FK, ...)
     ↓
events (event_id PK, campaign_id FK, user_id FK, ...)
     ↑
users (user_id PK, age, gender, location, ...)
```

## Структура проєкту
```
├── docker-compose.yml
├── scripts/
│   ├── 00_copy_data.py       # копіювання raw даних
│   └── 01_create_tables.sql  # DDL скрипти
├── etl/
│   ├── pipeline.py           # головний оркестратор
│   ├── extract/extract.py    # RAW шар
│   ├── transform/
│   │   ├── bronze.py         # 1NF трансформації
│   │   ├── silver.py         # 2NF трансформації
│   │   └── gold.py           # 3NF трансформації
│   └── load/load.py          # завантаження в MySQL
├── data/
│   ├── raw/                  # оригінальні CSV
│   ├── bronze/               # parquet після 1NF
│   ├── silver/               # parquet після 2NF
│   └── gold/                 # parquet після 3NF
└── logs/                     # логи pipeline
```

## Запуск

### 1. Клонувати репозиторій
```bash
git clone <repo_url>
cd HV_1_Python_Pandas_MySQL
```

### 2. Створити віртуальне середовище
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Налаштувати змінні середовища
Створи `.env` файл:
```
DB_HOST=localhost
DB_PORT=3306
DB_NAME=adtech_gold
DB_USER=adtech_user
DB_PASSWORD=adtech_pass
DB_ROOT_PASSWORD=adtech_root
```

### 4. Запустити MySQL
```bash
docker-compose up -d
```

### 5. Створити таблиці
```bash
# Windows PowerShell
Get-Content scripts/01_create_tables.sql | docker exec -i adtech_mysql mysql -u adtech_user -padtech_pass adtech_gold
```

### 6. Запустити pipeline
```bash
python -m etl.pipeline
```

## Дані

| Таблиця | Рядків |
|---------|--------|
| advertisers | 100 |
| campaigns | 1,013 |
| users | 700,000 |
| events | 10,000,000 |

## CTR розрахунок
```sql
SELECT
    c.campaign_name,
    COUNT(*) AS impressions,
    COUNT(e.click_timestamp) AS clicks,
    ROUND(COUNT(e.click_timestamp) / COUNT(*) * 100, 2) AS ctr_pct
FROM events e
JOIN campaigns c ON e.campaign_id = c.campaign_id
GROUP BY c.campaign_id, c.campaign_name
ORDER BY ctr_pct DESC;
```