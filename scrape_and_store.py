import os
import math
import random
import string
import re
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

##############################
#  PROXIES & USER-AGENTS
##############################
PROXIES = [
    "185.253.122.217:6026",
    "103.130.178.80:5744",
    "45.196.63.81:6715",
    "45.196.60.195:6535",
    "82.140.180.251:7211",
    "185.253.122.12:5821",
    "45.248.55.44:6630",
    "216.170.122.8:6046",
    "103.210.12.69:5997",
    "185.253.122.141:5950",
    "130.180.233.94:7665",
    "69.91.142.66:7558",
    "193.160.82.52:6024",
    "45.196.32.52:5684",
    "154.194.16.147:6066",
    "156.238.179.205:6773",
    "208.66.76.153:6077",
    "156.238.179.131:6699",
    "104.243.210.239:5887",
    "192.46.200.216:5886",
    "45.196.54.179:6758",
    "103.210.12.101:6029",
    "216.98.249.224:7205",
    "130.180.228.37:6321",
    "192.53.137.157:6445",
    "130.180.236.55:6060",
    "216.170.122.2:6040",
    "192.46.190.52:6645",
    "130.180.233.114:7685",
    "103.210.12.179:6107",
    "82.140.180.243:7203",
    "103.130.178.214:5878",
    "192.46.200.157:5827",
    "192.53.137.23:6311",
    "192.46.185.226:5916",
    "193.160.82.42:6014",
    "216.170.122.121:6159",
    "154.194.26.130:6371",
    "154.194.24.247:5857",
    "192.46.201.226:6740",
    "45.196.43.83:5810",
    "45.196.54.71:6650",
    "192.46.188.28:5687",
    "216.98.255.73:6695",
    "216.98.255.169:6791",
    "192.46.187.137:6715",
    "154.194.26.159:6400",
    "45.196.50.172:6494",
    "45.196.41.144:6518"
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    # ... add more if you want
]

def get_random_proxy():
    return random.choice(PROXIES)

def get_random_user_agent():
    return random.choice(USER_AGENTS)

##############################
#   POSTGRES CONFIG
##############################
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "db")
POSTGRES_DB   = os.getenv("POSTGRES_DB", "job_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASS = os.getenv("POSTGRES_PASS", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

def get_postgres_connection():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASS,
        port=POSTGRES_PORT
    )
    return conn

##############################
#  SCRAPER SETTINGS
##############################

# >>> IMPORTANT: If you want to omit Google due to timeouts/429, REMOVE "google" <<<
SITE_LIST = ["indeed", "linkedin", "zip_recruiter", "glassdoor"]
LOCATION = "USA"
RESULTS_WANTED = 1000000000
HOURS_OLD = 72

##############################
#  DATA CLEANING FUNCTIONS
##############################

def normalize_title(title: str) -> str:
    """Make job title lowercase, remove punctuation, etc."""
    if not title:
        return ""
    title = title.lower()
    title = title.translate(str.maketrans('', '', string.punctuation))
    title = re.sub(r'\s+', ' ', title).strip()
    return title

def safe_timestamp(val):
    """
    Convert the value to a valid datetime or return None.
    Some sites return 'NaN' or floats for date_posted.
    """
    if val is None:
        return None

    # if it's already datetime
    if isinstance(val, datetime):
        return val

    # if it's float or int
    if isinstance(val, (float, int)):
        # e.g. 'NaN' or numeric epoch -> treat as None
        if isinstance(val, float) and math.isnan(val):
            return None
        # If you want to convert numeric epoch -> datetime, do that. Otherwise None:
        return None

    # if it's string
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        # Attempt ISO parse
        try:
            return datetime.fromisoformat(val)
        except:
            # e.g. "NaN" or unknown format
            return None

    # fallback
    return None

def clean_company_name(val):
    """
    Convert numeric or NaN company names to None or a safe string.
    This ensures we never pass floats to the WHERE clause for company_name.
    """
    if val is None:
        return None

    if isinstance(val, str):
        # handle 'NaN' or empty
        v = val.strip()
        if v.lower() == "nan" or v == "":
            return None
        return v

    if isinstance(val, (float, int)):
        # if it's float and is NaN
        if isinstance(val, float) and math.isnan(val):
            return None
        # else we can cast to string or just discard
        return str(val)

    return None

##############################
#  MAIN SCRAPER LOGIC
##############################
def scrape_and_store():
    from jobspy import scrape_jobs

    conn = get_postgres_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    # 1) Fetch active keywords from the DB
    cursor.execute("SELECT name FROM keywords WHERE is_active = TRUE;")
    keywords = [row[0] for row in cursor.fetchall()]
    print(f"Active Keywords in DB: {keywords}")

    for keyword in keywords:
        print(f"\nScraping for keyword: {keyword}")

        # rotate proxy + user-agent
        proxy_choice = get_random_proxy()
        ua_choice = get_random_user_agent()
        proxies_list = [proxy_choice] 
        headers = {"User-Agent": ua_choice}

        try:
            jobs_df = scrape_jobs(
                site_name=SITE_LIST,
                search_term=keyword,
                location=LOCATION,
                results_wanted=RESULTS_WANTED,
                hours_old=HOURS_OLD,
                proxies=proxies_list,
                headers=headers,
                country_indeed='USA'
            )

            if jobs_df is None or jobs_df.empty:
                print(f"No jobs found for '{keyword}'")
                continue

            records = jobs_df.to_dict("records")
            now_utc = datetime.utcnow()

            # set() for unique companies
            companies_set = set()
            job_values = []

            for job in records:
                raw_title = job.get("title")
                norm_title = normalize_title(raw_title)
                
                company_val = clean_company_name(job.get("company"))
                date_posted_val = safe_timestamp(job.get("date_posted"))

                # add to set
                if company_val:
                    companies_set.add(company_val)

                row = (
                    job.get("site"),
                    raw_title,
                    norm_title,
                    company_val,
                    job.get("location"),
                    job.get("description"),
                    job.get("job_type"),
                    job.get("interval"),
                    job.get("min_amount"),
                    job.get("max_amount"),
                    job.get("job_url"),
                    date_posted_val,
                    now_utc
                )
                job_values.append(row)

            # -- UPSERT COMPANIES --
            for cname in companies_set:
                cursor.execute("""
                    INSERT INTO companies (company_name)
                    VALUES (%s)
                    ON CONFLICT (company_name) DO NOTHING;
                """, (cname,))

            # fetch company IDs
            company_id_map = {}
            if companies_set:
                placeholders = ",".join(["%s"]*len(companies_set))
                sql = f"""
                    SELECT id, company_name 
                    FROM companies
                    WHERE company_name IN ({placeholders})
                """
                cursor.execute(sql, tuple(companies_set))
                results = cursor.fetchall()
                company_id_map = {r[1]: r[0] for r in results}

            # -- UPSERT JOBS --
            insert_sql = """
                INSERT INTO jobs (
                    site, title, title_normalized, company_id,
                    location, description, job_type, interval,
                    min_amount, max_amount, job_url, date_posted, scraped_at
                )
                VALUES %s
                ON CONFLICT (job_url)
                DO UPDATE
                    SET
                        site = EXCLUDED.site,
                        title = EXCLUDED.title,
                        title_normalized = EXCLUDED.title_normalized,
                        company_id = EXCLUDED.company_id,
                        location = EXCLUDED.location,
                        description = EXCLUDED.description,
                        job_type = EXCLUDED.job_type,
                        interval = EXCLUDED.interval,
                        min_amount = EXCLUDED.min_amount,
                        max_amount = EXCLUDED.max_amount,
                        date_posted = EXCLUDED.date_posted,
                        scraped_at = EXCLUDED.scraped_at
                    WHERE
                        jobs.site IS DISTINCT FROM EXCLUDED.site
                        OR jobs.title IS DISTINCT FROM EXCLUDED.title
                        OR jobs.title_normalized IS DISTINCT FROM EXCLUDED.title_normalized
                        OR jobs.company_id IS DISTINCT FROM EXCLUDED.company_id
                        OR jobs.location IS DISTINCT FROM EXCLUDED.location
                        OR jobs.description IS DISTINCT FROM EXCLUDED.description
                        OR jobs.job_type IS DISTINCT FROM EXCLUDED.job_type
                        OR jobs.interval IS DISTINCT FROM EXCLUDED.interval
                        OR jobs.min_amount IS DISTINCT FROM EXCLUDED.min_amount
                        OR jobs.max_amount IS DISTINCT FROM EXCLUDED.max_amount
                        OR jobs.date_posted IS DISTINCT FROM EXCLUDED.date_posted
                        OR jobs.scraped_at IS DISTINCT FROM EXCLUDED.scraped_at
            """

            final_values = []
            for (
                site, raw_title, norm_title, company_val,
                loc, desc, job_type, interval_, min_amt, max_amt,
                job_url, date_posted_val, scraped_at
            ) in job_values:
                cid = company_id_map.get(company_val)
                row2 = (
                    site,
                    raw_title,
                    norm_title,
                    cid,
                    loc,
                    desc,
                    job_type,
                    interval_,
                    min_amt,
                    max_amt,
                    job_url,
                    date_posted_val,
                    scraped_at
                )
                final_values.append(row2)

            execute_values(cursor, insert_sql, final_values)
            print(f"Inserted/Updated {len(final_values)} records for '{keyword}'.")

        except Exception as e:
            print(f"Error scraping {keyword}: {e}")

    cursor.close()
    conn.close()
    print("\nScraping completed.")

if __name__ == "__main__":
    scrape_and_store()