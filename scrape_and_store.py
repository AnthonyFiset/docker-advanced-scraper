import os
import random
import math
import psycopg2
from psycopg2.extras import execute_values
from jobspy import scrape_jobs
from datetime import datetime
import string
import re

# ---------------------------------
# PROXIES & USER-AGENTS
# ---------------------------------
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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/95.0.4638.69 Safari/537.36"
    # ... add more if you want
]

def get_random_proxy():
    return random.choice(PROXIES)

def get_random_user_agent():
    return random.choice(USER_AGENTS)

# ---------------------------------
# POSTGRES SETTINGS
# ---------------------------------
import psycopg2

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

# ---------------------------------
# SCRAPER CONFIG
# ---------------------------------
# Remove "google" to avoid 429 from Google.
SITE_LIST = ["indeed", "linkedin", "zip_recruiter", "glassdoor"] 
LOCATION = "USA"
RESULTS_WANTED = 20
HOURS_OLD = 72

# For date_posted or other columns that might be float/NaN, we'll do a safe cast.
def safe_timestamp(val):
    """
    Attempt to cast val to a valid timestamp. 
    If val is float, NaN, or otherwise invalid, return None.
    """
    if val is None:
        return None

    # If it's already a datetime, just return it
    if isinstance(val, datetime):
        return val

    # If it's a float or int, likely invalid for a timestamp
    if isinstance(val, (float, int)):
        # check if it's NaN
        if isinstance(val, float) and math.isnan(val):
            return None
        # if it's a numeric epoch, you could convert like datetime.utcfromtimestamp(val)
        # but jobspy typically doesn't return epochs, so let's just treat it as None
        return None

    # If it's a string, attempt to parse
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        # optional: parse with dateutil or datetime
        try:
            # A naive approach; you might need dateutil for advanced formats
            return datetime.fromisoformat(val)
        except:
            # fallback
            return None

    return None

def normalize_title(title: str) -> str:
    if not title:
        return ""
    title = title.lower()
    title = title.translate(str.maketrans('', '', string.punctuation))
    title = re.sub(r'\s+', ' ', title).strip()
    return title

def clean_company_name(val):
    """
    Sometimes JobSpy might return a float (NaN) for company.
    Convert any non-string or NaN to None.
    """
    if val is None:
        return None
    # If it's a string already
    if isinstance(val, str):
        if val.strip().lower() in ["nan", ""]:
            return None
        return val.strip()

    # If it's numeric or float
    if isinstance(val, (float, int)):
        # check if it's NaN
        if isinstance(val, float) and math.isnan(val):
            return None
        # else convert to string, or just None
        return str(val)

    return None

def scrape_and_store():
    conn = get_postgres_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    # 1) Fetch active keywords from DB
    cursor.execute("SELECT name FROM keywords WHERE is_active = TRUE;")
    keywords = [row[0] for row in cursor.fetchall()]
    print(f"Active Keywords in DB: {keywords}")

    for keyword in keywords:
        print(f"\nScraping for keyword: {keyword}")

        proxy_choice = get_random_proxy()
        ua_choice = get_random_user_agent()

        # Round-robin approach: pass a list of proxies to jobspy
        proxies_list = [proxy_choice] 
        headers = {"User-Agent": ua_choice}

        try:
            from jobspy import scrape_jobs
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

            jobs_records = jobs_df.to_dict("records")
            now_utc = datetime.utcnow()

            # Build sets for companies
            company_set = set()
            job_values = []

            for job in jobs_records:
                raw_title = job.get("title", None)
                norm_title = normalize_title(raw_title)

                # Clean or cast company name
                company_val = clean_company_name(job.get("company", None))

                # Clean or cast date_posted
                date_posted_val = safe_timestamp(job.get("date_posted", None))

                # Add to set for upserting companies
                if company_val:
                    company_set.add(company_val)

                record_tuple = (
                    job.get("site", None),
                    raw_title,
                    norm_title,
                    company_val,
                    job.get("location", None),
                    job.get("description", None),
                    job.get("job_type", None),
                    job.get("interval", None),
                    job.get("min_amount", None),
                    job.get("max_amount", None),
                    job.get("job_url", None),
                    date_posted_val,
                    now_utc
                )
                job_values.append(record_tuple)

            # --- Upsert Companies ---
            for cname in company_set:
                cursor.execute("""
                    INSERT INTO companies (company_name)
                    VALUES (%s)
                    ON CONFLICT (company_name) DO NOTHING;
                """, (cname,))

            # Retrieve their IDs
            company_id_map = {}
            if company_set:
                placeholders = ",".join(["%s"] * len(company_set))
                sql_comp = f"SELECT id, company_name FROM companies WHERE company_name IN ({placeholders});"
                cursor.execute(sql_comp, tuple(company_set))
                results = cursor.fetchall()
                company_id_map = {r[1]: r[0] for r in results}

            # --- Upsert Jobs ---
            insert_query = """
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
                location, description, job_type, interval_,
                min_amount, max_amount, job_url, date_posted_val, scraped_at
            ) in job_values:

                comp_id = company_id_map.get(company_val, None)
                row = (
                    site,
                    raw_title,
                    norm_title,
                    comp_id,
                    location,
                    description,
                    job_type,
                    interval_,
                    min_amount,
                    max_amount,
                    job_url,
                    date_posted_val,
                    scraped_at
                )
                final_values.append(row)

            execute_values(cursor, insert_query, final_values)
            print(f"Inserted/Updated {len(final_values)} records for '{keyword}'.")

        except Exception as e:
            print(f"Error scraping {keyword}: {e}")

    cursor.close()
    conn.close()
    print("\nScraping completed.")

if __name__ == "__main__":
    scrape_and_store()