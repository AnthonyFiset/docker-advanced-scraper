import os
import random
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
    # ... (include all your proxies here)
    "45.196.50.172:6494",
    "45.196.41.144:6518"
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/89.0",
    # ... add more user agents if you want
]

def get_random_proxy():
    return random.choice(PROXIES)

def get_random_user_agent():
    return random.choice(USER_AGENTS)

# ---------------------------------
# POSTGRES CONFIG
# ---------------------------------
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
# SCRAPER SETTINGS
# ---------------------------------
SITE_LIST = ["indeed", "linkedin", "zip_recruiter", "glassdoor", "google"]
LOCATION = "USA"
RESULTS_WANTED = 20
HOURS_OLD = 72
# e.g. could pass country_indeed='USA', or google_search_term, etc. if needed.

# ---------------------------------
# TITLE NORMALIZATION
# ---------------------------------
def normalize_title(title: str) -> str:
    if not title:
        return ""
    title = title.lower()
    title = title.translate(str.maketrans('', '', string.punctuation))
    title = re.sub(r'\s+', ' ', title).strip()
    return title

def scrape_and_store():
    conn = get_postgres_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    # 1) Get active keywords from DB
    cursor.execute("SELECT name FROM keywords WHERE is_active = TRUE;")
    rows = cursor.fetchall()
    keywords = [row[0] for row in rows]
    print(f"Active Keywords in DB: {keywords}")

    for keyword in keywords:
        print(f"\nScraping for keyword: {keyword}")

        # Random proxy & user-agent
        proxy_choice = get_random_proxy()
        ua_choice = get_random_user_agent()

        # For JobSpy, proxies can be a list or a single "round robin" approach
        # We'll do single for simplicity, but you can pass multiple if needed.
        proxies_dict = [ proxy_choice ]  # If you pass a list, it round-robins.

        # Alternatively, if you want to pass them as the doc suggests:
        # proxies_dict = ["user:pass@host:port", "127.0.0.1:8080"]

        try:
            # 2) Scrape jobs using jobspy
            jobs_df = scrape_jobs(
                site_name=SITE_LIST,
                search_term=keyword,
                location=LOCATION,
                results_wanted=RESULTS_WANTED,
                hours_old=HOURS_OLD,
                proxies=proxies_dict,
                # google_search_term="software engineer jobs near USA since yesterday", # If you want a specific Google param
                country_indeed='USA',
                # Additional parameters as you see fit
            )

            if jobs_df is None or jobs_df.empty:
                print(f"No jobs found for '{keyword}'")
                continue

            # Convert to records
            jobs_records = jobs_df.to_dict(orient="records")
            now_utc = datetime.utcnow()

            # We'll upsert into companies & jobs
            company_set = set()
            job_values = []

            for job in jobs_records:
                raw_title = job.get("title", None)
                norm_title = normalize_title(raw_title)
                company_name = job.get("company", None)

                if company_name:
                    company_set.add(company_name)

                rec = (
                    job.get("site", None),
                    raw_title,
                    norm_title,
                    company_name,
                    job.get("location", None),
                    job.get("description", None),
                    job.get("job_type", None),
                    job.get("interval", None),
                    job.get("min_amount", None),
                    job.get("max_amount", None),
                    job.get("job_url", None),
                    job.get("date_posted", None),
                    now_utc
                )
                job_values.append(rec)

            # --- UPSERT COMPANIES ---
            for cname in company_set:
                cursor.execute("""
                    INSERT INTO companies (company_name)
                    VALUES (%s)
                    ON CONFLICT (company_name) DO NOTHING;
                """, (cname,))

            # Now get the company IDs
            if company_set:
                placeholders = ",".join(["%s"] * len(company_set))
                cursor.execute(f"SELECT id, company_name FROM companies WHERE company_name IN ({placeholders});",
                               tuple(company_set))
                results = cursor.fetchall()
                company_id_map = {r[1]: r[0] for r in results}
            else:
                company_id_map = {}

            # --- UPSERT JOBS ---
            # We'll skip rewriting if *all fields are identical* using a WHERE clause
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

            from psycopg2.extras import execute_values
            final_values = []
            for (
                site, raw_title, norm_title, company_name,
                location, description, job_type, interval_,
                min_amount, max_amount, job_url, date_posted, scraped_at
            ) in job_values:

                comp_id = company_id_map.get(company_name, None) if company_name else None
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
                    date_posted,
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