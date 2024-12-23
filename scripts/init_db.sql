CREATE TABLE IF NOT EXISTS keywords (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,  -- e.g. "Data Scientist"
    is_active BOOLEAN DEFAULT TRUE      -- toggle scraping on/off
);

CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(500) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS jobs (
    id SERIAL PRIMARY KEY,
    site VARCHAR(100),
    title VARCHAR(500),
    title_normalized VARCHAR(500),
    company_id INTEGER REFERENCES companies(id),
    location VARCHAR(500),
    description TEXT,
    job_type VARCHAR(100),
    interval VARCHAR(50),
    min_amount NUMERIC,
    max_amount NUMERIC,
    job_url TEXT UNIQUE,
    date_posted TIMESTAMP,
    scraped_at TIMESTAMP
);

-- Optionally, insert some starting keywords:
INSERT INTO keywords (name) VALUES ('Medical Assistant') ON CONFLICT DO NOTHING;
INSERT INTO keywords (name) VALUES ('Pharmacy Technician') ON CONFLICT DO NOTHING;
INSERT INTO keywords (name) VALUES ('Surgical Technician') ON CONFLICT DO NOTHING;
INSERT INTO keywords (name) VALUES ('Diesel Mechanic') ON CONFLICT DO NOTHING;