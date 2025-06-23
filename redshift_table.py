-- Use the correct schema
SET search_path TO public;

-- Step 0: Drop tables (only if resetting – optional)
DROP TABLE IF EXISTS reddit_cleaned;
DROP TABLE IF EXISTS reddit_cleaned_staging;

-- Step 1: Create final table (deduplicated)
CREATE TABLE reddit_cleaned (
  title VARCHAR(500),
  url VARCHAR(1000) PRIMARY KEY,
  score INTEGER,
  author VARCHAR(100),
  date TIMESTAMP
);

-- Step 2: Create staging table
CREATE TABLE reddit_cleaned_staging (
  title VARCHAR(500),
  url VARCHAR(1000),
  score INTEGER,
  author VARCHAR(100),
  date TIMESTAMP
);

-- Step 3: Load cleaned data from S3 into staging table
COPY reddit_cleaned_staging
FROM 's3://myredditbuckkk/clean_data/'
IAM_ROLE 'arn:aws:iam::125570278222:role/service-role/AmazonRedshift-CommandsAccessRole-20250529T145153'
FORMAT AS PARQUET;

-- Step 4: Merge unique records into final table
BEGIN;

-- Delete matching URLs from final table
DELETE FROM reddit_cleaned
WHERE url IN (
    SELECT url FROM reddit_cleaned_staging
);

-- ✅ Insert only the latest row per URL from staging
INSERT INTO reddit_cleaned
SELECT title, url, score, author, date
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY url ORDER BY date DESC) AS rn
  FROM reddit_cleaned_staging
) sub
WHERE rn = 1;

-- Clean up staging
TRUNCATE TABLE reddit_cleaned_staging;

COMMIT;

-- Step 5: Permissions for Power BI (Replace with your actual user)

-- Check current user
SELECT current_user;

-- Replace 'admin_user' below with the actual result of the above
GRANT USAGE ON SCHEMA public TO admin_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO admin_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO admin_user;
GRANT SELECT ON public.reddit_cleaned TO admin_user;

-- Optionally create a dedicated Power BI user (only run once)
-- If the user already exists, skip the CREATE line
CREATE USER powerbi_user WITH PASSWORD 'PowerB1userSecure!';

GRANT USAGE ON SCHEMA public TO powerbi_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO powerbi_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO powerbi_user;
GRANT SELECT ON public.reddit_cleaned TO powerbi_user;

-- Step 6: Optional Checks

-- Check if duplicate URLs exist
SELECT url, COUNT(*)
FROM reddit_cleaned
GROUP BY url
HAVING COUNT(*) > 1;

-- Verify uniqueness
SELECT COUNT(*) AS total_count, COUNT(DISTINCT url) AS distinct_count
FROM reddit_cleaned;

-- Preview most recent data
SELECT * FROM reddit_cleaned
ORDER BY date DESC
LIMIT 100;