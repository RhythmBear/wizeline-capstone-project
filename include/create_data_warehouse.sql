-- Fact table:
CREATE SCHEMA IF NOT EXISTS dw;
CREATE TABLE IF NOT EXISTS dw.fact_movie_analytics (
    customerid INTEGER,
    id_dim_devices VARCHAR,
    id_dim_location VARCHAR,
    id_dim_os VARCHAR,
    id_dim_date VARCHAR,
    amount_spent DECIMAL(18, 5),
    review_score INTEGER,
    review_count INTEGER,
    insert_date DATE);

-- Dim tables:
CREATE TABLE IF NOT EXISTS dw.dim_date (
    id_dim_date INTEGER,
    log_date DATE,
    day VARCHAR,
    month VARCHAR,
    year VARCHAR,
    season VARCHAR
);

CREATE TABLE IF NOT EXISTS dw.dim_devices (
    id_dim_devices INTEGER,
    device VARCHAR
);

CREATE TABLE IF NOT EXISTS dw.dim_location (
    id_dim_location INTEGER,
    location VARCHAR
);

CREATE TABLE IF NOT EXISTS dw.dim_os (
    id_dim_os INTEGER,
    os VARCHAR
);

