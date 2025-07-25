CREATE TABLE IF NOT EXISTS customer (
    id BIGSERIAL PRIMARY KEY,
    name TEXT,
    sex CHAR(1),
    mail TEXT,
    birthdate DATE,
    login_username TEXT,
    login_password TEXT
);

CREATE TABLE IF NOT EXISTS location (
    id UUID PRIMARY KEY,
    street_address TEXT,
    city TEXT,
    state TEXT,
    zipcode INT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS customer_location (
    id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT REFERENCES customer(id),
    location_id UUID REFERENCES location(id)
);

CREATE TABLE IF NOT EXISTS customer_phone (
    id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT REFERENCES customer(id),
    phone_number TEXT
);

CREATE TABLE IF NOT EXISTS product (
    id TEXT PRIMARY KEY,
    product_title TEXT,
    currency VARCHAR(15),
    price DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS shadow_product (
    id UUID PRIMARY KEY,
    product_id TEXT REFERENCES product(id),
    product_title TEXT,
    currency VARCHAR(15),
    price DECIMAL(10, 2),
    updated_at DATE
);

CREATE TABLE IF NOT EXISTS category (
    id BIGSERIAL PRIMARY KEY,
    category_name TEXT
);

CREATE TABLE IF NOT EXISTS product_category (
    id BIGSERIAL PRIMARY KEY,
    product_id TEXT REFERENCES product(id),
    category_id BIGINT REFERENCES category(id)
);

CREATE TABLE IF NOT EXISTS review (
    id TEXT PRIMARY KEY,
    customer_id BIGINT REFERENCES customer(id),
    product_id TEXT REFERENCES product(id),
    star_rating VARCHAR(15),
    helpful_votes INT,
    total_votes INT,
    marketplace VARCHAR(15),
    verified_purchase CHAR(1),
    review_headline TEXT,
    review_body TEXT,
    review_date DATE
);

\echo 'IMPORTING DATA...'

\echo 'Import customer table...'
COPY customer(name, sex, login_username, mail, birthdate, login_password, id)
FROM '/docker-entrypoint-initdb.d/data/processed_datasets/customer.csv'
DELIMITER ',' CSV HEADER;

\echo 'Import location table...'
COPY location(street_address, city, state, zipcode, country, id)
FROM '/docker-entrypoint-initdb.d/data/processed_datasets/location.csv'
DELIMITER ',' CSV HEADER
ESCAPE '\';

\echo 'Import customer_location table...'
COPY customer_location(customer_id, id, location_id)
FROM '/docker-entrypoint-initdb.d/data/processed_datasets/customer_location.csv'
DELIMITER ',' CSV HEADER;

\echo 'Import customer_phone table...'
COPY customer_phone(phone_number, customer_id, id)
FROM '/docker-entrypoint-initdb.d/data/processed_datasets/customer_phone.csv'
DELIMITER ',' CSV HEADER;

\echo 'Import product table...'
COPY product(id, product_title, price, currency)
FROM '/docker-entrypoint-initdb.d/data/processed_datasets/product.tsv'
WITH (
    FORMAT  csv,
    DELIMITER E'\t',
    HEADER,
    QUOTE '"',
    ESCAPE E'\\'
);

\echo 'Import shadow_product table...'
COPY shadow_product(product_id, product_title, price, currency, updated_at, id)
FROM '/docker-entrypoint-initdb.d/data/processed_datasets/shadow_product.tsv'
WITH (
    FORMAT csv,
    DELIMITER E'\t',
    HEADER,
    QUOTE '"',
    ESCAPE E'\\'
);

\echo 'Import category table...'
COPY category(category_name, id)
FROM '/docker-entrypoint-initdb.d/data/processed_datasets/category.csv'
DELIMITER ',' CSV HEADER;

\echo 'Import product_category table...'
COPY product_category(product_id, category_id, id)
FROM '/docker-entrypoint-initdb.d/data/processed_datasets/product_category.csv'
DELIMITER ',' CSV HEADER;