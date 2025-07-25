#!/bin/bash

for file in /docker-entrypoint-initdb.d/data/custom_kaggle_datasets/*; do
  echo "Importing $file ..."
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\COPY review(
      marketplace,
      customer_id,
      id,
      product_id,
      star_rating,
      helpful_votes,
      total_votes,
      verified_purchase,
      review_headline,
      review_body,
      review_date
    )
    FROM '$file'
    WITH (
      FORMAT csv,
      DELIMITER E'\t',
      HEADER,
      QUOTE '\"',
      ESCAPE E'\\\\',
      NULL '',
      FORCE_NULL (helpful_votes, total_votes, review_date)
    );"
done