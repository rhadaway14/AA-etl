-- Run these from Capella Query Workbench after creating buckets/scopes/collections.
-- The ingestion path uses KV operations, so indexes are for validation and operational queries only.

CREATE INDEX ix_changes_fare_time
ON `fare_history`.`airline`.`fare_changes_7d`(fare_key, changed_at);

CREATE INDEX ix_changes_batch_time
ON `fare_history`.`airline`.`fare_changes_7d`(source.batch_id, changed_at);

CREATE INDEX ix_current_route_date
ON `fares`.`airline`.`current_fares`(
  fare_data.carrier_code,
  fare_data.origin_city_code,
  fare_data.destination_city_code,
  fare_data.effective_date
);
