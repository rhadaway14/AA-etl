SELECT COUNT(*) AS current_fares
FROM `fares`.`airline`.`current_fares`;

SELECT COUNT(*) AS history_events
FROM `fare_history`.`airline`.`fare_changes_7d`;

SELECT *
FROM `fares`.`airline`.`batch_control`
ORDER BY started_at DESC
LIMIT 5;

SELECT *
FROM `fare_history`.`airline`.`fare_changes_7d`
WHERE fare_key = "fare::PF17DLUA7ZA5MEGEGMCO4"
ORDER BY changed_at DESC;

SELECT source.batch_id, change_type, COUNT(*) AS cnt
FROM `fare_history`.`airline`.`fare_changes_7d`
GROUP BY source.batch_id, change_type
ORDER BY source.batch_id DESC, cnt DESC;
