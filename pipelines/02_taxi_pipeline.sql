-- BRONZE : lecture des fichiers Parquet sans transformation.

CREATE OR REFRESH MATERIALIZED VIEW bronze_trips_pipeline
COMMENT 'Courses NYC Taxi brutes, lues depuis le Volume raw.'
AS
SELECT *
FROM read_files(
  '/Volumes/workspace/5lake_lab/raw/nyc_trips/',
  format => 'parquet'
);


-- SILVER : déduplication, calculs et contrôles qualité.

CREATE OR REFRESH MATERIALIZED VIEW silver_trips_pipeline
(
  CONSTRAINT courses_valides EXPECT (
    trip_distance IS NOT NULL
    AND trip_distance > 0
    AND fare_amount IS NOT NULL
    AND fare_amount > 0
    AND tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND tpep_dropoff_datetime >= tpep_pickup_datetime
  ) ON VIOLATION DROP ROW,

  CONSTRAINT pourcentage_valide EXPECT (
    tip_pct IS NOT NULL
    AND tip_pct >= 0
    AND tip_pct <= 30
  ) ON VIOLATION FAIL UPDATE
)
COMMENT 'Courses dedupliquees et controlees. Duree en minutes. tip_pct synthetique entre 0 et 30 ; 15 signifie 15 %.'
AS
SELECT
  *,
  timestampdiff(
    SECOND,
    tpep_pickup_datetime,
    tpep_dropoff_datetime
  ) / 60.0 AS trip_duration_min,

  CAST(
    pmod(
      xxhash64(
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        trip_distance,
        fare_amount,
        pickup_zip,
        dropoff_zip
      ),
      3001
    ) AS DOUBLE
  ) / 100.0 AS tip_pct

FROM (
  SELECT DISTINCT *
  FROM bronze_trips_pipeline
);


-- GOLD : indicateurs quotidiens.

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_stats
COMMENT 'Statistiques par date de prise en charge. CA = somme des fare_amount en USD, hors pourboires. Pourboires simules.'
AS
SELECT
  CAST(tpep_pickup_datetime AS DATE) AS trip_date,
  COUNT(*) AS trip_count,
  AVG(trip_distance) AS avg_trip_distance,
  SUM(fare_amount) AS revenue_usd,
  AVG(tip_pct) AS avg_tip_pct,
  AVG(fare_amount * tip_pct / 100.0) AS avg_tip_amount_usd
FROM silver_trips_pipeline
GROUP BY CAST(tpep_pickup_datetime AS DATE);