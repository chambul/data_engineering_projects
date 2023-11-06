CREATE OR REPLACE TABLE `tlc_taxi_dataset.analytics` AS (
SELECT
ft.trip_id,
ft.VendorID,
ft.passenger_count,
ft.trip_distance,
ddt.tpep_pickup_datetime,
ddt.tpep_dropoff_datetime,
dpl.pickup_latitude,
dpl.pickup_longitude,
ddl.dropoff_latitude,
ddl.dropoff_longitude,
drc.rate_code_name,
dpt.payment_type_name,
ft.fare_amount,
ft.extra,
ft.mta_tax,
ft.tip_amount,
ft.tolls_amount,
ft.improvement_surcharge,
ft.total_amount

from `tlc_taxi_dataset.fact_taxi` ft
JOIN `tlc_taxi_dataset.dim_datetime` ddt ON ft.datetime_id = ddt.datetime_id
JOIN `tlc_taxi_dataset.dim_pickup_location` dpl ON ft.pickup_loc_id = dpl.pickup_loc_id
JOIN `tlc_taxi_dataset.dim_dropoff_location` ddl ON ft.dropoff_loc_id = ddl.dropoff_loc_id
JOIN `tlc_taxi_dataset.dim_ratecode` drc ON ft.rate_code_id = drc.rate_code_id
JOIN `tlc_taxi_dataset.dim_payment_type`dpt ON ft.payment_type_id = dpt.payment_type_id
);
