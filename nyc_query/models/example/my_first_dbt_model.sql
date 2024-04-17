with source_data as (
    select vendorid, passenger_count, trip_distance, total_amount, payment_type, ratecodeid
    from nyc_dwh
)

select * from source_data