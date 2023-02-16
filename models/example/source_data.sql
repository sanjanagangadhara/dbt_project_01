with source as (
    select * from {{ source('SOURCE_01', 'INGESTION_SOURCE_RAW_DATA') }}
),

final_01 as (
    select * from source
)

select * from final_01

