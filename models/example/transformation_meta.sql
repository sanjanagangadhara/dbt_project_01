with transform as (
    select * from {{ source('SOURCE_02', 'TRANSFORMATION_META') }}
),

final_02 as (
    select * from transform
)

select * from final_02