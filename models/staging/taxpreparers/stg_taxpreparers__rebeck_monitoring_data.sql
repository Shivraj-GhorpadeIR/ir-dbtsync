with 

source as (

    select * from {{ source('taxpreparers', 'rebeck_monitoring_data') }}

),

renamed as (

    select
        account_name,
        entity_type,
        year,
        code,
        explanation,
        TO_DATE(date, 'MM/DD/YY') as date,
        translate(amount, '$,', '')::DECIMAL(19,2) as amount
    from source

)

select * from renamed