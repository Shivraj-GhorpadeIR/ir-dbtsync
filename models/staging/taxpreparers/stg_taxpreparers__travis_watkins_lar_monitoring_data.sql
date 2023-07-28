with

    source as (

        select * from {{ source("taxpreparers", "travis_watkins_lar_monitoring_data") }}

    ),

    renamed as (

        select
            account_name,
            entity_type,
            left(year, 4) as year,
            code,
            explanation,
            to_date(date, 'MM/DD/YY') as date,
            translate(amount, '$,', '')::DECIMAL(19,2) as amount,
            to_date(refund_date, 'MM/DD/YY') as refund_date,
            translate(refund_amount, '$,', '')::Decimal(19,2) as refund_amount
        from source

    )

select *
from renamed
