with 

source as (

    select * from {{ source('taxpreparers', 'crl_monitoring_data') }}

),

renamed as (

    select
        case_id,
        company,
        ein,
        form,
        period,
        type,
        transaction_code,
        transaction_desc,
        TO_DATE(trans_date, 'MM/DD/YY') as trans_date,
        transaction_amount
    from source

)

select * from renamed
