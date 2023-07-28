select 

    _file
    ,_line
    ,_modified
    ,cast(invoice_signed_date as date)  as invoice_signed_date
    ,expected_recovery_size
    ,credit_cnt
    ,estimated_ir_revenue
    ,_fivetran_synced

from {{ source('stg_ref', 'cash_collection_forecast_volume') }}