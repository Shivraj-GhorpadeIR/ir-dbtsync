select 

    _file
    ,_line
    ,_modified
    ,cast(date_min as date)     as date_min
    ,cast(date_max as date)     as date_max
    ,lag_under_200
    ,lag_over_200
    ,lag_all
    ,days_to_collect
    ,lag_general
    ,net_variance
    ,bad_debt
    ,_fivetran_synced

 from {{ source('stg_ref', 'cash_collection_lags') }}