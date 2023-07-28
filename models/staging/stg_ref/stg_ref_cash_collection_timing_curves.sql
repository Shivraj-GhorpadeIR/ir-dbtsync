select 

    _file
    ,_line
    ,_modified
    ,age
    ,inc_under_200
    ,cuml_under_200
    ,inc_over_200
    ,cuml_over_200
    ,inc_all
    ,cuml_all
    ,_fivetran_synced

from {{ source('stg_ref', 'cash_collection_timing_curves') }}