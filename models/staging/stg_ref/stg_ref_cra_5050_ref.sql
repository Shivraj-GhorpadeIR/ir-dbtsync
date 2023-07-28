select 

    _file
    ,_line
    ,_modified
    ,sage_id
    ,hs_deal_id
    ,sage_gl_quarter_raw
    ,_fivetran_synced
    
from {{ source('stg_ref', 'cra_5050_ref') }}