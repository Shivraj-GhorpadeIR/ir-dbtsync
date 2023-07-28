select *
from {{ source('stg_ref', 'quarter_erc_ref') }}