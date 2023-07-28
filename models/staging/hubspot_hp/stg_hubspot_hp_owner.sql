select *
, concat(first_name, concat(' ',  last_name)) as assigned_csc
 from {{ source('hubspot_hp','owner') }}
