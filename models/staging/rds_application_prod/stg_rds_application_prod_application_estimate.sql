select 
id
, refund
, name 
, contact_id
from {{ source('rds_application_prod', 'application_estimate') }}