select 
id
, current_step
, active_step
, history_date 
, hubspot_id
, estimate_id
from {{ source('rds_application_prod', 'application_historicaldeal') }}