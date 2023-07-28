select 
  deal_id
, company_id
, type_id
from {{ source('hubspot_hp', 'deal_company') }}