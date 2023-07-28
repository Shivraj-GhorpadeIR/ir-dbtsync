select 
  deal_id
, company_id
from {{ source('hubspot_milkshake', 'deal_company') }}