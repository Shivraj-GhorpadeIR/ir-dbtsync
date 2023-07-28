select dealname
-- , associated_company_id 
, company_name
, state_of_domestic_registration
, phone_number
, c.contact_phone_number
, c.company_id
-- , c.address
-- , c.address_2
, address_line_1
, address_line_2
, d.city
, city_st_zip
, postal_code
, c.country
from 
{{ ref('stg_hubspot_hp_deal') }} d
LEFT JOIN {{ ref('stg_hubspot_hp_deal_company') }} dc
ON d.deal_id = dc.deal_id
LEFT JOIN {{ ref('stg_hubspot_hp_company') }} c
ON dc.company_id = c.company_id