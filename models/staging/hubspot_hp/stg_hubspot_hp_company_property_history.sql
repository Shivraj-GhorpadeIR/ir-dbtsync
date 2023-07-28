select * from {{ source('hubspot_hp','company_property_history') }}
