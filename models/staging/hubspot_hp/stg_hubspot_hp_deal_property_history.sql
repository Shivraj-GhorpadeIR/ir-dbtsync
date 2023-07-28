select * from {{ source('hubspot_hp','deal_property_history') }}
