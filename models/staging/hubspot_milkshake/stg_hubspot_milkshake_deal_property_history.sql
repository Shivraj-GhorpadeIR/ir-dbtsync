select * from {{ source('hubspot_milkshake','deal_property_history') }}
