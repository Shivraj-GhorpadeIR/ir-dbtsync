select * from {{ source('hubspot_hp','product_property_history') }}
