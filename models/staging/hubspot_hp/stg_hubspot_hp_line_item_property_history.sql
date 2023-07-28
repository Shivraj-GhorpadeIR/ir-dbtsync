select * from {{ source('hubspot_hp','line_item_property_history') }}
