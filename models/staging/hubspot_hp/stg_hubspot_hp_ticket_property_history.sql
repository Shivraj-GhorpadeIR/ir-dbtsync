select * from {{ source('hubspot_hp','ticket_property_history') }}
