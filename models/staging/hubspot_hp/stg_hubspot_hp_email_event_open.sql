select * from {{ source('hubspot_hp','email_event_open') }}
