select * from {{ source('hubspot_hp','email_campaign') }}
