select * from {{ source('hubspot_hp','merged_deal') }}
