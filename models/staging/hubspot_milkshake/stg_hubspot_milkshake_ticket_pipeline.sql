select * from {{ source('hubspot_milkshake','ticket_pipeline') }}
