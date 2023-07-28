select * from {{ source('hubspot_milkshake','association_type') }}
