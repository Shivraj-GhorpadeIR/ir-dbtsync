select 
    deal_id, 
    value, 
    date_entered,
    _fivetran_active
 FROM
    {{ source('hubspot_hp', 'deal_stage') }} 
