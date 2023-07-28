select 
    ds.deal_id, 
    ps.stage_id as active_stage_id,
    ps.label as active_stage_label, 
    ds.date_entered as active_stage_entered
from {{ ref('stg_hubspot_hp_deal_stage') }} ds
left join {{ ref('stg_hubspot_hp_deal_pipeline_stage')}} ps ON ps.stage_id = ds.value
where ds._fivetran_active = true