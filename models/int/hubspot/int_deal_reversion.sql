SELECT dealname
    , CONCAT(CONCAT(d.dealname,' '),d.deal_id) As record_id
    , DATE(d.closedate) as close_date_daily
from {{ ref('stg_hubspot_hp_deals') }} d 
LEFT JOIN {{ ref('stg_hubspot_hp_deal_pipeline_stage') }} ps
ON d.deal_pipeline_stage_id = ps.stage_id
LEFT JOIN {{ ref('stg_hubspot_hp_deal_pipeline') }} p
ON d.deal_pipeline_id = p.pipeline_id
WHERE ps.label = 'Closed Lost'
AND p.label = 'The Honey Pot'
AND d.dealname NOT ilike '%ir57%'
AND d.dealname NOT ilike '%test%'
AND d.dealname NOT ilike '%impulse%' 
AND d.deal_id IN 
    (SELECT 
    DISTINCT(deal_id) 
    FROM {{ ref('stg_hubspot_hp_deal_stage') }}
    WHERE value IN ('3090816','10451316', '3090817'))