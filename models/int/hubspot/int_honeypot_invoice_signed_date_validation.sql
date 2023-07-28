select 
d.deal_id AS record_id
, d.dealname
, ps.label AS deal_stage
, p.label AS pipeline 
, d.invoice_signed_date
from {{ ref('stg_hubspot_hp_deal') }} d 
LEFT JOIN {{ ref('stg_hubspot_hp_deal_pipeline_stage') }} ps
ON d.deal_pipeline_stage_id = ps.stage_id
LEFT JOIN {{ ref('stg_hubspot_hp_deal_pipeline') }} p
ON d.deal_pipeline_id = p.pipeline_id
where 
ps.label IN  ('Awaiting Refund', 'Invoice Signed') 
AND p.label = 'The Honey Pot' AND invoice_signed_date IS NULL