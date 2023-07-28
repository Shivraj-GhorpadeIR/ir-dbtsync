select 
deal_id
, dealname
, CONCAT(CONCAT(o.first_name,' '),o.last_name) AS owner_full_name
, t.name as deal_owner_hubspot_team
, number_of_associated_solutions_escalation_tickets
from {{ ref('stg_hubspot_hp_deal') }} d 
LEFT JOIN {{ ref('stg_hubspot_hp_deal_pipeline_stage') }} ps
ON d.deal_pipeline_stage_id = ps.stage_id
LEFT JOIN {{ ref('stg_hubspot_hp_deal_pipeline') }} p
ON d.deal_pipeline_id = p.pipeline_id
LEFT JOIN {{ ref('stg_hubspot_hp_owner') }} o 
ON d.owner_id = o.owner_id 
LEFT JOIN {{ ref('stg_hubspot_hp_owner_team') }} ot
ON o.owner_id = ot.owner_id
LEFT JOIN {{ ref('stg_hubspot_hp_team') }} t
ON ot.team_id = t.id
WHERE ps.label = 'Docs Sent to CPA'
AND p.label = 'The Honey Pot'
AND sent_to_shell_law_tax_llc = 'Carrier Law'

