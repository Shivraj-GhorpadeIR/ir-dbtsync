select 
d.deal_id AS record_id
, d.dealname
, ps.label AS deal_stage
, d.assigned_tax_preparer
-- , d.sent_to_shell_law_tax_llc AS assigned_tax_preparer
, DATE(d.n_941_x_upload_date) AS upload_date_941
, d.statement_of_facts_received
, DATE(d.statement_of_facts_upload_date)
, d.time_between_docs_sent_to_cpa_and_941_x_upload_date/3600000 AS time_between_docs_sent_to_cpa_and_941_x_upload_date
, d.at_any_point_in_time_during_2020_or_2021_did_you_utilize_a_peo_professional_employer_organization_ AS PEO
, d.tax_attorney_rejected_date 
from {{ ref('stg_hubspot_hp_deals') }} d 
LEFT JOIN {{ ref('stg_hubspot_hp_deal_pipeline_stage') }} ps
ON d.deal_pipeline_stage_id = ps.stage_id
WHERE assigned_tax_preparer IN ('Rebeck', 'Travis Watkins Law', 'Thompson Coburn', 'Shell Law & Tax', 'Carlton Fields', 'DLA Piper')
-- AND d.dealname ilike '%ERC | McCullough Implement Co - Steve McCullough%'