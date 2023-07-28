SELECT 
  d.company_name as "Company Name",
  d.legal_company_name as "Legal Company Name",
  d.deal_id as "Deal ID",
  s.label as "Deal Stage",
  date_trunc('month', d.invoice_signed_date)::date as "Invoice Signed Date - Monthly"
from {{ ref('stg_hubspot_hp_deal') }} d
left join {{ ref('stg_hubspot_hp_deal_pipeline_stage') }} s on s.stage_id = d.deal_pipeline_stage_id