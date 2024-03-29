select
deal_id as honeypot_deal_id,
active_stage_label,
active_stage_entered,
active_stage_id,
dealname,
company_name,
credit_type,
total_invoiced_amount,
dynamic_gross_revenue,
gross_revenue_from_credits,
calculated_refund_amount,
initcap(replace(lower( hs_analytics_latest_source ), '_', ' ')) AS hs_analytics_latest_source,
initcap(replace(lower( hs_analytics_latest_source_company ), '_', ' ')) AS hs_analytics_latest_source_company,
initcap(replace(lower( hs_analytics_latest_source_contact ), '_', ' ')) AS hs_analytics_latest_source_contact,
hs_analytics_latest_source_data_1,
hs_analytics_latest_source_data_1_company,
hs_analytics_latest_source_data_1_contact,
hs_analytics_latest_source_data_2,
hs_analytics_latest_source_data_2_company,
hs_analytics_latest_source_data_2_contact,
hs_analytics_latest_source_timestamp,
hs_analytics_latest_source_timestamp_contact,
initcap(replace(lower( hs_analytics_source ), '_', ' ')),
hs_analytics_source_data_1,
hs_analytics_source_data_2,
hs_deal_stage_probability,
tax_prep_status,
assigned_tax_preparer,
ambassador_type,
utm_campaign,
utm_content,
utm_medium,
utm_source,
d.owner_id,
number_of_associated_ambassadors,
total_rpr_amount,
latest_deal_bucket,
original_deal_bucket,
average_w2_employees_2019,
average_w2_employees_2020,
average_w2_employees_2021,
date_entered_legacy_stage_invoice_signed,
date_entered_legacy_stage_docs_sent_to_cpa,
date_entered_legacy_stage_data_aggregation_started,
date_entered_legacy_stage_confirmed_lead,
date_entered_legacy_stage_initiated_document_upload,
date_entered_legacy_stage_refund_received_finalized,
date_entered_legacy_stage_closed_lost,
date_entered_legacy_stage_refund_qualified,
date_entered_legacy_stage_completed_document_upload,
date_entered_legacy_stage_cpa_created_invoice,
date_entered_legacy_stage_awaiting_refund,
date_entered_legacy_stage_data_aggregation_complete,
CONCAT(CONCAT(o.first_name,' '),o.last_name) AS owner_full_name

FROM  {{ ref('int_honeypot_deals_with_primary_company') }} d
LEFT JOIN {{ ref('stg_hubspot_hp_owner') }} o 
ON d.owner_id = o.owner_id 