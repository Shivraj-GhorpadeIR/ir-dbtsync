select 
'milkshake' AS source_pipeline
, milkshake_deal_id as deal_id
, NULL As dealname
, company_name_milkshake As company_name
, estimate_id AS estimate_id
, NULL AS hs_analytics_latest_source
, NULL AS hs_analytics_latest_source_company
, NULL AS hs_analytics_latest_source_contact
, NULL AS hs_analytics_latest_source_data_1
, NULL AS hs_analytics_latest_source_data_1_company
, NULL AS hs_analytics_latest_source_data_1_contact
, NULL AS hs_analytics_latest_source_data_2
, NULL AS hs_analytics_latest_source_data_2_company
, NULL AS hs_analytics_latest_source_data_2_contact
, NULL AS hs_analytics_latest_source_timestamp
, NULL AS hs_analytics_latest_source_timestamp_contact
, NULL AS hs_analytics_source
, NULL AS hs_analytics_source_data_1
, NULL AS hs_analytics_source_data_2
, NULL AS latest_deal_bucket
, NULL AS owner_id
, NULL AS owner_full_name
, DATE(date_entered_crm_stage_unqualified) AS date_unqualified
, DATE(date_entered_crm_stage_confirmed_lead) AS date_confirmed_lead
, DATE(date_entered_crm_stage_eligible) AS date_eligible
, DATE(date_entered_crm_stage_processing) AS date_processing
, DATE(date_entered_crm_step_review) AS date_review
, DATE(date_entered_crm_stage_contracting) AS date_contracting
, DATE(date_entered_crm_stage_closed_won) AS date_closed_won
, DATE(date_entered_crm_stage_closed_lost) AS date_closed_lost
, DATE(date_entered_crm_step_signature) AS date_signature
, NULL AS date_data_aggregation_started
, NULL AS date_refund_received_finalized
, NULL AS date_completed_document_upload
, NULL AS date_cpa_created_invoice
, NULL AS date_awaiting_refund
, NULL AS date_data_aggregation_complete
, NULL AS total_invoiced_amount
, NULL AS original_deal_bucket
, NULL AS average_w2_employees_2019
, NULL AS average_w2_employees_2020
, NULL AS average_w2_employees_2021
, refund
, refund_name
, NULL AS tax_prep_status
, NULL AS utm_campaign
, NULL AS utm_content
, NULL AS utm_medium
, NULL AS utm_source
, NULL AS dynamic_gross_revenue
, NULL AS gross_revenue_from_credits
, NULL AS calculated_refund_amount
, NULL AS hs_deal_stage_probability
, NULL AS number_of_associated_ambassadors
, NULL AS ambassador_type
, NULL AS total_rpr_amount
, NULL AS active_stage_label
, NULL AS active_stage_entered
, NULL AS active_stage_id
, NULL AS assigned_tax_preparer
, NULL AS credit_type
from {{ ref('int_milkshake_deals') }}

union 

select
'honeypot' AS source_pipeline
, honeypot_deal_id as deal_id
, dealname
, company_name_honeypot As company_name
, NULL AS estimate_id
, initcap(replace(lower( hs_analytics_latest_source ), '_', ' ')) AS hs_analytics_latest_source
, initcap(replace(lower( hs_analytics_latest_source_company ), '_', ' ')) AS hs_analytics_latest_source_company
, initcap(replace(lower( hs_analytics_latest_source_contact ), '_', ' ')) AS hs_analytics_latest_source_contact
, hs_analytics_latest_source_data_1
, hs_analytics_latest_source_data_1_company
, hs_analytics_latest_source_data_1_contact
, hs_analytics_latest_source_data_2
, hs_analytics_latest_source_data_2_company
, hs_analytics_latest_source_data_2_contact
, hs_analytics_latest_source_timestamp
, hs_analytics_latest_source_timestamp_contact
, initcap(replace(lower( hs_analytics_source ), '_', ' '))
, hs_analytics_source_data_1
, hs_analytics_source_data_2
, latest_deal_bucket
, owner_id
, owner_full_name
, NULL AS date_unqualified
, DATE(date_entered_legacy_stage_confirmed_lead) AS date_confirmed_lead
, DATE(date_entered_legacy_stage_refund_qualified) AS date_eligible
, DATE(date_entered_legacy_stage_initiated_document_upload) AS date_processing
, DATE(date_entered_legacy_stage_docs_sent_to_cpa) AS date_review
, NULL AS date_contracting
, DATE(date_entered_legacy_stage_invoice_signed) AS date_closed_won
, DATE(date_entered_legacy_stage_closed_lost) AS date_closed_lost
, NULL AS date_signature
, DATE(date_entered_legacy_stage_data_aggregation_started) AS date_data_aggregation_started
, DATE(date_entered_legacy_stage_refund_received_finalized) AS date_refund_received_finalized
, DATE(date_entered_legacy_stage_completed_document_upload) AS date_completed_document_upload
, DATE(date_entered_legacy_stage_cpa_created_invoice) AS date_cpa_created_invoice
, NULL AS date_awaiting_refund
, NULL AS date_data_aggregation_complete
, total_invoiced_amount AS total_invoiced_amount
, original_deal_bucket
, average_w2_employees_2019
, average_w2_employees_2020
, average_w2_employees_2021
, NULL AS refund
, NULL AS refund_name
, tax_prep_status
, utm_campaign
, utm_content
, utm_medium
, utm_source
, dynamic_gross_revenue
, gross_revenue_from_credits
, calculated_refund_amount
, hs_deal_stage_probability
, number_of_associated_ambassadors
, ambassador_type
, total_rpr_amount
, active_stage_label
, active_stage_entered
, active_stage_id
, assigned_tax_preparer
, credit_type
from {{ ref('int_honeypot_deals') }}