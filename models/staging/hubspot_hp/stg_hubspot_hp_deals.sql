select 
  deal_id 
, deal_pipeline_stage_id 
, deal_pipeline_id
, owner_id
, property_dealname AS dealname
, property_credit_type AS credit_type
, property_calculated_refund_amount AS calculated_refund_amount
, property_total_invoiced_amount AS total_invoiced_amount 
, property_dynamic_gross_revenue as dynamic_gross_revenue
, property_gross_revenue_from_credits as gross_revenue_from_credits
, initcap(replace(lower( property_hs_analytics_latest_source ), '_', ' ')) as hs_analytics_latest_source
, initcap(replace(lower( property_hs_analytics_latest_source_company ), '_', ' ')) as hs_analytics_latest_source_company
, initcap(replace(lower( property_hs_analytics_latest_source_contact ), '_', ' ')) as hs_analytics_latest_source_contact
, property_hs_analytics_latest_source_data_1 as hs_analytics_latest_source_data_1
, property_hs_analytics_latest_source_data_1_company as hs_analytics_latest_source_data_1_company
, property_hs_analytics_latest_source_data_1_contact as hs_analytics_latest_source_data_1_contact
, property_hs_analytics_latest_source_data_2 as hs_analytics_latest_source_data_2
, property_hs_analytics_latest_source_data_2_company as hs_analytics_latest_source_data_2_company
, property_hs_analytics_latest_source_data_2_contact as hs_analytics_latest_source_data_2_contact
, property_hs_analytics_latest_source_timestamp as hs_analytics_latest_source_timestamp
, property_hs_analytics_latest_source_timestamp_contact as hs_analytics_latest_source_timestamp_contact
, initcap(replace(lower( property_hs_analytics_source ), '_', ' ')) as hs_analytics_source
, property_hs_analytics_source_data_1 as hs_analytics_source_data_1
, property_hs_analytics_source_data_2 as hs_analytics_source_data_2
, property_hs_deal_stage_probability as hs_deal_stage_probability
, property_tax_prep_status as tax_prep_status
, property_sent_to_shell_law_tax_llc as assigned_tax_preparer
, property_ambassador_type as ambassador_type
, property_n_941_x_upload_date as n_941_x_upload_date
, property_statement_of_facts_received as statement_of_facts_received
, property_statement_of_facts_upload_date as statement_of_facts_upload_date
, property_time_between_docs_sent_to_cpa_and_941_x_upload_date as time_between_docs_sent_to_cpa_and_941_x_upload_date
, property_at_any_point_in_time_during_2020_or_2021_did_you_utilize_a_peo_professional_employer_organization_ as at_any_point_in_time_during_2020_or_2021_did_you_utilize_a_peo_professional_employer_organization_
, property_tax_attorney_rejected_date as tax_attorney_rejected_date
, property_utm_campaign as utm_campaign
, property_utm_content as utm_content
, property_utm_medium as utm_medium
, property_utm_source as utm_source
, property_number_of_associated_ambassadors as number_of_associated_ambassadors
, property_total_rpr_amount as total_rpr_amount
, property_invoice_signed_date as invoice_signed_date
, property_original_deal_bucket as original_deal_bucket
, property_latest_deal_bucket as latest_deal_bucket
, property_n_2020_erc_q_1_amount_formatted_ as n_2020_erc_q_1_amount_formatted_
, property_n_2020_erc_q_2_amount_text_ as n_2020_erc_q_2_amount_text_
, property_n_2020_erc_q_3_amount_text_ as n_2020_erc_q_3_amount_text_
, property_n_2020_erc_q_4_amount_text_ as n_2020_erc_q_4_amount_text_
, property_n_2021_erc_q_1_amount_text_ as n_2021_erc_q_1_amount_text_
, property_n_2021_erc_q_2_amount_text_ as n_2021_erc_q_2_amount_text_
, property_n_2021_erc_q_3_amount_text_ as n_2021_erc_q_3_amount_text_
, property_n_2021_erc_q_4_amount_formatted_ as n_2021_erc_q_4_amount_formatted_
, property_est_refund_amount_formatted_ as est_refund_amount_formatted_
, property_number_of_credits as number_of_credits
, property_closedate as closedate
, property_csc_owner as csc_owner
, property_latest_deal_stage_before_closed_lost as latest_deal_stage_before_closed_lost
, property_close_lost_categories_new_ as close_lost_categories_new_
, property_company_name as company_name
, property_legal_company_name as legal_company_name
FROM
    {{ source('hubspot_hp', 'deal') }}

WHERE property_dealname NOT ilike '%ir57%'
AND property_dealname NOT ilike '%ir 57%'
AND property_dealname NOT ilike '%ir-57%'
AND property_dealname NOT ilike '%impulse creative%'
-- AND property_dealname NOT ilike '%testing%'