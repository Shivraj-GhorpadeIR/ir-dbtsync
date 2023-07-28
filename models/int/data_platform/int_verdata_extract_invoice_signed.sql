
select 
deal_id
,company_name
-- ,date_entered_legacy_stage_confirmed_lead
-- ,date_entered_legacy_stage_refund_qualified
-- ,date_entered_legacy_stage_initiated_document_upload
-- ,date_entered_legacy_stage_completed_document_upload
-- ,date_entered_legacy_stage_data_aggregation_started
-- ,date_entered_legacy_stage_data_aggregation_complete
-- ,date_entered_legacy_stage_docs_sent_to_cpa
-- ,date_entered_legacy_stage_cpa_created_invoice
-- ,date_entered_legacy_stage_invoice_signed
-- ,date_entered_legacy_stage_awaiting_refund
-- ,date_entered_legacy_stage_refund_received_finalized
-- ,date_entered_legacy_stage_closed_lost
,company_id 
,address
,address_2
,city
,state
,zip
,phone
,intake_email
,temp_email
,contact_first_name
,contact_last_name
,contact_phone_number
from {{ ref('int_honeypot_deals_with_primary_company') }}
where date_entered_legacy_stage_invoice_signed IS NOT NULL 
AND date_entered_legacy_stage_awaiting_refund IS NULL 
AND date_entered_legacy_stage_refund_received_finalized IS NULL 
AND date_entered_legacy_stage_closed_lost IS NULL 
LIMIT 500