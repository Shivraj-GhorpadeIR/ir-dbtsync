select * from (
    select 
        ds.deal_id, 
        concat('date_entered_legacy_stage_', LOWER(replace( replace( ps.label, ' ', '_'), '/', '_' )))::varchar as active_step_pivot, 
        ds.date_entered 
    from {{ ref('stg_hubspot_hp_deal_stage') }} ds
    left join {{ ref('stg_hubspot_hp_deal_pipeline_stage')}} ps ON ps.stage_id = ds.value
    where ds.value in ('3090816',
    '8207984',
    '28761889',
    '36440226',
    '3591584',
    '3090817',
    '3090818',
    '8207982',
    '8207983',
    '8207988',
    '10451316',
    '28761890')
) PIVOT (
    min(date_entered)
    FOR active_step_pivot IN (
        'date_entered_legacy_stage_invoice_signed',
        'date_entered_legacy_stage_docs_sent_to_cpa',
        'date_entered_legacy_stage_data_aggregation_started',
        'date_entered_legacy_stage_confirmed_lead',
        'date_entered_legacy_stage_initiated_document_upload',
        'date_entered_legacy_stage_refund_received_finalized',
        'date_entered_legacy_stage_closed_lost',
        'date_entered_legacy_stage_refund_qualified',
        'date_entered_legacy_stage_completed_document_upload',
        'date_entered_legacy_stage_cpa_created_invoice',
        'date_entered_legacy_stage_awaiting_refund',
        'date_entered_legacy_stage_data_aggregation_complete'
        )
)
