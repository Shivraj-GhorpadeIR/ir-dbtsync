select 
call_id
, DATE(date_started) as date_started
, DATE(date_ended) AS date_ended, category, direction, external_number
, internal_number, target_id, target_kind, target_type, name
, email, talk_duration, voicemail, time_to_answer
, d.deal_id
, ps.label AS deal_stage
, p.label AS pipeline 
FROM {{ ref('stg_dialpad_calls_records') }} c
left join {{ ref('stg_hubspot_hp_deal') }} d
ON  c.external_number = d.phone_number 
LEFT JOIN {{ ref('stg_hubspot_hp_deal_pipeline_stage') }} ps
ON d.deal_pipeline_stage_id = ps.stage_id
LEFT JOIN {{ ref('stg_hubspot_hp_deal_pipeline') }} p
ON d.deal_pipeline_id = p.pipeline_id
where internal_number IN ('+18556002888','+18555809002', '+18437338637')
order by date_started desc



