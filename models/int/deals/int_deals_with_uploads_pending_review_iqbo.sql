SELECT
        d.deal_id as record_id,
        d.dealname as deal_name,
        ps.label as dealstage,
        d.createdate::Date as create_date,
        d.initiated_doc_upload_date,
        concat(csc_owner.first_name, concat(' ',  csc_owner.last_name)) as assigned_csc,
        hc.do_you_provide_healthcare_for_your_employees_,
        hc.have_you_accepted_funds_from_either_a_ppp_or_ppp_2_paycheck_protection_program_,
        round(d.what_was_your_1_st_ppp_disbursement_amount_, 2) as what_was_your_1st_ppp_disbursement_amount_,
        round(d.what_was_your_2_nd_ppp_disbursement_amount_, 2) as what_was_your_2nd_ppp_disbursement_amount_,
        d.number_of_uploads_pending_review,
        concat(iq_owner.first_name, concat(' ',  iq_owner.last_name)) as assigned_iq_csc
    FROM 
        {{ ref('stg_hubspot_hp_deal') }} d
        LEFT JOIN {{ ref('stg_hubspot_hp_deal_pipeline_stage') }} ps ON d.deal_pipeline_stage_id = ps.stage_id
        LEFT JOIN {{ ref('stg_hubspot_hp_deal_pipeline') }} p ON d.deal_pipeline_id = p.pipeline_id
        LEFT JOIN {{ ref('stg_hubspot_hp_deal_company') }} dc ON d.deal_id = dc.deal_id and dc.type_id = 5
        LEFT JOIN {{ ref('int_honeypot_companies') }} hc ON dc.company_id = hc.company_id and dc.type_id = 5
        LEFT JOIN {{ ref('stg_hubspot_hp_owner') }} csc_owner on d.csc_owner = csc_owner.owner_id
        LEFT JOIN {{ ref('stg_hubspot_hp_owner') }} iq_owner on d.assigned_iq_csc = iq_owner.owner_id
        -- LEFT JOIN hubspot.stages s on d.dealstage = s.id
        -- LEFT JOIN hubspot.associations a on d.id = a.from_id
        -- LEFT JOIN hubspot.companies c on c.id = a.to_id
        -- LEFT JOIN hubspot.owners csc_owner on d.csc_owner = csc_owner.id
        -- LEFT JOIN hubspot.owners iq_owner on d.assigned_iq_csc = iq_owner.id
    WHERE
    -- ((a.unlabeled = false AND a.type = 'deal_to_company') OR a.type is null)
    --         AND
        d.createdate > '2022-03-01'
            AND
        d.deal_pipeline_stage_id = 3591584
            AND
        d.number_of_uploads_pending_review > 0
            AND
        d.dealname not ilike '%ir57%'