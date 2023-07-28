SELECT
        d.deal_id as record_id,
        d.dealname as deal_name,
        ps.label as dealstage,
        d.total_refund_amount,
        d.closedate,
        concat(owner.first_name, concat(' ',  owner.last_name)) as deal_owner,
        d.amount,
        d.cpa_name,
        d.unique_identifier,
        d.closed_lost_category,
        d.check_any_and_all_of_the_quarters_that_your_business_was_impacted_as_a_result_of_covid_
    FROM 
        {{ ref('stg_hubspot_hp_deal') }} d
        LEFT JOIN {{ ref('stg_hubspot_hp_deal_pipeline_stage') }} ps 
        ON d.deal_pipeline_stage_id = ps.stage_id
    LEFT JOIN {{ ref('stg_hubspot_hp_owner') }} owner on d.owner_id = owner.owner_id
    where
    d.deal_pipeline_stage_id = 8207988
    and
    d.closedate > '2020-12-11'