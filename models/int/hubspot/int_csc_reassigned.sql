WITH deals_grouped_by_csc_owners as (
        SELECT
            count(*) as count_of_deals,
            CAST(d.csc_owner AS SUPER)
        FROM {{ ref('stg_hubspot_hp_deals') }} d
        WHERE
            d.latest_deal_stage_before_closed_lost = 'Initiated Doc Upload'
            AND
            d.deal_pipeline_stage_id = 3090818
            AND
            d.close_lost_categories_new_ = 'Non-Responsive'
            AND
            d.closedate > trunc(getdate())-75
                        AND
            d.closedate < trunc(getdate())-16
        group by d.csc_owner
    )

SELECT
        o.assigned_csc,
        d.count_of_deals
    FROM deals_grouped_by_csc_owners d
    LEFT JOIN {{ ref('stg_hubspot_hp_owner') }} o
    on d.csc_owner = o.owner_id
    order by count_of_deals desc