SELECT
        c.status,
        count(*) as credit_count,
        sum(c.estimated_refund_amount) as total_estimated_refund_amount
    FROM {{ ref('stg_hubspot_hp_credit') }} c
    where status is not null
    group by c.status