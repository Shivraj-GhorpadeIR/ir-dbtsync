select status, count(*) from (
SELECT
        c.status,
        c.estimated_refund_amount,
        d.dealname,
        c.name,
        c.id as credit_id,
        d.deal_id 
    FROM {{ ref('stg_hubspot_hp_credit') }} c
inner join {{ ref('stg_hubspot_hp_credit_to_deal') }} cd on cd.from_id = c.id and cd.type = 'credit_to_deal'
inner join {{ ref('stg_hubspot_hp_deals') }} d on cd.to_id = d.deal_id
    order by c.status
)
group by 1
order by 2