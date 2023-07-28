select 
    CASE WHEN h.honeypot_deal_id IS NULL THEN 'Milkshake' WHEN m.milkshake_deal_id IS NULL THEN 'Honeypot' END AS source,
    h.*, 
    m.*
from {{ ref('int_honeypot_deals') }} h
FULL OUTER JOIN {{ ref('int_milkshake_deals') }} m ON h.total_invoiced_amount = m.milkshake_deal_id