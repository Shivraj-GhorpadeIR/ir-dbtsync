select 
deal_id AS record_id
, dealname
, n_2020_erc_q_1_amount_formatted_ AS erc_amount_2020_q1
, n_2020_erc_q_2_amount_text_ AS erc_amount_2020_q2
, n_2020_erc_q_3_amount_text_ AS erc_amount_2020_q3
, n_2020_erc_q_4_amount_text_ AS erc_amount_2020_q4
, n_2021_erc_q_1_amount_text_ AS erc_amount_2021_q1
, n_2021_erc_q_2_amount_text_ AS erc_amount_2021_q2
, n_2021_erc_q_3_amount_text_ AS erc_amount_2021_q3
, n_2021_erc_q_4_amount_formatted_ AS erc_amount_2021_q4
, est_refund_amount_formatted_ As est_refund_amount
, gross_revenue_from_credits AS dynamic_gross_revenue_from_credits
, number_of_credits 
from {{ ref('stg_hubspot_hp_deals') }}
WHERE assigned_tax_preparer = 'Carlton Fields'
AND 
number_of_credits > 0