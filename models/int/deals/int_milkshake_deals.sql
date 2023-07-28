select 
ac.name as company_name_milkshake,
    ms.*,
    ae.refund AS refund,
    ae.name as refund_name
from {{ ref('prep_rds_application_prod__flattened_steps__mapped_stages') }} ms
LEFT JOIN {{ ref('stg_rds_application_prod_application_estimate') }} ae ON ms.estimate_id = ae.id
LEFT JOIN {{ ref('stg_rds_application_prod_application_company') }} ac ON ms.milkshake_deal_id = ac.deal_id