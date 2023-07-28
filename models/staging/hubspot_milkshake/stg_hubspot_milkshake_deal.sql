SELECT
    deal_id
, property_milkshake_deal_id
, property_name
, property_hs_object_id
, property_dealname
, property_company_name
, deal_pipeline_stage_id
, deal_pipeline_id
FROM
    {{ source('hubspot_milkshake', 'deal') }}