select 
  stage_id
, pipeline_id
, label
, display_order
, closed_won
from {{ source('hubspot_hp', 'deal_pipeline_stage') }} where pipeline_id = '8207981'

