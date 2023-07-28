select 
SUM(CASE WHEN datediff(day,property_customer_care_timestamp, sysdate ) between 0 AND 10 THEN 1 ELSE 0 END ) AS less_than_10_days
, SUM(CASE WHEN datediff(day,property_customer_care_timestamp, sysdate ) between 11 AND 21 THEN 1 ELSE 0 END ) AS less_than_21_days
, SUM(CASE WHEN datediff(day,property_customer_care_timestamp, sysdate ) between 22 AND 32 THEN 1 ELSE 0 END ) AS less_than_32_days
, SUM(CASE WHEN datediff(day,property_customer_care_timestamp, sysdate ) > 33 THEN 1 ELSE 0 END ) AS more_than_33_days
from {{ ref('stg_hubspot_hp_credit') }}
where property_customer_care_sub_status = 'Customer Care Original'


