SELECT
property_firstname AS firstname
    , property_lastname As lastname
    , property_phone As phone
    -- , property_mobilephone AS mobilephone
    -- , property_preferred_phone_number AS preferred_phone_number
    , property_email AS email
    , property_address AS address
    -- , property_state AS state
    , property_ip_state AS ip_state
    , property_ip_state_code AS ip_state_code
    -- , property_city AS city
    , property_ip_city As ip_city
    , property_zip As zip
    , property_zip_code AS zip_code
    , property_zip_codes AS zip_codes
FROM
    {{ ref('stg_hubspot_hp_contact') }}
    where property_firstname NOT ilike '%fake%'