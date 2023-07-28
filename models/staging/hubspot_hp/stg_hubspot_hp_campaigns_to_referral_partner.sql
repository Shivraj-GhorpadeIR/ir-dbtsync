select * from {{ source('hubspot_hp','campaigns_to_referral_partner') }}
