SELECT
        c.name as "Company name",
        d.first_name as "First Name",
        d.last_name as "Last Name",
        d.trade_name_or_dba_if_applicable as "Trade name",
        d.phone_number as "Phone Number",
        d.email_address as "Email address",
        d.website_url as "Website URL",
        d.address_on_file_with_irs as "Address line 1",
        d.address_line_2_on_file_with_irs as "Address line 2",
        d.city_on_file_with_irs as "City",
        d.state_on_file_with_irs as "State",
        d.postal_code_on_file_with_irs as "Zip Code",
        concat(deal_owner.first_name, concat(' ',  deal_owner.last_name)) as "Deal owner",
        concat(csc_owner.first_name, concat(' ',  csc_owner.last_name)) as "Assigned CSC",
        d.sent_to_shell_law_tax_llc as "Assigned Tax Preparer",
        d.associated_ambassador_name as "Associated Ambassador Name",
        c.lifecyclestage as "Lifecycle Stage",
        d.invoice_signed_date as "Invoice Signed Date",
        d.deal_id as "Deal ID",
        c.company_id as "Company ID",
        d.dealname as deal_name
    -- LEFT JOIN hubspot.associations a on c.id = a.from_id AND a.type = 'company_to_deal' and a.unlabeled = true
    FROM {{ ref('stg_hubspot_hp_company') }} c
    LEFT JOIN {{ ref('stg_hubspot_hp_deal_company') }}  dc ON c.company_id = dc.company_id AND dc.type_id = 5
    LEFT JOIN {{ ref('stg_hubspot_hp_deal') }} d on dc.deal_id = d.deal_id
    LEFT JOIN {{ ref('stg_hubspot_hp_owner') }} deal_owner on d.owner_id = deal_owner.owner_id
    LEFT JOIN {{ ref('stg_hubspot_hp_owner') }} csc_owner on d.csc_owner = csc_owner.owner_id
    WHERE
        d.invoice_signed_date >= date_trunc('month', getDate() - interval '1 month') AND d.invoice_signed_date < date_trunc('month', getDate())
        AND
        d.dealname not ilike '%test%' AND d.dealname not ilike '%ir57%'
    ORDER BY d.invoice_signed_date desc