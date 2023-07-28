SELECT
        d.invoice_signed_date as "Invoice Signed Date - Daily",
        c.name as "Name",
        d.credit_type as "Credit Type",
        d.ir_gross_revenue_percentage  as "IR Percentage (decimal)",
        c.estimated_refund_amount as "Est. Refund Amount",
        d.ir_gross_revenue_percentage as "IR Gross Revenue Percentage",
        c.n_50_payment as "50% Payment",
        d.company_name as "Associated Company Name",
        c.id as "Credit ID",
        d.deal_id as "Deal ID",
        co.company_id as "Company ID"
    FROM {{ ref('stg_hubspot_hp_credit') }} c
    LEFT JOIN {{ ref('stg_hubspot_hp_credit_to_deal') }} cd ON c.id = cd.from_id
    LEFT JOIN {{ ref('stg_hubspot_hp_deal') }} d ON cd.to_id = d.deal_id
    LEFT JOIN {{ ref('stg_hubspot_hp_deal_pipeline_stage') }} ps ON d.deal_pipeline_stage_id = ps.stage_id
    LEFT JOIN {{ ref('stg_hubspot_hp_deal_pipeline') }} p ON d.deal_pipeline_id = p.pipeline_id
    LEFT JOIN {{ ref('stg_hubspot_hp_deal_company') }}  dc ON d.deal_id = dc.company_id AND dc.type_id = 5
    LEFT JOIN {{ ref('stg_hubspot_hp_company') }} co ON dc.company_id = co.company_id AND dc.type_id = 5
    WHERE
        d.invoice_signed_date = current_date-1
        AND
        d.dealname not ilike '%ir57%'
        AND 
        p.pipeline_id in (11640880, 8207981)
    ORDER BY d.invoice_signed_date desc