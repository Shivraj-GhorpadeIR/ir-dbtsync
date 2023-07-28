select

    sage_id
    ,hs_credit_id
    ,client_name
    ,hs_deal_id
    ,quarter
    ,tax_preparer
    ,erc_number
    ,traunch
    ,address_line1
    ,address_line2
    ,address_city
    ,address_state
    ,address_zip
    ,deal_owner
    ,raistone_flag
    ,min(contract_date)                 as contract_date
    ,max(invoice_date)                  as invoice_date
    ,max(payment_date)                  as payment_date
    ,sum(estimated_refund_amount)       as estimated_refund_amount
    ,sum(actual_refund_amount)          as actual_refund_amount
    ,sum(estimated_ir_revenue)          as estimated_ir_revenue
    ,sum(ir_fee_invoiced)               as ir_fee_invoiced
    ,sum(ir_fee_recalc)                 as ir_fee_recalc
    ,sum(excess_ir_revenue)             as excess_ir_revenue
    ,sum(updated_actual_revenue)        as updated_actual_revenue
    ,sum(legacy_revenue_variance)       as legacy_revenue_variance
    ,sum(invoiced_ar)                   as invoiced_ar
    ,sum(contract_ar)                   as contract_ar
    ,sum(total_payment_amount)          as total_payment_amount
    ,sum(credit_memo_amount)            as credit_memo_amount
    ,sum(revenue_write_off_amount)      as revenue_write_off_amount
    ,sum(bad_debt_write_off_amount)     as bad_debt_write_off_amount
    ,sum(total_write_off_amount)        as total_write_off_amount
    ,sum(revenue_including_wo)          as revenue_including_wo

from {{ ref('rpt_erc_trx_credit') }}
where quarter is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
