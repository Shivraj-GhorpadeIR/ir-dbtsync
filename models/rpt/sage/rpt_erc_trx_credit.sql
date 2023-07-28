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
    ,trx_date
    ,raistone                                                                               as raistone_flag
    ,invoice_signed_date                                                                    as contract_date
    ,max_refund_received_date                                                               as invoice_date
    ,max_payment_received_date                                                              as payment_date

    ,expected_recovery                                                                      as estimated_refund_amount
    ,actual_recovery                                                                        as actual_refund_amount
    ,estimated_ir_revenue                                                                   as estimated_ir_revenue
    ,ir_fee_invoiced                                                                        as ir_fee_invoiced
    ,ir_fee_recalc                                                                          as ir_fee_recalc
    ,ir_fee_recalc-ir_fee_invoiced                                                          as excess_ir_revenue
    ,estimated_ir_revenue - ir_fee_invoiced + ir_fee_recalc + legacy_revenue_variance       as updated_actual_revenue
    ,legacy_revenue_variance                                                                as legacy_revenue_variance
    ,invoiced_ar                                                                            as invoiced_ar
    ,contract_ar                                                                            as contract_ar
    ,total_payment_amount                                                                   as total_payment_amount
    ,credit_memo_amount                                                                     as credit_memo_amount
    ,revenue_write_off_amount                                                               as revenue_write_off_amount
    ,bad_debt_write_off_amount                                                              as bad_debt_write_off_amount
    ,revenue_write_off_amount + bad_debt_write_off_amount                                   as total_write_off_amount
    ,estimated_ir_revenue - ir_fee_invoiced + ir_fee_recalc + 
        legacy_revenue_variance + credit_memo_amount + revenue_write_off_amount             as revenue_including_wo

from {{ ref('int_sage_erc_trx_credit') }}