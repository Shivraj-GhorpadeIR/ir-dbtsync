{{ config(materialized='table', sort='trx_date', dist='sage_id') }}

with so_document as (
select 
    cast(b.hs_credit_id as varchar)  as hs_credit_id
    ,cast(b.offer as varchar)       as offer
    ,b.reason_for_variance
    ,a.*
from {{ ref('stg_sage_so_document') }} a
left join {{ ref('stg_custom_sage_sodocument_custom_fields') }} b
    on a.docid = b.docid
)

, ar_invoice_item as (
select distinct 
    recordkey
    ,classid
    ,offsetglaccountno
from {{ ref('stg_sage_ar_invoice_item') }}
)

/* LOAD OE TRANSACTION DATA */
, invoice_signed_date as (
select

    a.custvendid                                    as sage_id
    ,a.hs_credit_id
	,b.classid                                      as quarter
    ,ponumber                                       as erc_number
    ,offer                                          as traunch
    ,reason_for_variance
    ,b.whencreated                                  as trx_date
    ,cast('invoice_signed_date' as varchar(100))    as attrib_name
    ,b.whencreated                                  as attrib_date
    ,cast(0 as decimal(18,2))                       as attrib_amount

from so_document a 
left join {{ ref('stg_sage_so_document_entry') }} b 
    on a.docid = b.dochdrid 
where left(a.ponumber,3) = 'ERC' and b.classid is not null and b.docparid = 'Estimates'
)

, refund_received_date as (
select

    custvendid                                      as sage_id
    ,a.hs_credit_id
    ,b.classid                                      as quarter
    ,ponumber                                       as erc_number
    ,offer                                          as traunch
    ,reason_for_variance
    ,b.whencreated                                  as trx_date,
	'refund_received_date'                          as attrib_name
    ,b.whencreated                                  as attrib_date
    ,cast(0 as decimal(18,2))                       as attrib_amount

from so_document a 
left join {{ ref('stg_sage_so_document_entry') }} b 
    on a.docid = b.dochdrid 
where left(a.ponumber,3) = 'ERC' and b.classid is not null and b.docparid = 'Sales Final'
)

, expected_recovery as (
select

    custvendid                                      as sage_id
    ,a.hs_credit_id
    ,b.classid                                      as quarter
    ,ponumber                                       as erc_number
    ,offer                                          as traunch
    ,reason_for_variance
    ,b.whencreated                                  as trx_date,
	'expected_recovery'                             as attrib_name
    ,null                                           as attrib_date
    ,sum(b.price)                                   as attrib_amount

from so_document a 
left join {{ ref('stg_sage_so_document_entry') }} b 
    on a.docid = b.dochdrid 
where left(a.ponumber,3) = 'ERC' and b.classid is not null and b.docparid = 'UDB Expected Recovery'
group by 1,2,3,4,5,6,7
)

, actual_recovery as (
select

    custvendid                                      as sage_id
    ,a.hs_credit_id
    ,b.classid                                      as quarter
    ,ponumber                                       as erc_number
    ,offer                                          as traunch
    ,reason_for_variance
    ,b.whencreated                                  as trx_date,
	'actual_recovery'                               as attrib_name
    ,null                                           as attrib_date
    ,sum(b.price)                                   as attrib_amount

from so_document a 
left join {{ ref('stg_sage_so_document_entry') }} b 
    on a.docid = b.dochdrid 
where left(a.ponumber,3) = 'ERC' and b.classid is not null and b.docparid = 'UDB Actual Recovery'
group by 1,2,3,4,5,6,7
)

, sage_invoiced_recovery as (
select

    custvendid                                      as sage_id
    ,a.hs_credit_id
    ,b.classid                                      as quarter
    ,ponumber                                       as erc_number
    ,offer                                          as traunch
    ,reason_for_variance
    ,b.whencreated                                  as trx_date,
	'sage_invoiced_recovery'                        as attrib_name
    ,null                                           as attrib_date
    ,b.price                                        as attrib_amount

from so_document a 
left join {{ ref('stg_sage_so_document_entry') }} b 
    on a.docid = b.dochdrid 
where left(a.ponumber,3) = 'ERC' and b.classid is not null and b.docparid = 'Sales Final'
)

, estimated_ir_revenue as (
select 
    custvendid                          as sage_id
    ,a.hs_credit_id
    ,b.classid                          as quarter
    ,ponumber                           as erc_number
    ,offer                              as traunch
    ,reason_for_variance
    ,b.whencreated                      as trx_date
    ,'estimated_ir_revenue'             as attrib_name
    ,null                               as attrib_date
    ,sum(b.total)                       as attrib_amount
from so_document a 
left join {{ ref('stg_sage_so_document_entry') }} b 
    on a.docid = b.dochdrid 
where left(a.ponumber,3) = 'ERC' and b.classid is not null and b.docparid = 'Estimates'
group by 1,2,3,4,5,6,7
)


, ir_fee_invoiced as (
select 

    custvendid                          as sage_id
    ,a.hs_credit_id
    ,b.classid                          as quarter
    ,ponumber                           as erc_number
    ,offer                              as traunch
    ,reason_for_variance
    ,b.whencreated                      as trx_date
    ,'ir_fee_invoiced'                  as attrib_name
    ,null                               as attrib_date
    ,b.total                            as attrib_amount

from so_document a 
left join {{ ref('stg_sage_so_document_entry') }} b 
    on a.docid = b.dochdrid 
where left(a.ponumber,3) = 'ERC' and b.classid is not null and b.docparid = 'Estimates to Final'
)

, ir_fee_recalc as (
select 

    custvendid                          as sage_id
    ,a.hs_credit_id
    ,b.classid                          as quarter
    ,ponumber                           as erc_number
    ,offer                              as traunch
    ,reason_for_variance
    ,b.whencreated                      as trx_date
    ,'ir_fee_recalc'                    as attrib_name
    ,null                               as attrib_date
    ,b.total                            as attrib_amount

from so_document a 
left join {{ ref('stg_sage_so_document_entry') }} b 
    on a.docid = b.dochdrid 
where left(a.ponumber,3) = 'ERC' and b.classid is not null and b.docparid = 'Sales Final'
)

, contract_ar as (
select 

    custvendid                          as sage_id
    ,a.hs_credit_id
    ,b.classid                          as quarter
    ,ponumber                           as erc_number
    ,offer                              as traunch
    ,reason_for_variance
    ,b.whencreated                      as trx_date
    ,'contract_ar'                      as attrib_name
    ,null                               as attrib_date
    ,sum(case 
        when b.docparid in( 'Estimates' )
            then b.total 
        else -b.total 
    end)                                 as attrib_amount
    
from so_document a 
left join {{ ref('stg_sage_so_document_entry') }} b 
    on a.docid = b.dochdrid 
where left(a.ponumber,3) = 'ERC' and b.classid is not null and b.docparid in ('Estimates','Estimates to Final')
group by 1,2,3,4,5,6,7
)

, credit_memo_amount as (
select 

    custvendid                          as sage_id
    ,a.hs_credit_id
    ,b.classid                          as quarter
    ,ponumber                           as erc_number
    ,offer                              as traunch
    ,reason_for_variance
    ,b.whencreated                      as trx_date
    ,'credit_memo_amount'               as attrib_name
    ,null                               as attrib_date
    ,-b.total                           as attrib_amount

from so_document a 
left join {{ ref('stg_sage_so_document_entry') }} b 
    on a.docid = b.dochdrid 
where left(a.ponumber,3) = 'ERC' and b.classid is not null and b.docparid = 'Credit Memo'
)

, revenue_write_off_amount as (
select 

    custvendid                          as sage_id
    ,a.hs_credit_id
    ,b.classid                          as quarter
    ,ponumber                           as erc_number
    ,offer                              as traunch
    ,reason_for_variance
    ,b.whencreated                      as trx_date
    ,'revenue_write_off_amount'         as attrib_name
    ,null                               as attrib_date
    ,-b.total                           as attrib_amount

from so_document a 
left join {{ ref('stg_sage_so_document_entry') }} b 
    on a.docid = b.dochdrid 
where left(a.ponumber,3) = 'ERC' and b.classid is not null and b.docparid = 'Write Off Rev'
)

, bad_debt_write_off_amount as (
select 

    custvendid                          as sage_id
    ,a.hs_credit_id
    ,b.classid                          as quarter
    ,ponumber                           as erc_number
    ,offer                              as traunch
    ,reason_for_variance
    ,b.whencreated                      as trx_date
    ,'bad_debt_write_off_amount'        as attrib_name
    ,null                               as attrib_date
    ,-b.total                           as attrib_amount

from so_document a 
left join {{ ref('stg_sage_so_document_entry') }} b 
    on a.docid = b.dochdrid 
where left(a.ponumber,3) = 'ERC' and b.classid is not null and b.docparid = 'Write Off'
)

/* LOAD CASH COLLECTED DATA FROM AR */
, total_payment_amount as(
select

    a.customerid                            as sage_id
    ,null                                   as hs_credit_id
    ,b.classid                              as quarter
    ,a.docnumber                            as erc_number
    ,null                                   as traunch
    ,null                                   as reason_for_variance
    ,c.paymentdate                          as trx_date
    ,'total_payment_amount'                 as attrib_name
    ,null                                   as attrib_date
    ,case 
        when left(a.recordid,3) = 'ETF'
            then -c.trx_amount
        else c.trx_amount
    end                                     as attrib_amount

from {{ ref('stg_sage_ar_invoice') }} a
join ar_invoice_item b 
    on a.recordno = b.recordkey 
join {{ ref('stg_sage_ar_invoice_payment') }} c 
    on a.recordno = c.recordkey    
join {{ ref('stg_sage_ar_payment') }} d 
    on c.parentpymt = d.recordno
where left(a.docnumber,3) = 'ERC' and ((left(a.recordid,2) = 'SF' and offsetglaccountno = 1200) or left(a.recordid,3) = 'ETF')
)

, total_payment_date as (
select 

    a.customerid                            as sage_id
    ,null                                   as hs_credit_id
    ,b.classid                              as quarter
    ,a.docnumber                            as erc_number
    ,null                                   as traunch
    ,null                                   as reason_for_variance
    ,c.paymentdate                          as trx_date
    ,'total_payment_date'                   as attrib_name
    ,c.paymentdate                          as attrib_date
    ,case
        when left(a.recordid,3) = 'ETF' 
            then 0 
        else 0 
    end                                     as attrib_amount

from {{ ref('stg_sage_ar_invoice') }} a
join ar_invoice_item b 
    on a.recordno = b.recordkey 
join {{ ref('stg_sage_ar_invoice_payment') }} c 
    on a.recordno = c.recordkey    
join {{ ref('stg_sage_ar_payment') }} d 
    on c.parentpymt = d.recordno
where left(a.docnumber,3) = 'ERC' and ((left(a.recordid,2) = 'SF' and offsetglaccountno = 1200) or left(a.recordid,3) = 'ETF')
)

/* LOAD CASH COLLECTED DATA FROM GL TO COMPARE AGAINST OE */
, total_payment_amount_gl as (
select

    sage_id
    ,cast(hs_credit_id as varchar)  as hs_credit_id
    ,quarter
    ,erc_number                     as erc_number
    ,cast(traunch as varchar)       as traunch
    ,null                           as reason_for_variance
    ,entry_date                     as trx_date
    ,'total_payment_amount_gl'      as attrib_name
    ,null                           as attrib_date
    ,amount*-1                      as attrib_amount

from {{ ref('prep_sage_erc_gl') }}
where account_category = 'Account Receivable' and quarter is not null and is_reversal = 'No' and batch_type in ('Receipts','Reversed Receipts') and acct_no = 1200 
)

/* LOAD LEGACY TRANSACTION DATA */
, legacy_invoiced_recovery as (
select 

    intacct_id                                      as sage_id
    ,cast(null as varchar)                          as hs_credit_id
    ,quarter
    ,erc_number
    ,cast(null as varchar)                          as traunch
    ,man_cra_explanation_of_diff                    as reason_for_variance
    ,man_cra_mon_yr_of_collection                   as trx_date
    ,'legacy_invoiced_recovery'                     as attrib_name
    ,null                                           as attrib_date
    ,man_cra_invoiced_recovery                      as attrib_amount

from {{ ref('stg_ref_cra_legacy_ref') }}
--where man_cra_mon_yr_of_collection is not null or man_cra_invoiced_recovery <> 0
)

, legacy_revenue_variance as (
select 

    intacct_id                      as sage_id
    ,null                           as hs_credit_id
    ,quarter
    ,erc_number
    ,null                           as traunch
    ,man_cra_explanation_of_diff    as reason_for_variance
    ,legacy_revenue_variance_date   as trx_date
    ,'legacy_revenue_variance'      as attrib_name
    ,null                           as attrib_date
    ,legacy_revenue_variance        as attrib_amount

from {{ ref('stg_ref_cra_legacy_ref') }}
where legacy_revenue_variance <> 0
)
	
, legacy_bad_debt_write_off_amount as (
select 
    intacct_id                      as sage_id
    ,null                           as hs_credit_id
    ,quarter
    ,erc_number
    ,null                           as traunch
    ,man_cra_explanation_of_diff    as reason_for_variance
    ,legacy_revenue_variance_date   as trx_date
    ,'bad_debt_write_off_amount'    as attrib_name
    ,null                           as attrib_date
    ,-legacy_revenue_variance       as attrib_amount

from {{ ref('stg_ref_cra_legacy_ref') }}
where legacy_revenue_variance <> 0
)
	
, legacy_total_payment_amount as (
select 

    intacct_id                      as sage_id
    ,null                           as hs_credit_id
    ,quarter
    ,erc_number
    ,null                           as traunch
    ,man_cra_explanation_of_diff    as reason_for_variance
    ,legacy_revenue_variance_date   as trx_date
    ,'total_payment_amount'         as attrib_name
    ,null                           as attrib_date
    ,legacy_revenue_variance        as attrib_amount

from {{ ref('stg_ref_cra_legacy_ref') }}
where legacy_revenue_variance <> 0
)

, legacy_total_payment_date as (
select

    intacct_id                      as sage_id
    ,null                           as hs_credit_id
    ,quarter
    ,erc_number
    ,null                           as traunch
    ,man_cra_explanation_of_diff    as reason_for_variance
    ,legacy_revenue_variance_date   as trx_date
    ,'total_payment_date'           as attrib_name
    ,legacy_revenue_variance_date   as attrib_date
    ,0                              as attrib_amount

from {{ ref('stg_ref_cra_legacy_ref') }}
where legacy_revenue_variance <> 0
)

, legacy_credit_memo_amount as (
select 

    customerid                      as sage_id
    ,null                           as hs_credit_id
    ,classid                        as quarter
    ,entrydescription               as erc_number
    ,null                           as traunch
    ,null                           as reason_for_variance
    ,entry_date                     as trx_date
    ,'credit_memo_amount'           as attrib_name
    ,null                           as attrib_date
    ,-amount                        as attrib_amount

from {{ ref('stg_sage_gl_detail') }}
where batch_title like ('%Adjustments%') and classid is not null and accountno = 4000
)

, legacy_invoiced_ar as (
select 

    customerid                      as sage_id
    ,null                           as hs_credit_id
    ,classid                        as quarter
    ,entrydescription               as erc_number
    ,null                           as traunch
    ,null                           as reason_for_variance
    ,entry_date                     as trx_date
    ,'invoiced_ar'                  as attrib_name
    ,null                           as attrib_date
    ,-amount                        as attrib_amount

from {{ ref('stg_sage_gl_detail') }}
where batch_title like ('%Adjustments%') and classid is not null and accountno = 4000
)

select * from invoice_signed_date
union
select * from refund_received_date
union
select * from expected_recovery
union
select * from actual_recovery
union
select * from sage_invoiced_recovery
union
select * from estimated_ir_revenue
union
select * from ir_fee_invoiced
union
select * from ir_fee_recalc
union
select * from contract_ar
union
select * from credit_memo_amount
union
select * from revenue_write_off_amount
union
select * from bad_debt_write_off_amount
union
select * from total_payment_amount
union
select * from total_payment_date
union
select * from total_payment_amount_gl
union
select * from legacy_invoiced_recovery
union
select * from legacy_revenue_variance
union
select * from legacy_bad_debt_write_off_amount
union
select * from legacy_total_payment_amount
union
select * from legacy_total_payment_date
union
select * from legacy_credit_memo_amount
union
select * from legacy_invoiced_ar

