with erc_trx_credit_stg as (
select

    a.sage_id
	,x.hs_deal_id
	,a.quarter	
	,c.name                                                                              as client_name
	,d.mailaddress_address_1                                                             as address_line1
	,d.mailaddress_address_2                                                             as address_line2
	,d.mailaddress_city                                                                  as address_city
	,d.mailaddress_state                                                                 as address_state
	,d.mailaddress_zip                                                                   as address_zip
	,c.deal_owner
	,c.tax_preparer
    ,a.trx_date
	,case when b.sage_id is null then 'No' else 'Yes' end                                as payment_5050
 	,case when c.custtype = 'Raistone' then 'Yes'  else 'No' end                         as raistone
 	,case when c.custtype = 'Advance' then 'Yes' else 'No' end                           as erca
	,cast(min(isnull(cast(a.hs_credit_id as bigint),x.hs_credit_id)) as varchar)         as hs_credit_id
	,cast(max(isnull(cast(a.hs_credit_id as bigint),x.hs_credit_id)) as varchar)         as hs_credit_id_alt
	,max(a.reason_for_variance)                                                          as reason_for_variance
	,max(a.traunch)                                                                      as traunch
	,max(a.erc_number)                                                                   as erc_number
	,min(case when attrib_name = 'invoice_signed_date' then attrib_date end)             as invoice_signed_date
--	,min(case when attrib_name = 'refund_received_date' then attrib_date end)            as min_refund_received_date
	,max(case when attrib_name = 'refund_received_date' then attrib_date end)            as max_refund_received_date
--	,min(case when attrib_name = 'total_payment_date' then attrib_date end)              as min_payment_received_date  -- SHOULD include CM/WO?
	,max(case when attrib_name = 'total_payment_date' then attrib_date end)              as max_payment_received_date  -- SHOULD include CM/WO?
	,sum(case when attrib_name = 'expected_recovery' then attrib_amount else 0 end)      as expected_recovery
	,sum(case when attrib_name = 'actual_recovery' then attrib_amount else 0 end)        as actual_recovery
	,sum(case when attrib_name = 'estimated_ir_revenue' then attrib_amount else 0 end)   as estimated_ir_revenue
	,sum(case when attrib_name = 'ir_fee_invoiced' then attrib_amount else 0 end)        as ir_fee_invoiced
	,sum(case when attrib_name = 'ir_fee_recalc' then attrib_amount else 0 end)          as ir_fee_recalc	
	,sum(case when attrib_name = 'ir_fee_recalc' then attrib_amount else 0 end - 
        case when attrib_name = 'ir_fee_invoiced' then attrib_amount else 0 end)         as excess_ir_revenue

	,sum(case when attrib_name in ('estimated_ir_revenue','ir_fee_recalc','legacy_revenue_variance') then attrib_amount else 0 end 
		- case when attrib_name = 'ir_fee_invoiced' then attrib_amount else 0 end)           as updated_actual_revenue

	,sum(case when attrib_name = 'legacy_revenue_variance' then attrib_amount else 0 end)    as legacy_revenue_variance

	,sum(case when attrib_name in ('estimated_ir_revenue','ir_fee_recalc','legacy_revenue_variance') then attrib_amount else 0 end 
		- case when attrib_name in ('ir_fee_invoiced','contract_ar','total_payment_amount') then attrib_amount else 0 end)      as invoiced_ar

	,sum(case when attrib_name in ('estimated_ir_revenue','ir_fee_recalc','legacy_revenue_variance','credit_memo_amount','revenue_write_off_amount','bad_debt_write_off_amount') 
		then attrib_amount else 0 end 
        - case when attrib_name in ('ir_fee_invoiced','contract_ar','total_payment_amount_gl') then attrib_amount else 0 end)   as invoiced_ar_gl

	,sum(case when attrib_name = 'contract_ar' then attrib_amount else 0 end)    as contract_ar

	,sum(case when attrib_name in ('total_payment_amount','credit_memo_amount','revenue_write_off_amount','bad_debt_write_off_amount') then attrib_amount else 0 end) as total_payment_amount
	,sum(case when attrib_name = 'total_payment_amount_gl' then attrib_amount else 0 end)            as total_payment_amount_gl
	,sum(case when attrib_name = 'credit_memo_amount' then attrib_amount else 0 end)                 as credit_memo_amount
	,sum(case when attrib_name = 'revenue_write_off_amount' then attrib_amount else 0 end)           as revenue_write_off_amount
	,sum(case when attrib_name = 'bad_debt_write_off_amount' then attrib_amount else 0 end)          as bad_debt_write_off_amount

	,sum(case when attrib_name = 'revenue_write_off_amount' then attrib_amount else 0 end)
		+ sum(case when attrib_name = 'bad_debt_write_off_amount' then attrib_amount else 0 end)     as total_write_off_amount

	,sum(case when attrib_name in ('estimated_ir_revenue','ir_fee_recalc','legacy_revenue_variance','credit_memo_amount','revenue_write_off_amount') then attrib_amount else 0 end 
		- case when attrib_name = 'ir_fee_invoiced' then attrib_amount else 0 end)                   as revenue_including_wo

	,sum(case when attrib_name = 'sage_invoiced_recovery' then attrib_amount else 0 end)             as sage_invoiced_recovery
	,sum(case when attrib_name = 'legacy_invoiced_recovery' then attrib_amount else 0 end)           as legacy_invoiced_recovery
	,cast(0 as numeric(38,2))                                                                        as invoiced_recovery
	,cast(0 as numeric(38,2))                                                                        as recovery_variance

from {{ ref('prep_sage_erc_trx_detail') }} a
left join {{ ref('stg_ref_cra_5050_ref') }} b 
    on a.sage_id = b.sage_id 
    and a.quarter = b.sage_gl_quarter_raw
left join {{ ref('erc_ref') }} x 
    on a.sage_id = x.sage_id 
    and a.quarter = x.sage_gl_quarter_raw
left join {{ ref('stg_sage_customer') }} c 
    on a.sage_id = c.customerid 
left join {{ ref('stg_sage_contact') }}  d 
    on c.displaycontactkey = d.recordno 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)

select

    a.sage_id

	,case
        when a.hs_deal_id is null and a.sage_id = b.sage_id
            then b.hs_deal_id
        else a.hs_deal_id
    end                                         as hs_deal_id

	,a.quarter	
	,a.client_name
	,a.address_line1
	,a.address_line2
	,a.address_city
	,a.address_state
	,a.address_zip
	,a.deal_owner
	,a.tax_preparer
    ,a.trx_date
	,a.payment_5050
 	,a.raistone
 	,a.erca
	,a.hs_credit_id

	,case
        when a.hs_credit_id = a.hs_credit_id_alt
            then null
        else a.hs_credit_id_alt
    end                                         as hs_credit_id_alt

	,a.reason_for_variance
	,a.traunch
	,a.erc_number
	,a.invoice_signed_date
--	,a.min_refund_received_date
	,a.max_refund_received_date

--	,case when a.total_payment_amount = 0 then null else a.min_payment_received_date end    as min_payment_received_date

	,case
        when a.total_payment_amount = 0
            then null
        else a.max_payment_received_date
    end                                         as max_payment_received_date

	,a.expected_recovery
	,a.actual_recovery
	,a.estimated_ir_revenue
	,a.ir_fee_invoiced
	,a.ir_fee_recalc	
	,a.excess_ir_revenue
    ,a.updated_actual_revenue
    ,a.legacy_revenue_variance
    ,a.invoiced_ar
    ,a.invoiced_ar_gl
    ,a.contract_ar
    ,a.total_payment_amount
	,a.total_payment_amount_gl
	,a.credit_memo_amount
	,a.revenue_write_off_amount
	,a.bad_debt_write_off_amount
    ,a.total_write_off_amount
    ,a.revenue_including_wo
    ,a.sage_invoiced_recovery
    ,a.legacy_invoiced_recovery
    
    ,case 
        when a.sage_invoiced_recovery > a.legacy_invoiced_recovery 
            then a.sage_invoiced_recovery 
        else a.legacy_invoiced_recovery 
    end                                                             as invoiced_recovery

    ,a.recovery_variance

from erc_trx_credit_stg a
left join (select distinct 
                sage_id
                , hs_deal_id 
          from {{ ref('erc_ref') }} 
          where hs_deal_id is not null) b
    on a.sage_id = b.sage_id
    and a.hs_deal_id is null
where trx_date is not null
