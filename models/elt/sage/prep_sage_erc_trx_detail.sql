with erc_trx as (
select 
    sage_id
    ,hs_credit_id
    ,quarter
    ,erc_number
    ,traunch
    ,reason_for_variance
    ,trx_date
    ,attrib_name
    ,attrib_date
    ,attrib_amount
from {{ ref("prep_sage_erc_trx") }}
)

, gl_detail as (
select 
    customerid
    ,classid
    ,docnumber
    ,accountno
    ,entry_date
    ,amount
from {{ ref('stg_sage_gl_detail') }}   
where modulekey = '4.AR' and accountno in (4003,6910) and (batch_title like ('%Write Off%') or batch_title like ('%Write Off Rev%'))
)

, erc_trx_detail_stg as (
select 
    sage_id
    ,case
        when hs_credit_id = '5791727118'
            then '5791542140'
        when sage_id = 'C1003092' and hs_credit_id = '1032023'   
            then '2676530824'
        else hs_credit_id
    end                                                         as hs_credit_id
    ,case 
        when sage_id = 'C1000339' and quarter = '2020-Q3'
            then '2021-943'
        when sage_id = 'C1000339' and quarter = '2020-Q4'
            then '2020-943'
        when sage_id = 'C1000537' and quarter = '2020-Q4'
            then '2020-943'
        when sage_id = 'C1000537' and quarter = '2021-Q1'
            then '2021-943'
        when sage_id = 'C1003125' and quarter = '2020-Q4'
            then '2020-943'
        when sage_id = 'C1003125' and quarter = '2021-Q4'
            then '2021-943'
        when left(quarter,3) = '943'
            then right(quarter,4) + '-943'
        else quarter
    end                                                         as quarter
    ,erc_number
    ,traunch
    ,reason_for_variance
    ,trx_date
    ,attrib_name
    ,attrib_date
    ,attrib_amount

from erc_trx
)

, attrib_name_fix as (

select

    sage_id
    ,hs_credit_id
    ,quarter
    ,erc_number
    ,traunch
    ,reason_for_variance
    ,trx_date

    ,case 
        when b.accountno = 4003 and (a.sage_id = b.customerid 
                                    and a.quarter = b.classid 
                                    and a.erc_number = b.docnumber 
                                    and a.trx_date = b.entry_date 
                                    and a.attrib_amount = -b.amount)
            then 'revenue_write_off_amount' 
        when b.accountno = 6910 and (a.sage_id = b.customerid 
                                    and a.quarter = b.classid 
                                    and a.erc_number = b.docnumber 
                                    and a.trx_date = b.entry_date 
                                    and a.attrib_amount = -b.amount)
            then 'bad_debt_write_off_amount'
        else a.attrib_name
    end                                                                 as attrib_name

    ,attrib_date
    ,attrib_amount

from erc_trx_detail_stg a
left join gl_detail b
    on a.sage_id = b.customerid 
    and a.quarter = b.classid 
    and a.erc_number = b.docnumber 
    and a.trx_date = b.entry_date 
    and a.attrib_amount = -b.amount
    and a.attrib_name in('bad_debt_write_off_amount','revenue_write_off_amount')
)

, miss_erc_num_fix as (
select 
    a.sage_id
    ,a.hs_credit_id
    ,a.quarter

    ,case
        when (a.erc_number is null or len(a.erc_number) = 0) and b.erc_number is not null
            then b.erc_number
        else a.erc_number
    end                                 as erc_number

    ,a.traunch
    ,a.reason_for_variance
    ,a.trx_date
    ,a.attrib_name
    ,a.attrib_date
    ,a.attrib_amount

from attrib_name_fix a
left join {{ ref('erc_ref') }} b
    on a.sage_id = b.sage_id 
    and a.quarter = b.sage_quarter_clean 
    and a.erc_number is null
)

, miss_credit_id_fix_1 as (
select 
    a.sage_id
    ,case
        when a.hs_credit_id is null and cast(b.hs_credit_id as varchar) is not null
            then cast(b.hs_credit_id as varchar)
        else a.hs_credit_id
    end                             as hs_credit_id
    ,a.quarter
    ,a.erc_number
    ,a.traunch
    ,a.reason_for_variance
    ,a.trx_date
    ,a.attrib_name
    ,a.attrib_date
    ,a.attrib_amount

from miss_erc_num_fix a
left join {{ ref('erc_ref') }} b
    on a.sage_id = b.sage_id 
    and a.quarter = b.sage_quarter_clean 
    and a.erc_number = b.erc_number 
    and a.erc_number is not null 
    and a.hs_credit_id is null
)


select distinct
    a.sage_id
    ,case
        when a.hs_credit_id is null and cast(b.hs_credit_id as varchar) is not null
            then cast(b.hs_credit_id as varchar)
        else a.hs_credit_id
    end                             as hs_credit_id
    ,a.quarter
    ,a.erc_number
    ,a.traunch
    ,a.reason_for_variance
    ,a.trx_date
    ,a.attrib_name
    ,a.attrib_date
    ,a.attrib_amount

from miss_credit_id_fix_1 a
left join {{ ref('erc_ref') }} b
    on a.sage_id = b.sage_id 
    and a.quarter = b.sage_quarter_clean 
    and a.erc_number = b.erc_number_alt 
    and a.erc_number is not null 
    and a.hs_credit_id is null

