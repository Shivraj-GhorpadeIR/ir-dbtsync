with quarter_erc_ref as (
select *
from {{ ref('stg_ref_quarter_erc_ref') }}
)

, so_document as (
select 
    b.hs_credit_id
    ,a.*
from {{ ref('stg_sage_so_document') }} a
left join {{ ref('stg_custom_sage_sodocument_custom_fields') }} b
    on a.docid = b.docid
)

, so_document_entry as (
select *
from {{ ref('stg_sage_so_document_entry') }}
)

, customer as (
select *
from {{ ref('stg_sage_customer') }}
)

, credit as (
select *
from {{ ref('stg_hubspot_hp_credit') }}
)

, gl_detail as (
select *
from {{ ref('stg_sage_gl_detail') }}
)

, credit_to_deal as (
select *
from {{ ref('stg_hubspot_hp_credit_to_deal') }}
)

, erc_ref_stg as (
select 
    a.custvendid                    as sage_id,
	c.name                          as client_name,
	c.hs_deal_id                    as sage_deal_id,
	min(a.hs_credit_id)             as sage_credit_id,
	max(a.hs_credit_id)             as sage_credit_id_alt,
	min(a.ponumber)                 as erc_number,
	max(a.ponumber)                 as erc_number_alt,
	e.classid                       as sage_oe_quarter_raw,
	g.classid                       as sage_gl_quarter_raw,
	q.quarter_clean                 as sage_quarter_clean,
	b.name                          as hs_quarter_raw,
	q2.quarter_clean                as quarter_clean,
	cast(c.hs_deal_id as bigint)    as hs_deal_id,
	cast(null as bigint)            as hs_deal_id_alt,
	min(b.id)                       as hs_credit_id,
	max(b.id)                       as hs_credit_id_alt
from so_document a 
left join so_document_entry e 
    on a.docid = e.dochdrid 
left join customer c 
    on a.custvendid = c.customerid 
left join quarter_erc_ref q 
    on a.message = q.quarter_raw
left join credit b 
    on a.hs_credit_id = b.id
left join quarter_erc_ref q2 
    on b.name = q2.quarter_raw
left join (select distinct customerid, classid, quarter_clean 
            from gl_detail g 
			join quarter_erc_ref q 
                on g.classid = q.quarter_raw) g
    on a.custvendid = g.customerid 
    and q.quarter_clean = g.quarter_clean
where left(a.ponumber,3) = 'ERC' and a.message is not null and g.classid is not null-- and b.name is not null
group by 1,2,3,8,9,10,11,12
)

, alt_column_nulls as (
select 
    sage_id,
	client_name,
	sage_deal_id,
	sage_credit_id,

	case 
        when sage_credit_id = sage_credit_id_alt 
            then null 
        else sage_credit_id_alt 
    end                                                 as sage_credit_id_alt,

	erc_number,

	case
        when erc_number = erc_number_alt
            then null
        else erc_number_alt
    end                                                 as erc_number_alt,

	sage_oe_quarter_raw,
	
    case 
        when sage_gl_quarter_raw is null and right(sage_quarter_clean,3) = '943' 
            then '943-' + left(sage_quarter_clean,4)
        else sage_quarter_clean 
    end                                                 as sage_gl_quarter_raw,
	
    sage_quarter_clean,
	hs_quarter_raw,
	quarter_clean,
	hs_deal_id,
	hs_deal_id_alt,
	hs_credit_id,

	case 
        when hs_credit_id = hs_credit_id_alt 
            then null 
        else hs_credit_id_alt 
    end                                                 as hs_credit_id_alt

from erc_ref_stg
)

, merged_companies as (
select

    a.sage_id
	,a.client_name
	,a.sage_deal_id
	,a.sage_credit_id
    ,a.sage_credit_id_alt
	,a.erc_number
    ,a.erc_number_alt
	,a.sage_oe_quarter_raw
	,a.sage_gl_quarter_raw
	,a.sage_quarter_clean
	,a.hs_quarter_raw
	,a.quarter_clean
	,a.hs_deal_id
	
    ,case
        when b.from_id is not null and a.hs_deal_id <> b.to_id
            then b.to_id
        else a.hs_deal_id_alt
    end                                                         as hs_deal_id_alt
    
	,a.hs_credit_id
    ,a.hs_credit_id_alt

from erc_ref_stg a
Left join credit_to_deal b
    on a.hs_credit_id = b.from_id
)

, de_dupe_stg as (
select 
    a.sage_id + a.sage_oe_quarter_raw AS combined_id
from merged_companies a
    join (
        select 
            sage_id
            ,sage_oe_quarter_raw
        from merged_companies
        where sage_oe_quarter_raw <> '-1'
        group by sage_id, sage_oe_quarter_raw
        having count(*) > 1
    ) b 
        on a.sage_id = b.sage_id 
        and a.sage_oe_quarter_raw = b.sage_oe_quarter_raw
)

, de_dupe as (
select *
from merged_companies
where hs_credit_id is null and sage_id + sage_oe_quarter_raw in (select combined_id from de_dupe_stg)
)

, gl_de_dupe_recs as (
select 
    sage_id
    ,sage_gl_quarter_raw
from de_dupe
where sage_gl_quarter_raw <> '-1'
group by sage_id, sage_gl_quarter_raw
having count(*) > 1
)

, gl_flag_dupe_recs as (
select 

    a.sage_id
	,a.client_name
	,a.sage_deal_id

    ,case
        when isnull(a.sage_oe_quarter_raw, 'X') <> isnull(a.sage_gl_quarter_raw, 'X')
            then '-1'
        else a.sage_credit_id
    end                             as sage_credit_id

    ,a.sage_credit_id_alt
	,a.erc_number
    ,a.erc_number_alt
	,a.sage_oe_quarter_raw

	,case
        when isnull(a.sage_oe_quarter_raw, 'X') <> isnull(a.sage_gl_quarter_raw, 'X')
            then '-1'
        else a.sage_gl_quarter_raw
    end                             as sage_gl_quarter_raw

	,a.sage_quarter_clean
	,a.hs_quarter_raw
	,a.quarter_clean
	,a.hs_deal_id
    ,a.hs_deal_id_alt
	,a.hs_credit_id
    ,a.hs_credit_id_alt

from de_dupe a
left join gl_de_dupe_recs b 
    on a.sage_id = b.sage_id 
    and a.sage_gl_quarter_raw = b.sage_gl_quarter_raw
)

, oe_de_dupe_recs as (
select 
    sage_id
    ,sage_oe_quarter_raw
from gl_flag_dupe_recs
where sage_oe_quarter_raw <> '-1'
group by sage_id, sage_oe_quarter_raw
having count(*) > 1
)

, oe_flag_dupe_recs as (
select 

    a.sage_id
	,a.client_name
	,a.sage_deal_id

    ,case
        when a.sage_id = b.sage_id and a.sage_oe_quarter_raw = b.sage_oe_quarter_raw and isnull(a.sage_oe_quarter_raw, 'X') <> isnull(a.sage_gl_quarter_raw, 'X')
            then '-1'
        else a.sage_credit_id
    end                                         as sage_credit_id

    ,a.sage_credit_id_alt
	,a.erc_number
    ,a.erc_number_alt
	
    ,case
        when a.sage_id = b.sage_id and a.sage_oe_quarter_raw = b.sage_oe_quarter_raw and isnull(a.sage_oe_quarter_raw, 'X') <> isnull(a.sage_gl_quarter_raw, 'X')
            then '-1'
        else a.sage_oe_quarter_raw
    end                                         as sage_oe_quarter_raw

	,a.sage_gl_quarter_raw
	,a.sage_quarter_clean
	,a.hs_quarter_raw
	,a.quarter_clean
	,a.hs_deal_id
    ,a.hs_deal_id_alt
	,a.hs_credit_id
    ,a.hs_credit_id_alt

FROM gl_flag_dupe_recs a
left join oe_de_dupe_recs b 
    on a.sage_id = b.sage_id 
    and a.sage_oe_quarter_raw = b.sage_oe_quarter_raw
)

select 

    sage_id
	,client_name
	,sage_deal_id
    ,sage_credit_id
    ,sage_credit_id_alt
	,erc_number
    ,erc_number_alt
    ,sage_oe_quarter_raw
	,sage_gl_quarter_raw
	,sage_quarter_clean
	,hs_quarter_raw
	,quarter_clean
	,hs_deal_id
    ,hs_deal_id_alt
	,hs_credit_id
    ,hs_credit_id_alt

from oe_flag_dupe_recs