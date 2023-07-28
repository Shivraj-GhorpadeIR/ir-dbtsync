select 

    g.customerid                    as sage_id,
    g.docnumber                     as erc_number,
    c.hs_deal_id,
    x.hs_credit_id,
    c.name                          as client_name,
    d.mailaddress_address_1         as address_line1,
    d.mailaddress_address_2         as address_line2,
    d.mailaddress_city              as address_city,
    d.mailaddress_state             as address_state,
    d.mailaddress_zip               as address_zip,
    c.deal_owner,
    g.classid                       as quarter,
    x.quarter_clean,
    c.offer                         as traunch,
    c.custtype                      as cust_type,
    c.tax_preparer,
    g.recordtype                    as record_type,
    g.symbol                        as journal,
    g.entry_date,
    left(entry_date,7)              as entry_month,

  	case 
        when left(recordid,3) = 'Bad' 
            then 'Bad Debt Recovery' 
        when left(recordid,3) = 'ETF' 
            then 'Estimate to Final' 
        when left(recordid,2) = 'SF' 
            then 'Sales Final' 
  		when left(recordid,2) = 'WO' 
            then 'Write Off' 
        when left(recordid,2) = 'CM' 
            then 'Credit Memo' 
        when left(recordid,1) = 'E' 
            then 'Estimate' 
        else 'Other' 
    end                             as entry_type,

  	case 
        when g.accountno = 1200 and g.modulekey = '8.SO' 
            then 'Estimated Refund' 
  		when g.accountno = 4001 and g.modulekey = '8.SO' 
            then 'Actual Refund' 
  		else 'Account Receivable' 
    end                             as account_category,
  	
    case 
        when g.accountno = 4003 and left(batch_title,strpos(batch_title,':')-1) = 'Credit Memo' 
            then 'Credit Memo'
  		when g.accountno = 4000 and left(batch_title,strpos(batch_title,':')-1) = 'Adjustments' 
            then 'Credit Memo'
	  	when g.accountno = 4003 and left(batch_title,strpos(batch_title,':')-1) in ('Write Off','Write Off Rev') 
            then 'Revenue Write Off'
	  	when g.accountno = 6910 and left(batch_title,strpos(batch_title,':')-1) in ('Write Off','Write Off Rev') 
            then 'Bad Debt Write Off'
	  	when g.accountno in (4000,4020) 
            then 'Revenue'
  		when g.accountno = 1210 
            then 'Contract AR'
  		when g.accountno = 1200 
            then 'Invoiced AR'
  		--when g.accountno = 4020 then 'Revenue Variance'
  		when g.accountno between 1000 and 1099 
            then 'Cash Received' 
  		when g.accountno in (6910,6915,4003,4005) 
            then 'Bad Debt'
  		when g.accountno = 2205 
            then 'Raistone Clearing'
  		when g.accountno = 4001 and g.modulekey = '8.SO' 
            then 'Actual Refund'
  		else 'Other' 
    end                                 as account_type,
 	
    case 
        when strpos(batch_title,':') = 0 and strpos(batch_title,'(') = 0 
            then batch_title
 		when (strpos(batch_title,':') < strpos(batch_title,'(') and strpos(batch_title,':') > 0) or (strpos(batch_title,':') > 0 and strpos(batch_title,'(') = 0) 
            then left(batch_title,strpos(batch_title,':')-1)
 		when (strpos(batch_title,':') > strpos(batch_title,'(') and strpos(batch_title,'(') > 0) or (strpos(batch_title,':') = 0 and strpos(batch_title,'(') > 0) 
            then left(batch_title,strpos(batch_title,'(')-1)
   	    else batch_title
    end                                 as batch_type,

  	g.accountno                         as acct_no,
  	g.accounttitle                      as acct_title,
  	g.line_no,
  	g.modulekey                         as module_key,
  	g.amount,
  	cast('No' as varchar(3))            as is_reversal

from {{ ref('stg_sage_gl_detail') }} g 
left join {{ ref('stg_sage_customer') }} c 
  on g.customerid = c.customerid 
left join {{ ref('stg_sage_contact') }} d 
  on c.displaycontactkey = d.recordno 
left join {{ ref('erc_ref') }} x 
  on g.customerid = x.sage_id 
  and g.classid = x.sage_gl_quarter_raw
where (g.modulekey = '4.AR' or g.accountno = 1200 or g.accountno = 4001)