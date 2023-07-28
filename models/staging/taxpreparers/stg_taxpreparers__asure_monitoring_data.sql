with 

source as (

    select * from {{ source('taxpreparers','asure_monitoring_data') }}

),

renamed as (

    select
        fein,
        fein_2,
        business_type,
        company_name,
        tax_period,
        days_to_processing,
        TO_DATE(_971_amended_tax_return_or_claim_forwarded_for_processing_date_, 'MM/DD/YY') as _971_amended_tax_return_or_claim_forwarded_for_processing_date_,
        translate(_971_amended_tax_return_or_claim_forwarded_for_processing_, '$,', '')::DECIMAL(19,2) as _971_amended_tax_return_or_claim_forwarded_for_processing_,
        TO_DATE(_291_reduced_or_removed_prior_tax_assessed_date_, 'MM/DD/YY') as _291_reduced_or_removed_prior_tax_assessed_date_,
        translate(_291_reduced_or_removed_prior_tax_assessed_, '$,()', '')::DECIMAL(19,2) as _291_reduced_or_removed_prior_tax_assessed_,
        _766_credit_type,
        TO_DATE(_766_credit_date, 'MM/DD/YY') as _766_credit_date,
        translate(_766_credit_amount, '$,()', '')::DECIMAL(19,2) as _766_credit_amount,
        TO_DATE(_776_interest_credited_to_your_account_date_, 'MM/DD/YY') as _776_interest_credited_to_your_account_date_,
        translate(_776_interest_credited_to_your_account_, '$,()', '')::DECIMAL(19,2) as _776_interest_credited_to_your_account_,
        TO_DATE(_846_refund_issued_date_, 'MM/DD/YY') as _846_refund_issued_date_,
        translate(_846_refund_amount_, '$,()', '')::DECIMAL(19,2) as _846_refund_amount_
    from source

)

select * from renamed