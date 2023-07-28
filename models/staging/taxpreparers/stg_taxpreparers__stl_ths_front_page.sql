with 

source as (

    select * from {{ source('taxpreparers', 'slt_ths_front_page') }}

),

renamed as (

    select
        type,
        customer_type,
        assigned_tax_pro,
        active,
        intelli_order,
        ssn_ein,
        account_name,
        last_business_name,
        spouse_total_balance,
        first_name,
        spouse_ssn,
        entity_balance,
        caf_passed,
        current_yr_caf_passed,
        in_compliance,
        fta,
        open_exams,
        est_pmt_amt_prior_yr,
        _payments_prior_yr,
        in_collections,
        lien_active,
        TO_DATE(last_transcript_date, 'MM/DD/YY') as last_transcript_date,
        TO_DATE(report_date, 'MM/DD/YY') as report_date,
        TO_DATE(last_change_date, 'MM/DD/YY') as last_change_date,
        civ_pen_bal,
        payroll_bal,
        unemployment_bal,
        cell_phone,
        email,
        address,
        city,
        state,
        zip,
        id_theft,
        customer_code,
        TO_TIMESTAMP(created_date, 'MM/DD/YY HH24:MI') as created_date,
        _2021_income_check_status,
        _72_mth_payment,
        _84_mth_payment,
        csed_payment
    from source

)

select * from renamed