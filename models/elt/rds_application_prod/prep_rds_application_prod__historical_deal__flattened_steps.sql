select *
from
    (
        select
            id as milkshake_deal_id,
            estimate_id,
            concat('date_entered_crm_step_', replace(active_step, '-', '_'))::varchar as active_step_pivot,
            history_date
        from {{ ref("stg_rds_application_prod_application_historicaldeal") }}
    ) pivot (
        min(history_date)
        for active_step_pivot in (
            'date_entered_crm_step_company_info_general',
            'date_entered_crm_step_company_info_ownership',
            'date_entered_crm_step_covid_disruptions_additional',
            'date_entered_crm_step_covid_quarters_impact',
            'date_entered_crm_step_payroll_wages',
            'date_entered_crm_step_company_info_peo',
            'date_entered_crm_step_benefits_general',
            'date_entered_crm_step_ppp_upload',
            'date_entered_crm_step_company_info_banking',
            'date_entered_crm_step_payroll_tax_returns_943',
            'date_entered_crm_step_user_profile',
            'date_entered_crm_step_tax_attorney',
            'date_entered_crm_step_awaiting_refund',
            'date_entered_crm_step_getting_started',
            'date_entered_crm_step_closed_lost',
            'date_entered_crm_step_payroll_tax_returns_general',
            'date_entered_crm_step_review',
            'date_entered_crm_step_completed',
            'date_entered_crm_step_ppp_general',
            'date_entered_crm_step_company_info_employee_family',
            'date_entered_crm_step_company_info_employee_owners',
            'date_entered_crm_step_covid_disruptions_description',
            'date_entered_crm_step_profit_and_loss',
            'date_entered_crm_step_company_info_employee_general',
            'date_entered_crm_step_covid_disruptions',
            'date_entered_crm_step_payroll_tax_returns_941',
            'date_entered_crm_step_company_info_affiliated',
            'date_entered_crm_step_signature'
        )
    )
