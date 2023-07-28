select
    *,
    -- Provided mapping files for these stages has no steps for these 2 stages, leaving them here to be updated later
    null as date_entered_crm_stage_unqualified,
    null as date_entered_crm_stage_confirmed_lead,
    date_entered_crm_step_getting_started as date_entered_crm_stage_eligible,
    coalesce(
        date_entered_crm_step_user_profile,
        date_entered_crm_step_company_info_general,
        date_entered_crm_step_company_info_affiliated,
        date_entered_crm_step_company_info_peo,
        date_entered_crm_step_company_info_banking,
        date_entered_crm_step_company_info_employee_general,
        date_entered_crm_step_company_info_employee_owners,
        date_entered_crm_step_company_info_employee_family,
        date_entered_crm_step_covid_disruptions,
        date_entered_crm_step_covid_disruptions_additional,
        date_entered_crm_step_covid_disruptions_description,
        date_entered_crm_step_ppp_general,
        date_entered_crm_step_ppp_upload,
        date_entered_crm_step_payroll_wages,
        date_entered_crm_step_benefits_general,
        date_entered_crm_step_payroll_tax_returns_general,
        date_entered_crm_step_payroll_tax_returns_941,
        date_entered_crm_step_payroll_tax_returns_943,
        date_entered_crm_step_profit_and_loss,
        date_entered_crm_step_review
    ) as date_entered_crm_stage_processing,
    date_entered_crm_step_tax_attorney as date_entered_crm_stage_contracting,
    coalesce(
        date_entered_crm_step_completed, date_entered_crm_step_awaiting_refund
    ) as date_entered_crm_stage_closed_won,
    date_entered_crm_step_closed_lost as date_entered_crm_stage_closed_lost
from {{ ref("prep_rds_application_prod__historical_deal__flattened_steps") }}
