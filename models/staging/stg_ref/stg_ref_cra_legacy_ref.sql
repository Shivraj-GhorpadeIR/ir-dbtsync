select 

    _file
    ,_line
    ,_modified
    ,intacct_id
    ,hubspot_deal_id
    ,erc_number
    ,quarter
    ,dec_5050_amt
    ,intacct_cra_ir_fee
    ,man_cra_ir_fee
    ,ir_fee_var
    ,status
    ,man_cra_invoiced_recovery
    ,man_cra_explanation_of_diff
    ,cast(man_cra_mon_yr_of_collection as date)     as man_cra_mon_yr_of_collection
    ,legacy_revenue_variance
    ,cast(legacy_revenue_variance_date as date)     as legacy_revenue_variance_date
    ,_fivetran_synced

from {{ source('stg_ref', 'cra_legacy_ref') }}