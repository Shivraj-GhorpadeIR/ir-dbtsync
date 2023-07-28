select
    
    _file
    ,_line
    ,_modified
    ,cast(datekey as date)      as datekey
    ,cast(dateiso as date)      as dateiso
    ,cast(yyyymm as varchar)    as yyyymm
    ,yyyy_mm
    ,year
    ,month
    ,month_2_digit
    ,monthliteral
    ,monthabbrv
    ,day
    ,day_2_digit
    ,dow
    ,dowliteral
    ,dowabbrv
    ,isholiday
    ,week
    ,weekiso
    ,isweekday
    ,isweekend
    ,quarter
    ,qq
    ,yyyy_qq
    ,cast(monthbegin as date)   as monthbegin
    ,cast(monthend as date)     as monthend
    ,ismonthend
    ,daysremain
    ,daysremaintozero
    ,cast(yesterday as date)    as yesterday
    ,cast(tomorrow as date)     as tomorrow
    ,cast(sdlm as date)         as sdlm
    ,cast(stlm as date)         as stlm
    ,cast(sdly as date)         as sdly
    ,cast(stly as date)         as stly
    ,cast(sdnm as date)         as sdnm
    ,cast(stnm as date)         as stnm
    ,cast(sdny as date)         as sdny
    ,cast(stny as date)         as stny
    ,issbaholiday
    ,yyyy_ww
    ,cast(weekending as date)   as weekending
    ,_fivetran_synced

from {{ source('stg_ref', 'date_ref') }}