select * from {{ source('greenhouse','email_cc') }}
