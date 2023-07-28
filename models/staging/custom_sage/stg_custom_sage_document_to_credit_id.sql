select *
from {{ source('custom_sage', 'document_to_credit_id_temp') }}
