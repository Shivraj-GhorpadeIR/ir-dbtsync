select *
from {{ source('custom_sage', 'sodocument_custom_fields') }}