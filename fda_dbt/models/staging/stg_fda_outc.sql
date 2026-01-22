select *
from {{ source('fda', 'outc') }}