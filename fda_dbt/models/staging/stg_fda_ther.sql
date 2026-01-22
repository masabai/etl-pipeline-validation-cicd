select *
from {{ source('fda', 'ther') }}