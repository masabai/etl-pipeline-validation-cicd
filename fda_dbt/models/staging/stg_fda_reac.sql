select *
from {{ source('fda', 'reac') }}