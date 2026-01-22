select *
from {{ source('fda', 'rpsr') }}