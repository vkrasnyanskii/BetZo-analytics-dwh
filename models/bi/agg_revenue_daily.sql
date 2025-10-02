with deps as (
  select toDate(timestamp) as d, sum(amount) as deposit_minor
  from {{ raw('deposits') }}
  group by d
),
wd as (
  select toDate(create_date) as d, sum(amount) as withdrawal_minor
  from {{ raw('withdrawals') }}
  group by d
)
select
  coalesce(deps.d, wd.d) as d,
  coalesce(deps.deposit_minor, 0)    as deposit_minor,
  coalesce(wd.withdrawal_minor, 0)   as withdrawal_minor
from deps
full join wd on deps.d = wd.d
