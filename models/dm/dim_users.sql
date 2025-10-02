select distinct
  user_id,
  email,
  date_of_registration,
  latest_activity_at,
  currency,
  verification_status,
  brand
from {{ ref('users') }}
