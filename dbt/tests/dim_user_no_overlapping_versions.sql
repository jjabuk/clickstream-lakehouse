-- Two versions of the same user must never be valid at the same time, or the SCD2 join in the
-- fact would return two rows for one page view.
select
    a.user_id,
    a.effective_from as first_from,
    b.effective_from as second_from
from {{ ref('dim_user') }} as a
inner join {{ ref('dim_user') }} as b
    on
        a.user_id = b.user_id
        and a.user_key < b.user_key
        and a.effective_from < b.effective_to
        and b.effective_from < a.effective_to
