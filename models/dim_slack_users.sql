select 1 as user_id, '2021-01-01 14:18:27' as joined_at, true as is_active_past_quarter, true as has_messaged
union all 
select 2 as user_id, '2021-02-03 17:18:55' as joined_at, false as is_active_past_quarter, true as has_messaged
union all 
select 3 as user_id, '2021-04-01 11:01:28' as joined_at, false as is_active_past_quarter, false as has_messaged