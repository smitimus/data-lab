-- For each employee on each day, clock_in and clock_out events must be equal in count,
-- and break_start and break_end events must be equal in count.
-- Unpaired events indicate missed punches in the timeclock data.
-- Returns one row per (employee, day) pair that has unmatched events.
-- Excludes today: morning-shift employees who clocked in but haven't yet clocked
-- out are legitimately open and should not trigger the test.

select
    employee_id,
    event_date,
    count(*) filter (where event_type = 'clock_in')     as clock_ins,
    count(*) filter (where event_type = 'clock_out')    as clock_outs,
    count(*) filter (where event_type = 'break_start')  as break_starts,
    count(*) filter (where event_type = 'break_end')    as break_ends
from {{ ref('stg_timeclock_events') }}
where event_date < current_date
group by employee_id, event_date
having
    count(*) filter (where event_type = 'clock_in')    !=
    count(*) filter (where event_type = 'clock_out')
    or
    count(*) filter (where event_type = 'break_start') !=
    count(*) filter (where event_type = 'break_end')
