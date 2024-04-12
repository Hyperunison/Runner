-- DEATH
SELECT
    public.participant.vital_status as "c.cause",
    public.participant.id as participant_id
FROM public.participant
WHERE
true
LIMIT 100

-- OBSERVATION
-- SPECIMEN


SELECT
    c_6618f51c0342c.preservation_mode as "c.name",
    c_6618f51c0342c.material_type as "c.type",
    c_6618f51c0342c.date::date as "c.date",
    public.participant.id as participant_id
FROM public.participant
JOIN sample as c_6618f51c0342c ON public.participant.id=c_6618f51c0342c.person_id
WHERE
true

LIMIT 100