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