-- DEATH
SELECT
    public.participant.vital_status as "c.cause",
    public.participant.id as participant_id
FROM public.participant
WHERE
true
LIMIT 100

-- OBSERVATION

    WITH __sys_subquery_c AS (


SELECT
    'microsatellite_instability' as "name", null as "date", null as "start_date", null as "end_date", null as "type", null as "value_as_number", null as "value_as_string", null as "qualifier", null as "unit", null as "source", public.participant.microsatellite_instability as "value",
public.participant.id as participant_id
FROM public.participant
WHERE
true

UNION ALL


SELECT
    'response to therapy' as "name", o1_661904ed1e12a_661904ed20167.date as "date", null as "start_date", null as "end_date", null as "type", null as "value_as_number", null as "value_as_string", null as "qualifier", null as "unit", null as "source", o1_661904ed1e12a_661904ed20167.specific_response as "value",
public.participant.id as participant_id
FROM public.participant
JOIN response_to_therapy as o1_661904ed1e12a_661904ed20167 ON public.participant.id=o1_661904ed1e12a_661904ed20167.person_id
WHERE
true

UNION ALL


SELECT
    'availability invasion front digital imaging' as "name", o1_661904ed1e135_661904ed2134b.date as "date", null as "start_date", null as "end_date", null as "type", null as "value_as_number", null as "value_as_string", null as "qualifier", null as "unit", null as "source", o1_661904ed1e135_661904ed2134b.availability_invasion_front_digital_imaging as "value",
public.participant.id as participant_id
FROM public.participant
JOIN histopathology as o1_661904ed1e135_661904ed2134b ON public.participant.id=o1_661904ed1e135_661904ed2134b.person_id
WHERE
true

UNION ALL


SELECT
    'grade' as "name",
    null as "date",
    null as "start_date",
    null as "end_date",
    null as "type",
    null as "value_as_number",
    null as "value_as_string",
    null as "qualifier",
    null as "unit",
    null as "source",
    o1_661904ed1e13f_661904ed222b4.grade || '; UICC version: ' || uicc_version || '; WHO version=' || who_version as "value",
    public.participant.id as participant_id
FROM public.participant
JOIN histopathology as o1_661904ed1e13f_661904ed222b4
    ON public.participant.id=o1_661904ed1e13f_661904ed222b4.person_id
WHERE
true

UNION ALL


SELECT
    'mismatch_repair_gene_expression' as "name", null as "date", null as "start_date", null as "end_date", null as "type", null as "value_as_number", null as "value_as_string", null as "qualifier", null as "unit", null as "source", public.participant.mismatch_repair_gene_expression as "value",
public.participant.id as participant_id
FROM public.participant
WHERE
true
)


SELECT
    c_661904ed24371.name as "c.name", c_661904ed24371.date as "c.date", c_661904ed24371.value as "c.value",
public.participant.id as participant_id
FROM public.participant
JOIN __sys_subquery_c as c_661904ed24371 ON c_661904ed24371.participant_id = public.participant.id
WHERE
true

LIMIT 100

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