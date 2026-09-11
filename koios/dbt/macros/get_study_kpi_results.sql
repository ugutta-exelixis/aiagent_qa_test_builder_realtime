{% macro get_study_kpi_results(study_id) %}

{% if study_id in ['XB628-101' ] %}
select study_id
,"visit description" as status
,'In Screening in Last 30 Days' as KPI_Name
,'Escalation' as study_phase
,count(distinct subject) as count
,last_modified_date
,current_date as load_date
from {{ref ('ctms_stg__subject_visit_summary_report')}}
where  (current_date  - cast("actual date" as date))<31
and "visit description" = 'Screening'
and study_id = '{{ study_id }}'
group by study_id, "visit description",last_modified_date
 
{% elif study_id in ['XB010-101'] %}
SELECT
    sr.study_id,
    sr."visit description" AS status,
    'In Screening in Last 30 Days' AS kpi_name,
    COALESCE(
        ss."study phase",
        ref."Study Phase"
    ) AS study_phase, 
    COUNT(DISTINCT sr.subject) AS count,
    sr.last_modified_date,
    CURRENT_DATE AS load_date
FROM {{ ref('ctms_stg__subject_visit_summary_report') }} sr
LEFT JOIN {{ ref('ctms_stg__subject_summary_report') }} ss
    ON sr.study_id = ss.study_id
    AND sr.subject = ss.subject 
LEFT JOIN koios_raw."xb010-101_study_phase_ref" ref
    ON sr.subject = ref.subject
WHERE (CURRENT_DATE - CAST(sr."actual date" AS DATE)) < 31
  AND sr."visit description" IN ('Screening', 'In Screening')
  AND sr.study_id = '{{ study_id }}' 
GROUP BY
    sr.study_id,
    sr."visit description",
    COALESCE(ss."study phase", ref."Study Phase"),
    sr.last_modified_date

{% else %}
select sr.study_id
,sr."visit description" as  status
,'In Screening in Last 30 Days' as KPI_Name
,"study phase" as study_phase
,count(distinct sr.subject) as count
,last_modified_date
,current_date as load_date
from {{ref ('ctms_stg__subject_visit_summary_report')}} sr left join {{ref('ctms_stg__subject_summary_report')}} ss
on sr.study_id = ss.study_id 
where (current_date  - cast("actual date" as date))<31
and sr."visit description" in ('Screening','In Screening')  
and sr.study_id = '{{ study_id }}'
group by sr.study_id, "visit description",last_modified_date,"study phase"

{% endif %}
union all


{% if study_id in ['XL092-305','XB010-101','XL495-101', 'XL309-101','XL092-201'] %}
select study_id, 
'All' as  status,
'Total Screened' as KPI_Name,
"study phase" as Study_phase,
count(distinct subject),
irt_data_refreshed_date as last_modified_date
,current_date as load_date
from {{ref('ctms_stg__subject_summary_report')}}  
where study_id ='{{study_id}}' and status!='In-Active'
group by study_id,"study phase", irt_data_refreshed_date

{% elif study_id in ['XB628-101'] %}
select study_id, 
'All' as  status,
'Total Screened' as KPI_Name,
'Escalation' as Study_phase,
count(distinct subject),
irt_data_refreshed_date as last_modified_date
,current_date as load_date
from {{ref('ctms_stg__subject_summary_report')}}  
where study_id ='{{study_id}}' and status!='In-Active'
group by study_id, irt_data_refreshed_date


{% elif study_id in ['XL092-002'] %}
SELECT
    study_id,
    'All' AS status,
    'Total Screened' AS KPI_Name,
    study_phase_category AS study_phase,
    COUNT(DISTINCT RIGHT(subject, 4)) AS count_of_participants,
    irt_data_refreshed_date as last_modified_date
	,current_date as load_date
FROM (
    SELECT
        study_id,subject,status,"tumor type","study phase",irt_data_refreshed_date,
        CASE
             WHEN status = 'Screen Failure' AND "tumor type" = 'Solid Tumor' THEN 'Escalation'
            WHEN status = 'In Screening' AND "tumor type" = 'Solid Tumor' THEN 'Escalation'
            WHEN status IN ('In Screening', 'Screening') AND "tumor type" <> 'Solid Tumor' THEN 'Expansion'
            WHEN status LIKE 'Screen Failure%' AND "tumor type" <> 'Solid Tumor' THEN 'Expansion'
            ELSE "study phase"
        END AS study_phase_category
    FROM {{ref('ctms_stg__subject_summary_report')}}
    WHERE
        study_id = '{{study_id}}'
        AND status IS NOT NULL
        AND RIGHT(subject, 4) IS NOT NULL
        AND RIGHT(subject, 4) NOT IN ('-', '--', '`---', 'VOID', 'OID.', '1262', 'Void', 'OID1', 'OID2', '0000')
) AS sub
GROUP BY study_id,study_phase_category,irt_data_refreshed_date


{% elif study_id in ['XB002-101'] %}
select studyid 
,'All' as  status
,'Total Screened' as KPI_Name
,"study stage" as Study_phase
,count("subject number") as count
,EDC_Data_Refreshed_Date as last_modified_date 
,current_date as load_date
from {{ref ('ctms_stg__subject_summary')}} 
where studyid ='{{study_id}}'
and "subject number" is not null  
and "subject number" not in ('-','--','---','VOID','OID.','OID1','OID2','0000')
group by  studyid ,"study stage",EDC_Data_Refreshed_Date

{% elif study_id in ['XL092-303','XL092-304','XL092-311'] %}
select study_id, 
'All' as  status,
'Total Screened' as KPI_Name,
"study phase" as Study_phase,
count(distinct subject),
irt_data_refreshed_date as last_modified_date
,current_date as load_date
from {{ref('ctms_stg__subject_summary_report')}} 
where study_id ='{{study_id}}' and status!='In-Active'
group by study_id,"study phase", irt_data_refreshed_date

{% else %}
select study_id, 
'All' as  status,
'Total Screened' as KPI_Name,
'Escalation' as Study_phase,
count(distinct subject),
irt_data_refreshed_date as last_modified_date
,current_date as load_date
from {{ref('ctms_stg__subject_summary_report')}} 
where study_id ='{{study_id}}' and status!='In-Active'
group by study_id,"study phase", irt_data_refreshed_date
{%endif%}

union all
{% if study_id in ['XB628-101']%}
select study_id
,"visit description" as  status
,'Enrolled in last 30 days' as KPI_Name
,'Escalation' as study_phase
,count(distinct subject) as count
,last_modified_date
,current_date as load_date
from {{ref ('ctms_stg__subject_visit_summary_report')}}
where  (current_date  - cast("actual date" as date))<31
and "visit description" = 'Randomization'
and study_id = '{{ study_id }}'
group by study_id, "visit description",last_modified_date

{% elif study_id in ['XB010-101'] %}
SELECT
    sr.study_id,
    sr."visit description" AS status,
   'Enrolled in last 30 days' AS kpi_name,
    COALESCE(
        ss."study phase",
        ref."Study Phase"
    ) AS study_phase,
    COUNT(DISTINCT sr.subject) AS count,
    sr.last_modified_date,
    CURRENT_DATE AS load_date  
FROM {{ref('ctms_stg__subject_visit_summary_report') }} sr 
LEFT JOIN {{ref('ctms_stg__subject_summary_report') }} ss
    ON sr.study_id = ss.study_id
    AND sr.subject = ss.subject 
LEFT JOIN koios_raw."xb010-101_study_phase_ref" ref
    ON sr.subject = ref.subject
WHERE (CURRENT_DATE - CAST(sr."actual date" AS DATE)) < 31
  AND sr."visit description" IN ('Randomization')
  AND sr.study_id = 'XB010-101'
GROUP BY
    sr.study_id,
    sr."visit description",
    COALESCE(ss."study phase", ref."Study Phase"),
    sr.last_modified_date
{%else%}
select sr.study_id
,"visit description" as  status
,'Enrolled in last 30 days' as KPI_Name
,"study phase" as Study_phase
,count(distinct sr.subject) as count
,last_modified_date
,current_date as load_date
from {{ref ('ctms_stg__subject_visit_summary_report')}} sr left join {{ref('ctms_stg__subject_summary_report')}} ss
on sr.study_id = ss.study_id 
where  (current_date  - cast("actual date" as date))<31
and "visit description" = 'Randomization'
and sr.study_id = '{{ study_id }}'
group by sr.study_id, "visit description",last_modified_date,"study phase"
{%endif%}
union all

{% if study_id in [ 'XB002-101','XL092-009','XL092-001'] %}
select studyid
,null as edc_status
,'Enrollment Progress Actual' as kpi_name
,coalesce(
        "study stage",
        case
            when studyid = 'XL092-009' then 'Escalation'
            else "study stage"
        end
    ) as "study stage"
,count(distinct "subject number") as count
,EDC_Data_Refreshed_Date as last_modified_date
,current_date as load_date
from {{ref('ctms_stg__subject_summary')}}
where studyid = '{{ study_id }}'
and "subject status" in ('Randomized','On Treatment','Off Study','In Follow-up')
group by studyid, EDC_Data_Refreshed_Date,"study stage"

{% else %}
SELECT
    ssr.study_id,
    NULL AS status, 
    'Enrollment Progress Actual' AS kpi_name,
    ssr."study phase" AS study_phase, 
    COUNT(DISTINCT ssr.subject) AS count, 
    ssr.irt_data_refreshed_date AS last_modified_date, 
    CURRENT_DATE AS load_date 
FROM {{ ref('ctms_stg__subject_summary_report') }} ssr   -- IRT
LEFT JOIN {{ ref('ctms_stg__subject_summary') }} ss      -- EDC
    ON ssr.study_id = ss.studyid
   AND REPLACE(ssr.subject,' ','') =
       RIGHT(REPLACE(ss."subject number",' ',''),4)
LEFT JOIN {{ source('koios_staging', 'participant_status_master') }} lkp
    ON UPPER(ssr.status) IS NOT DISTINCT FROM UPPER(lkp.participant_status_irt)
   AND UPPER(ss."subject status") IS NOT DISTINCT FROM UPPER(lkp.participant_status_edc)
WHERE ssr.study_id = '{{ study_id }}'
  AND (
        CASE 
            WHEN UPPER(ssr.status) IS NOT DISTINCT FROM UPPER(lkp.participant_status_irt)
             AND UPPER(ss."subject status") IS NOT DISTINCT FROM UPPER(lkp.participant_status_edc) 
            THEN lkp.participant_status
            ELSE ssr.status
        END 
      ) IN ('Discontinued','Enrolled','Randomized','On Treatment','Off Study','In Follow-up')
GROUP BY
    ssr.study_id,
    ssr."study phase",
    ssr.irt_data_refreshed_date

 {%endif%}
 
 --participant status KPI--
union all
{% if study_id in ['XL309-101','XL092-311']%}
select sr.study_id,
	CASE
    		WHEN sr.status ='Discontinued' THEN 'In Follow-up' 
        	WHEN sr.status='Enrolled' THEN 'Randomized'
            WHEN sr.status='Screening' THEN 'In Screening'             
		ELSE sr.status
	END as status,
	'Participant Status'as kpi_name,
    sr."study phase" AS study_phase,
	count(distinct subject ) as count,
    ss.EDC_Data_Refreshed_Date AS last_modified_date
	,current_date as load_date
	FROM {{ref('ctms_stg__subject_summary_report')}} sr
left join {{ref('ctms_stg__subject_summary')}} ss
    ON sr.study_id = ss.studyid
    AND RIGHT(sr.subject, 4) = RIGHT(ss."subject number", 4)
 WHERE sr.study_id = '{{ study_id }}'
and split_part(sr.subject,'-',2) not in (
select split_part(ss."subject number",'-',3) from {{ref('ctms_stg__subject_summary')}} ss where studyid = '{{ study_id }}' )
 GROUP BY
    sr.study_id,
    sr.status,
    sr."study phase",
    ss.EDC_Data_Refreshed_Date

{% elif study_id in ['XB628-101','XB371-101'] %}
select sr.study_id,
	CASE
    		WHEN sr.status ='Discontinued' THEN 'In Follow-up' 
        	WHEN sr.status='Enrolled' THEN 'Randomized'
            WHEN sr.status='Screening' THEN 'In Screening'
		ELSE sr.status
	END as status,
	'Participant Status'as kpi_name,
    'Escalation' AS study_phase,
	count(distinct subject ) as count,
    ss.EDC_Data_Refreshed_Date AS last_modified_date
	,current_date as load_date
	FROM {{ref('ctms_stg__subject_summary_report')}}sr
left join {{ref('ctms_stg__subject_summary')}} ss
    ON sr.study_id = ss.studyid
    AND RIGHT(sr.subject, 4) = RIGHT(ss."subject number", 4)
 WHERE sr.study_id = '{{study_id}}'
and split_part(sr.subject,'-',2) not in (
select split_part(ss."subject number",'-',3) from "koios"."koios_staging"."ctms_stg__subject_summary" ss where studyid = '{{study_id}}' )
 GROUP BY
    sr.study_id,
    sr.status,
    ss.EDC_Data_Refreshed_Date

{% elif study_id in ['XL092-009'] %}
SELECT
    ss.studyid,
    case
        WHEN sr."status" IS NOT NULL THEN (
            case
                WHEN ss."subject status" IS NULL 
                     AND sr."status" NOT IN ('Discontinued', 'Enrolled', 'Screening') 
                    THEN sr."status"
                WHEN (ss."subject status" IS NULL OR ss."subject status" = 'In Screening')  
                     AND sr."status" IN ('Enrolled', 'Randomized') 
                    THEN 'Randomized'
                WHEN (ss."subject status" IS NULL OR ss."subject status" IN ('On Treatment', 'In Screening')) 
                     AND sr."status" = 'Discontinued' 
                    THEN 'In Follow-up'
                WHEN ss."subject status" NOT IN ('Screen Failed', 'Screen Failure', 'In Screening') 
                     AND sr."status" IN ('Discontinued', 'Enrolled', 'Randomized', 'Treatment Completed') 
                    THEN ss."subject status"
                WHEN (ss."subject status" IS NULL OR ss."subject status" IN ('Screen Failed', 'Screen Failure', 'In Screening')) 
                     AND sr."status" IN ('Randomized', 'Enrolled', 'Treatment Completed') 
                    THEN sr."status"
                WHEN sr."status" = 'Screening' 
                    THEN 'In Screening'
                WHEN sr."status" IN ('Screen Failure Rollback Requested', 'Screen Failure') 
                    THEN sr."status"
                ELSE ss."subject status"
            END
        )
        ELSE ss."subject status"
    END AS status,
    'Participant Status'as kpi_name,
    'Escalation' AS study_phase,
    COUNT(distinct "subject number") AS count,
    ss.EDC_Data_Refreshed_Date AS last_modified_date
	,current_date as load_date
FROM {{ref('ctms_stg__subject_summary_report')}} sr
left join {{ref('ctms_stg__subject_summary')}} ss
    ON sr.study_id = ss.studyid
    AND RIGHT(sr.subject, 4) = RIGHT(ss."subject number", 4)
 WHERE ss.studyid = '{{study_id}}'  
GROUP BY
    ss.studyid,
    ss."subject status",
    sr.status,
    sr."study phase",
    ss.EDC_Data_Refreshed_Date


{% elif study_id in ['XL495-101'] %}
SELECT
    ss.studyid,
    case
        WHEN sr."status" IS NOT NULL THEN (
            case
                WHEN ss."subject status" IS NULL 
                     AND sr."status" NOT IN ('Discontinued', 'Enrolled', 'Screening') 
                    THEN sr."status"
                WHEN (ss."subject status" IS NULL OR ss."subject status" = 'In Screening')  
                     AND sr."status" IN ('Enrolled', 'Randomized') 
                    THEN 'Randomized'
                WHEN (ss."subject status" IS NULL OR ss."subject status" IN ('On Treatment', 'In Screening')) 
                     AND sr."status" = 'Discontinued' 
                    THEN 'In Follow-up'
                WHEN ss."subject status" NOT IN ('Screen Failed', 'Screen Failure', 'In Screening') 
                     AND sr."status" IN ('Discontinued', 'Enrolled', 'Randomized', 'Treatment Completed') 
                    THEN ss."subject status"
                WHEN (ss."subject status" IS NULL OR ss."subject status" IN ('Screen Failed', 'Screen Failure', 'In Screening')) 
                     AND sr."status" IN ('Randomized', 'Enrolled', 'Treatment Completed') 
                    THEN sr."status"
                WHEN sr."status" = 'Screening' 
                    THEN 'In Screening'
                WHEN sr."status" IN ('Screen Failure Rollback Requested', 'Screen Failure') 
                    THEN sr."status"
                ELSE ss."subject status"
            END
        )
        ELSE ss."subject status"
    END AS status,
    'Participant Status'as kpi_name,
    sr."study phase" AS study_phase,
    COUNT(distinct "subject number") AS count,
    ss.EDC_Data_Refreshed_Date AS last_modified_date
	,current_date as load_date
FROM  {{ref('ctms_stg__subject_summary_report')}} sr
left join {{ref('ctms_stg__subject_summary')}} ss
    ON sr.study_id = ss.studyid
    AND RIGHT(sr.subject, 4) = RIGHT(ss."subject number", 4)
 WHERE ss.studyid = '{{study_id}}' 
GROUP BY
    ss.studyid,
    ss."subject status",
    sr.status,
    sr."study phase",
    ss.EDC_Data_Refreshed_Date


{% elif study_id in ['XB002-101'] %}
select 
    ss.studyid,
    case
        WHEN sr.participant_status IS NOT NULL THEN (
            case
                WHEN ss."subject status" IS NULL 
                     AND sr.participant_status NOT IN ('Discontinued', 'Enrolled', 'Screening') 
                    THEN sr.participant_status
                WHEN (ss."subject status" IS NULL OR ss."subject status" = 'In Screening')  
                     AND sr.participant_status IN ('Enrolled', 'Randomized') 
                    THEN 'Randomized'
                WHEN (ss."subject status" IS NULL OR ss."subject status" IN ('On Treatment', 'In Screening')) 
                     AND sr.participant_status = 'Discontinued' 
                    THEN 'In Follow-up'
                WHEN ss."subject status" NOT IN ('Screen Failed', 'Screen Failure', 'In Screening') 
                     AND sr.participant_status IN ('Discontinued', 'Enrolled', 'Randomized', 'Treatment Completed') 
                    THEN ss."subject status"
                WHEN (ss."subject status" IS NULL OR ss."subject status" IN ('Screen Failed', 'Screen Failure', 'In Screening')) 
                     AND sr.participant_status IN ('Randomized', 'Enrolled', 'Treatment Completed') 
                    THEN sr.participant_status
                WHEN sr.participant_status = 'Screening' 
                    THEN 'In Screening'
                WHEN sr.participant_status IN ('Screen Failure Rollback Requested', 'Screen Failure') 
                    THEN sr.participant_status
                ELSE ss."subject status"
            END
        )
        ELSE ss."subject status"
    END AS status,
    'Participant Status'as kpi_name,
    sr.participant_type  AS study_phase,
    COUNT(distinct "subject number") AS count,
    ss.EDC_Data_Refreshed_Date AS last_modified_date
	,current_date as load_date
FROM  {{ref('ctms_stg__early_phase_subject_status')}} sr
left join {{ref('ctms_stg__subject_summary')}} ss
    ON sr.studyid = ss.studyid
    AND RIGHT(sr.subject, 4) = RIGHT(ss."subject number", 4)
 WHERE ss.studyid = '{{study_id}}'  
GROUP BY
    ss.studyid,
    ss."subject status",
    sr.participant_status,
    sr.participant_type,
    ss.EDC_Data_Refreshed_Date

{% elif study_id in ['XL092-002'] %}
SELECT
    main.study_id,
    main.final_status AS status,
    main.kpi_name,
    main.final_study_phase AS study_phase,
    COUNT(DISTINCT main.participant_id) AS count,   
    null as last_modified_date,
    main.load_date
FROM (
    SELECT DISTINCT
        ssr.study_id,
        RIGHT(ssr.subject, 4) AS participant_id,             
        CASE
            WHEN ssr.status IS NULL THEN ss."subject status"
            WHEN ss."subject status" IS NULL AND ssr.status NOT IN ('Discontinued', 'Enrolled', 'Screening') THEN ssr.status
            WHEN (ss."subject status" IS NULL OR ss."subject status" = 'In Screening')
                 AND ssr.status IN ('Enrolled', 'Randomized') THEN 'Randomized'
            WHEN (ss."subject status" IS NULL OR ss."subject status" IN ('On Treatment', 'In Screening'))
                 AND ssr.status = 'Discontinued' THEN 'In Follow-up'
            WHEN ss."subject status" NOT IN ('Screen Failed', 'Screen Failure', 'In Screening')
                 AND ssr.status IN ('Discontinued', 'Enrolled', 'Randomized', 'Treatment Completed') THEN ss."subject status"
            WHEN (ss."subject status" IS NULL OR ss."subject status" IN ('Screen Failed', 'Screen Failure', 'In Screening'))
                 AND ssr.status IN ('Randomized', 'Enrolled', 'Treatment Completed') THEN ssr.status
            WHEN ssr.status = 'Screening' THEN 'In Screening'
            WHEN ssr.status IN ('Screen Failure Rollback Requested', 'Screen Failure') THEN ssr.status
            ELSE ss."subject status"
        END AS final_status,
        'Participant Status' AS kpi_name,        
        COALESCE(
            CASE
                WHEN sp.study_id IS NOT NULL THEN sp.calculated_phase
                ELSE ssr."study phase"
            END,
            ssr."study phase"
        ) AS final_study_phase, 
        ss.EDC_Data_Refreshed_Date AS last_modified_date,
        current_date AS load_date 
    FROM {{ ref('ctms_stg__subject_summary_report') }} ssr
    LEFT JOIN {{ ref('ctms_stg__subject_summary') }} ss
        ON ssr.study_id = ss.studyid 
        AND RIGHT(ssr.subject, 4) = RIGHT(ss."subject number", 4) 
    LEFT JOIN (
        SELECT DISTINCT   
            study_id,
            RIGHT(subject, 4) AS participant_id,
            status AS participant_status,
            CASE
                WHEN status = 'Screen Failure' AND "tumor type" = 'Solid Tumor' THEN 'Escalation'
                WHEN status = 'In Screening' AND "tumor type" = 'Solid Tumor' THEN 'Escalation'
                WHEN status IN ('In Screening', 'Screening') AND "tumor type" <> 'Solid Tumor' THEN 'Expansion'
                WHEN status LIKE 'Screen Failure%' AND "tumor type" <> 'Solid Tumor' THEN 'Expansion'
                ELSE "study phase"
            END AS calculated_phase
        FROM {{ ref('ctms_stg__subject_summary_report') }}
        WHERE study_id = '{{ study_id }}'
 
        UNION   
 
        SELECT DISTINCT
            studyid AS study_id,
            participant_id,
            participant_status,
            participant_type AS calculated_phase
        FROM {{ ref('ctms_stg__early_phase_subject_status') }}
        WHERE studyid = '{{ study_id }}'
          AND TRIM(studyid) || TRIM(participant_id) NOT IN (
              SELECT TRIM(study_id) || TRIM(RIGHT(subject, 4))
              FROM {{ ref('ctms_stg__subject_summary_report') }}
              WHERE study_id = '{{ study_id }}'
          )
    ) sp
    ON ssr.study_id = sp.study_id 
    AND RIGHT(ssr.subject, 4) = sp.participant_id
 
    WHERE ssr.study_id = '{{  study_id }}'
      AND ssr.status != 'Screen Failure Rollback Requested'
) AS main
GROUP BY
    main.study_id,
    main.final_status,
    main.kpi_name,
    main.final_study_phase,     
    main.load_date

{% elif study_id in ['XB010-101'] %}
 
SELECT
    main.study_id,
    main.final_status AS status,
    main.kpi_name,
    main.final_study_phase AS study_phase,
    COUNT(DISTINCT main.participant_id) AS count,    
    main.last_modified_date,
    main.load_date
FROM (
    SELECT DISTINCT
        ssr.study_id,
        RIGHT(ssr.subject, 4) AS participant_id,   
        CASE
            WHEN ssr.status IS NULL THEN ss."subject status"
            WHEN ss."subject status" IS NULL AND ssr.status NOT IN ('Discontinued', 'Enrolled', 'Screening') THEN ssr.status
            WHEN (ss."subject status" IS NULL OR ss."subject status" = 'In Screening')
                 AND ssr.status IN ('Enrolled', 'Randomized') THEN 'Randomized'
            WHEN (ss."subject status" IS NULL OR ss."subject status" IN ('On Treatment', 'In Screening'))
                 AND ssr.status = 'Discontinued' THEN 'In Follow-up'
            WHEN ss."subject status" NOT IN ('Screen Failed', 'Screen Failure', 'In Screening')
                 AND ssr.status IN ('Discontinued', 'Enrolled', 'Randomized', 'Treatment Completed') THEN ss."subject status"
            WHEN (ss."subject status" IS NULL OR ss."subject status" IN ('Screen Failed', 'Screen Failure', 'In Screening'))
                 AND ssr.status IN ('Randomized', 'Enrolled', 'Treatment Completed') THEN ssr.status
            WHEN ssr.status = 'Screening' THEN 'In Screening'
            WHEN ssr.status IN ( 'Screen Failure') THEN ssr.status
            ELSE ss."subject status"
        END AS final_status, 
        'Participant Status' AS kpi_name, 
        ssr."study phase" as final_study_phase, 
        ssr.IRT_Data_Refreshed_Date AS last_modified_date,
        current_date AS load_date 
   	FROM {{ref('ctms_stg__subject_summary_report')}} ssr
left join {{ref('ctms_stg__subject_summary')}} ss
        ON ssr.study_id = ss.studyid
        AND RIGHT(ssr.subject, 4) = RIGHT(ss."subject number", 4)
    WHERE ssr.study_id =  '{{ study_id }}' 
      AND ssr.status != 'Screen Failure Rollback Requested'
) AS main 
GROUP BY
    main.study_id,
    main.final_status,
    main.kpi_name,
    main.final_study_phase,
    main.last_modified_date,
    main.load_date
 

{% else %}
SELECT
    t.study_id,
    t.status,
    'Participant Status' AS kpi_name,
    t.study_phase,
    COUNT(DISTINCT t.subject_id) AS count,
    t.last_modified_date,
    CURRENT_DATE AS load_date
FROM (
    SELECT
        sr.study_id,
        RIGHT(sr."subject", 4) AS subject_id,
        CASE
            WHEN sr."status" IS NOT NULL THEN
                CASE
                   WHEN ss."subject status" IS NULL
                         AND sr."status" NOT IN ('Discontinued', 'Enrolled', 'Screening')
                        THEN sr."status"
                    WHEN (ss."subject status" IS NULL OR ss."subject status" = 'In Screening')
                        AND sr."status" IN ('Enrolled', 'Randomized')
                        THEN 'Randomized' 
                    WHEN ss."subject status" IN ('In Follow-up', 'On Treatment')
                         AND sr."status" = 'Discontinued'
                        THEN 'In Follow-up'
                    WHEN ss."subject status" NOT IN ('Screen Failed', 'Screen Failure', 'In Screening')
                         AND sr."status" IN ('Discontinued', 'Enrolled', 'Randomized', 'Treatment Completed')
                        THEN ss."subject status"
                    WHEN (ss."subject status" IS NULL
                          OR ss."subject status" IN ('Screen Failed', 'Screen Failure', 'In Screening'))
                         AND sr."status" IN ('Randomized', 'Enrolled', 'Treatment Completed')
                        THEN sr."status" 
                    WHEN sr."status" IN ('Screening', 'In Screening')
                        THEN 'In Screening' 
                    WHEN sr."status" IN ('Screen Failure Rollback Requested', 'Screen Failure')
                        THEN sr."status" 
                    ELSE ss."subject status"
                END
            ELSE ss."subject status"
        END AS status,
        sr."study phase" AS study_phase,
        sr.IRT_Data_Refreshed_Date AS last_modified_date
 	FROM {{ref('ctms_stg__subject_summary_report')}} sr
left join {{ref('ctms_stg__subject_summary')}} ss
        ON sr.study_id = ss.studyid
        AND RIGHT(sr.subject, 4) = RIGHT(ss."subject number", 4)
    WHERE sr.study_id = '{{ study_id }}'
    AND sr.status not in ('Screen Failure Rollback Requested' ,'In-Active')
) t
GROUP BY
    t.study_id,
    t.status,
    t.study_phase,
    t.last_modified_date
 
{%endif%}    

union all

{% if study_id in ['XL092-305'] %}
select study_protocol_number as study_id
,'Active' as status
,'Country Activation status Actual' as kpi_name
,Null as study_phase
,count(*) as count
,last_modified_date
,current_date as load_date
from {{ref ('ctms_stg__study_country')}} 
where study_protocol_number = '{{ study_id }}'
and country_status in ('Active','Enrollment Closed')
group by study_protocol_number, last_modified_date
{% else %}
select study_protocol_number as study_id
,country_status as status
,'Country Activation status Actual' as kpi_name
,Null as study_phase
,count(*) as count
,last_modified_date
,current_date as load_date
from {{ref ('ctms_stg__study_country')}} 
where study_protocol_number = '{{ study_id }}'
and country_status in ('Active')
group by study_protocol_number, country_status,last_modified_date
{% endif %}
union all

select study_protocol_number as study_id
,country_status as status
,'Country Activation Status Forecast' as kpi_name
,Null as study_phase
,count(*) as count
,last_modified_date
,current_date as load_date
from {{ref ('ctms_stg__study_country')}} 
where study_protocol_number = '{{ study_id }}'
and country_status in ('Active','Planned','Approved')
group by study_protocol_number, country_status,last_modified_date
union all

{% if study_id in ['XL309-101','XL092-201'] %}
select study_id,
null as status,
'Site Activation Status Actual' as kpi_name,
null as study_phase,
count(*),
null as last_modified_date
,current_date as load_date
from {{ref('ctms_stg__study_site_phase')}} 
where study_id = '{{ study_id}}' and 
study_site_status in('Site Closed','Enrollment Open')
group by study_id,last_modified_date

{% else %}
select ss.study_protocol_number as study_id,
null as status,
'Site Activation status Actual' as kpi_name,
null as study_phase, 
count(distinct study_site_id) as count,
null as last_modified_date
,current_date as load_date
from {{ref('ctms_stg__study_site')}} ss
where ss.study_protocol_number='{{study_id}}' 
and ss.study_site_status in ('Activated', 'Active Enrolling', 'Enrollment Open','Enrollment Closed','Closed','Close Out Ready', 'Enrollment On-Hold') 
and site_type = 'Main' 
and site_number not in ('TRIO')
GROUP BY ss.study_protocol_number
{% endif %}
union all


select studyid,
null as status,
'Missing pages' as kpi_name,
null as study_phase,
sum("missing pgs (entry not started)"),
edc_data_refreshed_date as last_modified_date
,current_date as load_date
from  {{ref ('ctms_stg__subject_summary')}} 
where studyid = '{{ study_id }}' 
group by  studyid, edc_data_refreshed_date

union all
select studyid
, "query_status" as status
, 'Open queries'as kpi_name
, Null as study_phase
, count("query_id") as count
,last_modified_date
,current_date as load_date
from {{ref ('ctms_stg__query_detail')}}
where studyid = '{{ study_id }}'
and "query_status" in ('Opened')
group by studyid,"query_status",last_modified_date

union all

select studyid
, "query_status" as status
, 'Queries closed'as kpi_name
, Null as study_phase
, count("query_id") as count
,last_modified_date
,current_date as load_date
from {{ref ('ctms_stg__query_detail')}}
where studyid = '{{ study_id }}'
and "query_status" in ('Closed')
group by studyid,"query_status",last_modified_date
union all

select studyid
, "query_status" as status
,'Open in 30-60 days' as kpi_name
,Null as study_phase
,count(*) as count
,last_modified_date
,current_date as load_date
from {{ref ('ctms_stg__query_detail')}}
where studyid = '{{ study_id }}'
  and "query_status" = 'Opened'
  and ( current_date  - cast("opened_date" as date))>31
  and ( current_date  - cast("opened_date" as date))<=61
group by studyid,"query_status",last_modified_date
union all
select studyid
, "query_status" as status
,'Open in 60-90 days' as kpi_name
,null as study_phase
,count(*) as count
,last_modified_date
,current_date as load_date
from {{ref ('ctms_stg__query_detail')}}
where studyid = '{{ study_id }}'
  and "query_status" = 'Opened'
  and ( current_date  - cast("opened_date" as date))>61
  and ( current_date  - cast("opened_date" as date))<=91
group by studyid,"query_status",last_modified_date
union all
select studyid
, "query_status" as status
,'Open in 90 days' as kpi_name
,null as study_phase
,count(*) as count
,last_modified_date
,current_date as load_date
from {{ref ('ctms_stg__query_detail')}}
  where studyid = '{{ study_id }}'
  and "query_status" = 'Opened'
  and ( current_date  - cast("opened_date" as date))>91
group by studyid,"query_status",last_modified_date
union all

select ssr.study_id,
'In_Screenig' as status,
'Cohort Summary Report' as KPI_Name,
null as study_phase,
count(distinct subject) ,
irt_data_refreshed_date
,current_date as load_date
from {{ref('ctms_stg__subject_summary_report')}} ssr 
left join {{ref('ctms_stg__cohort_summary_report')}} csr
on ssr.study_id=csr.study_id
and lower(ssr.cohort)=lower(csr.cohort)
where status = 'Screening'
and status is not null
and ssr.study_id ='{{study_id}}'
and ssr.study_id not in ('XL092-303','XL092-304','XL092-305'  )
group by ssr.study_id,irt_data_refreshed_date

union all

select ssr.study_id,
'Total Discontinued' as status,
'Cohort Summary Report' as KPI_Name,
Null as study_phase,
count(distinct subject),
ssr.irt_data_refreshed_date 
,current_date as load_date
from {{ref('ctms_stg__subject_summary_report')}} ssr
left join {{ref('ctms_stg__cohort_summary_report')}} csr
on ssr.study_id=csr.study_id
and lower(ssr.cohort)=lower(csr.cohort)
where ssr.study_id ='{{study_id}}'
and ssr.study_id not in ('XL092-303','XL092-304','XL092-305','XL092-311')
and status in ('Discontinued')
group by ssr.study_id,status,ssr.irt_data_refreshed_date 
union all

select ssr.study_id,
'Total Enrolled' as status,
'Cohort Summary Report' as KPI_Name,
Null as study_phase,
count(distinct subject),
ssr.irt_data_refreshed_date
,current_date as load_date
from {{ref('ctms_stg__subject_summary_report')}} ssr 
left join {{ref('ctms_stg__cohort_summary_report')}} csr
on ssr.study_id=csr.study_id
and lower(ssr.cohort)=lower(csr.cohort)
where ssr.study_id ='{{study_id}}'
and ssr.study_id not in ('XL092-303','XL092-304','XL092-305','XL092-311')
and status in ('Enrolled','Discontinued')
group by ssr.study_id,ssr.irt_data_refreshed_date
union all

select ssr.study_id,
'Screen Failed' as status,
'Cohort Summary Report' as KPI_Name,
null as study_phase,
count(distinct subject),
ssr.irt_data_refreshed_date 
,current_date as load_date
from {{ref('ctms_stg__subject_summary_report')}} ssr 
left join {{ref('ctms_stg__cohort_summary_report')}} csr
on ssr.study_id=csr.study_id
and lower(ssr.cohort)=lower(csr.cohort)
where ssr.study_id ='{{study_id}}'
and ssr.study_id not in ('XL092-303','XL092-304','XL092-305','XL092-311')
and status in('Screen Failure' ,'Screen Failed')
group by ssr.study_id,ssr.irt_data_refreshed_date 
union all

{% if study_id in ['XB010-101'] %}
select ssr.study_id,
'Total Screened' as status,
'Cohort Summary Report' as KPI_Name,
null as study_phase,
count(distinct subject),
ssr.irt_data_refreshed_date 
,current_date as load_date
from {{ref('ctms_stg__subject_summary_report')}} ssr 
left join {{ref('ctms_stg__cohort_summary_report')}} csr
on ssr.study_id=csr.study_id
and lower(ssr.cohort)=lower(csr.cohort)
where ssr.study_id ='{{study_id}}'
and ssr."study phase" is not null
and ssr.study_id not in ('XL092-303','XL092-304','XL092-305','XL092-311')
and status in ('Enrolled','Discontinued','Screen Failure','In Screening', 'Screening','Screened','Screen Failed')
group by ssr.study_id,ssr.irt_data_refreshed_date
{% else %}
select ssr.study_id,
'Total Screened' as status,
'Cohort Summary Report' as KPI_Name,
null as study_phase,
count(distinct subject),
ssr.irt_data_refreshed_date 
,current_date as load_date
from {{ref('ctms_stg__subject_summary_report')}} ssr 
left join {{ref('ctms_stg__cohort_summary_report')}} csr
on ssr.study_id=csr.study_id
and lower(ssr.cohort)=lower(csr.cohort)
where ssr.study_id ='{{study_id}}'
and ssr.study_id not in ('XL092-303','XL092-304','XL092-305','XL092-311')
and status in ('Enrolled','Discontinued','Screen Failure','In Screening', 'Screening','Screened','Screen Failed')
group by ssr.study_id,ssr.irt_data_refreshed_date

{% endif %}
union all 

select
    ssr.study_id ,
    coalesce(lkp.participant_status,ssr.status) as status,
    '#Active on IP' as kpi_name,
    ssr."study phase" as study_phase,
    count(distinct ssr.subject) as count,
   ssr.irt_data_refreshed_date as last_modified_date,
    current_date as load_date
from  {{ref('ctms_stg__subject_summary_report')}}  ssr   -- IRT
left join  {{ref('ctms_stg__subject_summary')}}  ss      -- EDC
    on ssr.study_id = ss.studyid
   and replace(ssr.subject,' ','') =
    right(replace(ss."subject number",' ',''),4)
left join  {{ source('koios_staging', 'participant_status_master') }} lkp
    on upper(ssr.status) is not distinct from upper(lkp.participant_status_irt)
   and upper(ss."subject status") is not distinct from upper(lkp.participant_status_edc) 
where ssr.study_id = '{{study_id}}'
  and
  (
      case
          when upper(ssr.status) is not distinct from upper(lkp.participant_status_irt)
           and upper(ss."subject status") is not distinct from upper(lkp.participant_status_edc)
          then lkp.participant_status
          else ssr.status
      end
  ) = 'On Treatment' 
group by ssr.study_id, ssr.status,ssr."study phase",last_modified_date,lkp.participant_status

union all 

select
    ssr.study_id ,
    coalesce(lkp.participant_status,ssr.status)  as status,
    '#Active on Study' as kpi_name,
    ssr."study phase" as study_phase,
    count(distinct ssr.subject) as count,
   ssr.irt_data_refreshed_date as last_modified_date,
    current_date as load_date
from  {{ref('ctms_stg__subject_summary_report')}}  ssr   -- IRT
left join  {{ref('ctms_stg__subject_summary')}}  ss      -- EDC
    on ssr.study_id = ss.studyid
   and replace(ssr.subject,' ','') =
    right(replace(ss."subject number",' ',''),4)
left join  {{ source('koios_staging', 'participant_status_master') }} lkp
    on upper(ssr.status) is not distinct from upper(lkp.participant_status_irt)
   and upper(ss."subject status") is not distinct from upper(lkp.participant_status_edc) 
where ssr.study_id = '{{study_id}}'
  and
  (
      case
          when upper(ssr.status) is not distinct from upper(lkp.participant_status_irt)
           and upper(ss."subject status") is not distinct from upper(lkp.participant_status_edc)
          then lkp.participant_status
          else ssr.status
      end
  ) IN ('On Treatment','Randomized','In Follow-up') 
group by ssr.study_id, ssr.status,ssr."study phase",last_modified_date,lkp.participant_status
 
{% endmacro %}
