{% macro stg_subject_visit_summary_report() %}
cast(study_id as varchar) as  study_id
,cast(country as varchar) as  country
,cast(studysite as varchar) as  studysite
,cast(investigator as varchar) as  investigator
,cast(subject as varchar) as  subject
,cast("discontinued rollback request approved?" as varchar) as  "discontinued rollback request approved?"
,cast("screen failure rollback request approved?" as varchar) as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,cast("visit description" as varchar) as  "visit description"
,cast("scheduled date [local]" as varchar) as  "scheduled date"
,cast("actual date [local]" as varchar) as  "actual date"
,md5(study_id||country||studysite||subject||"visit id") as hash_key
{% endmacro %}