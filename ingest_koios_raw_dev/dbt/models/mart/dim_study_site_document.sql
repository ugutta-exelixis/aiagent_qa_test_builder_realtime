with dim_study_site_document as(
    select 
    md5(study_protocol_number) as study_id_sk,
    md5(study_site_id) as study_site_id_doc_id_sk,
    study_protocol_number as study_id,
    study_site_id as study_site_id_doc_id,
    to_date(ip_pack_submitted_to_iedr,'DD-MON-YY') as ip_pack_submitted_to_iedr,
    to_date(ip_pack_rejected_by_iedr,'DD-MON-YY') as ip_pack_rejected_by_iedr,
    to_date(ip_pack_approval_by_iedr,'DD-MON-YY') as ip_pack_approval_by_iedr,
    to_DATE(ip_pack_submitted_to_sponsor,'DD-MON-YY') as ip_pack_submitted_to_sponsor,
    to_date(ip_pack_rejected_by_sponsor,'DD-MON-YY') as ip_pack_rejected_by_sponsor,
    to_date(ip_pack_approval_by_sponsor,'DD-MON-YY') as ip_pack_approval_by_sponsor,
    case when first_cda_sent_date = 'Not Required' then Null else to_date(first_cda_sent_date,'DD-MON-YY') end as first_cda_sent_date,
    case when first_cda_completion_date = 'Not Required' then Null else to_date(first_cda_completion_date,'DD-MON-YY') end as first_cda_completion_date,
    case when first_srq_sent_date = 'Not Required' then Null else to_date(first_srq_sent_date,'DD-MON-YY') end as first_srq_sent_date,
    case when first_srq_complete_date = 'Not Required' then Null Else to_date(first_srq_complete_date,'DD-MON-YY') end as first_srq_complete_date,
    to_date(load_date,'YYYY-MM-DD') as load_date,
    current_timestamp as aud_created_date,
    current_timestamp as aud_updated_date
     from {{ref('ctms_stg__study_site')}}
)
select * from  dim_study_site_document