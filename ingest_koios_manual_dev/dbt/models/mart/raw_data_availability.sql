with raw_data_availability as ( 
select  'xl102-101_query_detail' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl102-101_query_detail" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl102-101_query_detail")
union
select  'pra_exl_contact_association_xl092_001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_contact_association_xl092_001" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_contact_association_xl092_001")
union
select  'pra_exl_address_association_xl092_001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_address_association_xl092_001" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_address_association_xl092_001")
union
select  'xl184-315_query_detail_na_row' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl184-315_query_detail_na_row" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl184-315_query_detail_na_row")
union
select  'pra_exl_address' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_address" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_address")
union
select  'enroll_forecast_xb002-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."enroll_forecast_xb002-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."enroll_forecast_xb002-101")
union
select  'pra_exl_study_metric_xb002_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_metric_xb002_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_metric_xb002_101")
union
select  'pra_exl_study_subject_xb002_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_subject_xb002_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_subject_xb002_101")
union
select  'pra_exl_study_country_xl092_001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_country_xl092_001" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_country_xl092_001")
union
select  'pra_exl_study_milestone_xl092_001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_milestone_xl092_001" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_milestone_xl092_001")
union
select  'subject_summary_report_xl092_001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."subject_summary_report_xl092_001" where last_modified_date = (select max(last_modified_date) from  koios_raw."subject_summary_report_xl092_001")
union
select  'pra_exl_study_site_xl092_001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_xl092_001" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_xl092_001")
union
select  'pra_exl_study_subject_xl092_001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_subject_xl092_001" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_subject_xl092_001")
union
select  'xl102-101_subject_summary' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl102-101_subject_summary" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl102-101_subject_summary")
union
select  'xl092-303_overall_edc_study_metrics' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-303_overall_edc_study_metrics" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-303_overall_edc_study_metrics")
union
select  'xl184-315_query_details_uncl' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl184-315_query_details_uncl" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl184-315_query_details_uncl")
union
select  'pra_exl_contact' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_contact" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_contact")
union
select  'subject_visit_summary_report_xl092_001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."subject_visit_summary_report_xl092_001" where last_modified_date = (select max(last_modified_date) from  koios_raw."subject_visit_summary_report_xl092_001")
union
select  'xl092-303_query_detail' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-303_query_detail" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-303_query_detail")
union
select  'xl102-101_subject_summary_by_visit' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl102-101_subject_summary_by_visit" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl102-101_subject_summary_by_visit")
union
select  'xl092-303_subject_summary' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-303_subject_summary" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-303_subject_summary")
union
select  'pra_exl_account_association_xb002_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_account_association_xb002_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_account_association_xb002_101")
union
select  'pra_exl_srm_enroll_proj_xb002-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_enroll_proj_xb002-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_enroll_proj_xb002-101")
union
select  'pra_exl_study_milestone_xb002_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_milestone_xb002_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_milestone_xb002_101")
union
select  'pra_exl_study_submission_xb002_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_submission_xb002_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_submission_xb002_101")
union
select  'subject_summary_report_xl184_315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."subject_summary_report_xl184_315" where last_modified_date = (select max(last_modified_date) from  koios_raw."subject_summary_report_xl184_315")
union
select  'pra_exl_srm_proj_sites_xl092_001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_proj_sites_xl092_001" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_proj_sites_xl092_001")
union
select  'xl184-315_subject_summary' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl184-315_subject_summary" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl184-315_subject_summary")
union
select  'pra_exl_study_metric_xl092_001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_metric_xl092_001" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_metric_xl092_001")
union
select  'pra_exl_study_site_monitor_xl092_001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_monitor_xl092_001" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_monitor_xl092_001")
union
select  'xl092-002_overall_edc_study_metrics' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-002_overall_edc_study_metrics" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-002_overall_edc_study_metrics")
union
select  'site_contact_details_xl092-304' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."site_contact_details_xl092-304" where last_modified_date = (select max(last_modified_date) from  koios_raw."site_contact_details_xl092-304")
union
select  'xl092-303_subject_summary_by_visit' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-303_subject_summary_by_visit" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-303_subject_summary_by_visit")
union
select  'xl184-315_subject_summary_by_visit' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl184-315_subject_summary_by_visit" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl184-315_subject_summary_by_visit")
union
select  'pra_exl_address_association_xb002_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_address_association_xb002_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_address_association_xb002_101")
union
select  'pra_exl_srm_proj_sites_xb002_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_proj_sites_xb002_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_proj_sites_xb002_101")
union
select  'pra_exl_study_site_monitor_xb002_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_monitor_xb002_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_monitor_xb002_101")
union
select  'pra_exl_study_xb002-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_xb002-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_xb002-101")
union
select  'xl092-304_subject_summary' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-304_subject_summary" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-304_subject_summary")
union
select  'xl184-315_overall_edc_study_metrics' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl184-315_overall_edc_study_metrics" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl184-315_overall_edc_study_metrics")
union
select  'pra_exl_srm_enroll_proj_xl092-001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_enroll_proj_xl092-001" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_enroll_proj_xl092-001")
union
select  'pra_exl_account' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_account" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_account")
union
select  'pra_exl_contact_association_xb002_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_contact_association_xb002_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_contact_association_xb002_101")
union
select  'pra_exl_study_country_xb002_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_country_xb002_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_country_xb002_101")
union
select  'pra_exl_study_site_xb002_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_xb002_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_xb002_101")
union
select  'xl184-315_query_detail_apac' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl184-315_query_detail_apac" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl184-315_query_detail_apac")
union
select  'enroll_forecast_xl092-001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."enroll_forecast_xl092-001" where last_modified_date = (select max(last_modified_date) from  koios_raw."enroll_forecast_xl092-001")
union
select  'pra_exl_account_association_xl092_001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_account_association_xl092_001" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_account_association_xl092_001")
union
select  'pra_exl_account_association_xl092-002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_account_association_xl092-002" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_account_association_xl092-002")
union
select  'pra_exl_address_association_xl092-002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_address_association_xl092-002" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_address_association_xl092-002")
union
select  'pra_exl_contact_association_xl092-002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_contact_association_xl092-002" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_contact_association_xl092-002")
union
select  'enroll_forecast_xl092-002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."enroll_forecast_xl092-002" where last_modified_date = (select max(last_modified_date) from  koios_raw."enroll_forecast_xl092-002")
union
select  'pra_exl_srm_enroll_proj_xl092-002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_enroll_proj_xl092-002" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_enroll_proj_xl092-002")
union
select  'pra_exl_srm_proj_sites_xl092_002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_proj_sites_xl092_002" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_proj_sites_xl092_002")
union
select  'pra_exl_study_country_xl092-002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_country_xl092-002" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_country_xl092-002")
union
select  'pra_exl_study_metric_xl092-002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_metric_xl092-002" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_metric_xl092-002")
union
select  'pra_exl_study_milestone_xl092-002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_milestone_xl092-002" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_milestone_xl092-002")
union
select  'pra_exl_study_site_monitor_xl092-002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_monitor_xl092-002" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_monitor_xl092-002")
union
select  'pra_exl_study_site_xl092-002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_xl092-002" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_xl092-002")
union
select  'pra_exl_study_subject_xl092-002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_subject_xl092-002" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_subject_xl092-002")
union
select  'pra_exl_study_submission_xl092-002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_submission_xl092-002" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_submission_xl092-002")
union
select  'pra_exl_study_submission_xl092_001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_submission_xl092_001" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_submission_xl092_001")
union
select  'pra_exl_study_xl092_001' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_xl092_001" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_xl092_001")
union
select  'pra_exl_study_xl092-002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_xl092-002" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_xl092-002")
union
select  'pra_exl_account_association_xl092-303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_account_association_xl092-303" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_account_association_xl092-303")
union
select  'pra_exl_address_association_xl092-303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_address_association_xl092-303" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_address_association_xl092-303")
union
select  'pra_exl_contact_association_xl092-303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_contact_association_xl092-303" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_contact_association_xl092-303")
union
select  'enroll_forecast_xl092-303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."enroll_forecast_xl092-303" where last_modified_date = (select max(last_modified_date) from  koios_raw."enroll_forecast_xl092-303")
union
select  'pra_exl_srm_enroll_proj_xl092-303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_enroll_proj_xl092-303" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_enroll_proj_xl092-303")
union
select  'pra_exl_srm_proj_sites_xl092_303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_proj_sites_xl092_303" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_proj_sites_xl092_303")
union
select  'pra_exl_study_country_xl092-303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_country_xl092-303" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_country_xl092-303")
union
select  'pra_exl_study_metric_xl092-303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_metric_xl092-303" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_metric_xl092-303")
union
select  'pra_exl_study_milestone_xl092-303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_milestone_xl092-303" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_milestone_xl092-303")
union
select  'pra_exl_study_site_monitor_xl092-303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_monitor_xl092-303" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_monitor_xl092-303")
union
select  'pra_exl_study_site_xl092-303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_xl092-303" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_xl092-303")
union
select  'pra_exl_study_subject_xl092-303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_subject_xl092-303" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_subject_xl092-303")
union
select  'pra_exl_study_submission_xl092-303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_submission_xl092-303" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_submission_xl092-303")
union
select  'pra_exl_study_xl092-303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_xl092-303" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_xl092-303")
union
select  'pra_exl_contact_association_xl102_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_contact_association_xl102_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_contact_association_xl102_101")
union
select  'pra_exl_srm_enroll_proj_xl102-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_enroll_proj_xl102-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_enroll_proj_xl102-101")
union
select  'pra_exl_study_submission_xl102_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_submission_xl102_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_submission_xl102_101")
union
select  'pra_exl_srm_proj_sites_xl102_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_proj_sites_xl102_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_proj_sites_xl102_101")
union
select  'pra_exl_study_country_xl102_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_country_xl102_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_country_xl102_101")
union
select  'pra_exl_account_association_xl102_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_account_association_xl102_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_account_association_xl102_101")
union
select  'pra_exl_study_xl102_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_xl102_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_xl102_101")
union
select  'subject_visit_summary_report_xl184_315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."subject_visit_summary_report_xl184_315" where last_modified_date = (select max(last_modified_date) from  koios_raw."subject_visit_summary_report_xl184_315")
union
select  'pra_exl_study_metric_xl102_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_metric_xl102_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_metric_xl102_101")
union
select  'pra_exl_study_milestone_xl102_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_milestone_xl102_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_milestone_xl102_101")
union
select  'pra_exl_study_site_monitor_xl102_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_monitor_xl102_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_monitor_xl102_101")
union
select  'pra_exl_study_site_xl102_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_xl102_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_xl102_101")
union
select  'pra_exl_study_subject_xl102_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_subject_xl102_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_subject_xl102_101")
union
select  'enroll_forecast_xl102-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."enroll_forecast_xl102-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."enroll_forecast_xl102-101")
union
select  'pra_exl_address_association_xl102_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_address_association_xl102_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_address_association_xl102_101")
union
select  'pra_exl_address_association_xl184_311' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_address_association_xl184_311" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_address_association_xl184_311")
union
select  'pra_exl_contact_association_xl184_021' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_contact_association_xl184_021" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_contact_association_xl184_021")
union
select  'pra_exl_study_submission_xl184_021' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_submission_xl184_021" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_submission_xl184_021")
union
select  'pra_exl_srm_enroll_proj_xl184-021' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_enroll_proj_xl184-021" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_enroll_proj_xl184-021")
union
select  'pra_exl_account_association_xl184_021' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_account_association_xl184_021" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_account_association_xl184_021")
union
select  'pra_exl_study_xl184_021' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_xl184_021" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_xl184_021")
union
select  'xl102-101_overall_edc_study_metrics' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl102-101_overall_edc_study_metrics" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl102-101_overall_edc_study_metrics")
union
select  'pra_exl_study_country_xl184_021' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_country_xl184_021" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_country_xl184_021")
union
select  'pra_exl_study_metric_xl184_021' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_metric_xl184_021" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_metric_xl184_021")
union
select  'pra_exl_study_milestone_xl184_021' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_milestone_xl184_021" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_milestone_xl184_021")
union
select  'pra_exl_study_site_monitor_xl184_021' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_monitor_xl184_021" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_monitor_xl184_021")
union
select  'pra_exl_study_site_xl184_021' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_xl184_021" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_xl184_021")
union
select  'pra_exl_srm_proj_sites_xl184_021' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_proj_sites_xl184_021" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_proj_sites_xl184_021")
union
select  'pra_exl_address_association_xl184_021' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_address_association_xl184_021" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_address_association_xl184_021")
union
select  'enroll_forecast_xl184-021' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."enroll_forecast_xl184-021" where last_modified_date = (select max(last_modified_date) from  koios_raw."enroll_forecast_xl184-021")
union
select  'pra_exl_account_association_xl184_311' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_account_association_xl184_311" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_account_association_xl184_311")
union
select  'pra_exl_study_submission_xl184_311' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_submission_xl184_311" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_submission_xl184_311")
union
select  'pra_exl_address_association_xl184_312' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_address_association_xl184_312" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_address_association_xl184_312")
union
select  'pra_exl_account_association_xl184_312' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_account_association_xl184_312" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_account_association_xl184_312")
union
select  'pra_exl_study_xl184_311' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_xl184_311" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_xl184_311")
union
select  'pra_exl_study_country_xl184_311' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_country_xl184_311" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_country_xl184_311")
union
select  'pra_exl_study_metric_xl184_311' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_metric_xl184_311" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_metric_xl184_311")
union
select  'pra_exl_study_milestone_xl184_311' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_milestone_xl184_311" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_milestone_xl184_311")
union
select  'pra_exl_study_site_monitor_xl184_311' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_monitor_xl184_311" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_monitor_xl184_311")
union
select  'pra_exl_study_site_xl184_311' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_xl184_311" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_xl184_311")
union
select  'pra_exl_study_subject_xl184_311' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_subject_xl184_311" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_subject_xl184_311")
union
select  'pra_exl_contact_association_xl184_311' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_contact_association_xl184_311" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_contact_association_xl184_311")
union
select  'enroll_forecast_xl184-311' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."enroll_forecast_xl184-311" where last_modified_date = (select max(last_modified_date) from  koios_raw."enroll_forecast_xl184-311")
union
select  'pra_exl_srm_enroll_proj_xl184-311' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_enroll_proj_xl184-311" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_enroll_proj_xl184-311")
union
select  'pra_exl_srm_proj_sites_xl184_311' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_proj_sites_xl184_311" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_proj_sites_xl184_311")
union
select  'pra_exl_study_submission_xl184_312' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_submission_xl184_312" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_submission_xl184_312")
union
select  'pra_exl_address_association_xl184_315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_address_association_xl184_315" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_address_association_xl184_315")
union
select  'pra_exl_contact_association_xl184-312' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_contact_association_xl184-312" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_contact_association_xl184-312")
union
select  'pra_exl_study_xl184_312' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_xl184_312" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_xl184_312")
union
select  'pra_exl_study_country_xl184_312' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_country_xl184_312" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_country_xl184_312")
union
select  'pra_exl_study_metric_xl184_312' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_metric_xl184_312" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_metric_xl184_312")
union
select  'pra_exl_study_milestone_xl184_312' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_milestone_xl184_312" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_milestone_xl184_312")
union
select  'pra_exl_study_site_monitor_xl184_312' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_monitor_xl184_312" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_monitor_xl184_312")
union
select  'pra_exl_study_site_xl184_312' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_xl184_312" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_xl184_312")
union
select  'pra_exl_study_subject_xl184_312' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_subject_xl184_312" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_subject_xl184_312")
union
select  'pra_exl_contact_association_xl184-315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_contact_association_xl184-315" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_contact_association_xl184-315")
union
select  'pra_exl_srm_enroll_proj_xl184-312' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_enroll_proj_xl184-312" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_enroll_proj_xl184-312")
union
select  'enroll_forecast_xl184-312' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."enroll_forecast_xl184-312" where last_modified_date = (select max(last_modified_date) from  koios_raw."enroll_forecast_xl184-312")
union
select  'pra_exl_account_association_xl184_315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_account_association_xl184_315" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_account_association_xl184_315")
union
select  'pra_exl_srm_proj_sites_xl184_312' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_proj_sites_xl184_312" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_proj_sites_xl184_312")
union
select  'pra_exl_srm_enroll_proj_xl184-315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_enroll_proj_xl184-315" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_enroll_proj_xl184-315")
union
select  'pra_exl_study_submission_xl184_315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_submission_xl184_315" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_submission_xl184_315")
union
select  'pra_exl_study_xl184_315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_xl184_315" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_xl184_315")
union
select  'pra_exl_study_country_xl184_315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_country_xl184_315" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_country_xl184_315")
union
select  'pra_exl_study_metric_xl184_315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_metric_xl184_315" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_metric_xl184_315")
union
select  'pra_exl_study_milestone_xl184_315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_milestone_xl184_315" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_milestone_xl184_315")
union
select  'pra_exl_study_site_monitor_xl184_315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_monitor_xl184_315" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_monitor_xl184_315")
union
select  'pra_exl_study_site_xl184_315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_xl184_315" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_xl184_315")
union
select  'pra_exl_study_subject_xl184_315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_subject_xl184_315" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_subject_xl184_315")
union
select  'xl092_002_earlyphase_subject_status' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092_002_earlyphase_subject_status" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092_002_earlyphase_subject_status")
union
select  'enroll_forecast_xl184-315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."enroll_forecast_xl184-315" where last_modified_date = (select max(last_modified_date) from  koios_raw."enroll_forecast_xl184-315")
union
select  'pra_exl_srm_proj_sites_xl184_315' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_proj_sites_xl184_315" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_proj_sites_xl184_315")
union
select  'xl092-002_subject_summary' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-002_subject_summary" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-002_subject_summary")
union
select  'xl092_002_earlyphase_subject_visit' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092_002_earlyphase_subject_visit" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092_002_earlyphase_subject_visit")
union
select  'xl092-002_query_detail' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-002_query_detail" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-002_query_detail")
union
select  'subject_summary_report_xl092_002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."subject_summary_report_xl092_002" where last_modified_date = (select max(last_modified_date) from  koios_raw."subject_summary_report_xl092_002")
union
select  'xl092-002_subject_summary_by_visit' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-002_subject_summary_by_visit" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-002_subject_summary_by_visit")
union
select  'xl092-304_overall_edc_study_metrics' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-304_overall_edc_study_metrics" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-304_overall_edc_study_metrics")
union
select  'subject_visit_summary_report_xb002_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."subject_visit_summary_report_xb002_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."subject_visit_summary_report_xb002_101")
union
select  'subject_visit_summary_report_xl092_002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."subject_visit_summary_report_xl092_002" where last_modified_date = (select max(last_modified_date) from  koios_raw."subject_visit_summary_report_xl092_002")
union
select  'xb002-101_missing_pages' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xb002-101_missing_pages" where last_modified_date = (select max(last_modified_date) from  koios_raw."xb002-101_missing_pages")
union
select  'exelixis_medpace_ctms_enrollmentprojection_xl184-313' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."exelixis_medpace_ctms_enrollmentprojection_xl184-313" where last_modified_date = (select max(last_modified_date) from  koios_raw."exelixis_medpace_ctms_enrollmentprojection_xl184-313")
union
select  'exelixis_medpace_ctms_milestone_xl184-313' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."exelixis_medpace_ctms_milestone_xl184-313" where last_modified_date = (select max(last_modified_date) from  koios_raw."exelixis_medpace_ctms_milestone_xl184-313")
union
select  'exelixis_medpace_ctms_monitoringvisit_xl184-313' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."exelixis_medpace_ctms_monitoringvisit_xl184-313" where last_modified_date = (select max(last_modified_date) from  koios_raw."exelixis_medpace_ctms_monitoringvisit_xl184-313")
union
select  'exelixis_medpace_ctms_organizationpersonnel_xl184-313' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."exelixis_medpace_ctms_organizationpersonnel_xl184-313" where last_modified_date = (select max(last_modified_date) from  koios_raw."exelixis_medpace_ctms_organizationpersonnel_xl184-313")
union
select  'exelixis_medpace_ctms_patient_xl184-313' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."exelixis_medpace_ctms_patient_xl184-313" where last_modified_date = (select max(last_modified_date) from  koios_raw."exelixis_medpace_ctms_patient_xl184-313")
union
select  'exelixis_medpace_ctms_patientvariable_xl184-313' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."exelixis_medpace_ctms_patientvariable_xl184-313" where last_modified_date = (select max(last_modified_date) from  koios_raw."exelixis_medpace_ctms_patientvariable_xl184-313")
union
select  'exelixis_medpace_ctms_patientvisit_xl184-313' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."exelixis_medpace_ctms_patientvisit_xl184-313" where last_modified_date = (select max(last_modified_date) from  koios_raw."exelixis_medpace_ctms_patientvisit_xl184-313")
union
select  'exelixis_medpace_ctms_protocoldeviation_xl184-313' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."exelixis_medpace_ctms_protocoldeviation_xl184-313" where last_modified_date = (select max(last_modified_date) from  koios_raw."exelixis_medpace_ctms_protocoldeviation_xl184-313")
union
select  'exelixis_medpace_ctms_site_xl184-313' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."exelixis_medpace_ctms_site_xl184-313" where last_modified_date = (select max(last_modified_date) from  koios_raw."exelixis_medpace_ctms_site_xl184-313")
union
select  'exelixis_medpace_ctms_sitecontract_xl184-313' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."exelixis_medpace_ctms_sitecontract_xl184-313" where last_modified_date = (select max(last_modified_date) from  koios_raw."exelixis_medpace_ctms_sitecontract_xl184-313")
union
select  'exelixis_medpace_ctms_sitepersonel_xl184-313' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."exelixis_medpace_ctms_sitepersonel_xl184-313" where last_modified_date = (select max(last_modified_date) from  koios_raw."exelixis_medpace_ctms_sitepersonel_xl184-313")
union
select  'xl092-001_query_detail' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-001_query_detail" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-001_query_detail")
union
select  'xl092-001_subject_summary' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-001_subject_summary" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-001_subject_summary")
union
select  'xl092-001_subject_summary_by_visit' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-001_subject_summary_by_visit" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-001_subject_summary_by_visit")
union
select  'xl092-001_overall_edc_study_metrics' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-001_overall_edc_study_metrics" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-001_overall_edc_study_metrics")
union
select  'pra_exl_account_association_xl114-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_account_association_xl114-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_account_association_xl114-101")
union
select  'pra_exl_contact_association_xl114-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_contact_association_xl114-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_contact_association_xl114-101")
union
select  'pra_exl_study_submission_xl114-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_submission_xl114-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_submission_xl114-101")
union
select  'pra_exl_srm_enroll_proj_xl114-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_enroll_proj_xl114-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_enroll_proj_xl114-101")
union
select  'pra_exl_srm_proj_sites_xl114_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_srm_proj_sites_xl114_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_srm_proj_sites_xl114_101")
union
select  'pra_exl_address_association_xl114-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_address_association_xl114-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_address_association_xl114-101")
union
select  'pra_exl_study_xl114-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_xl114-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_xl114-101")
union
select  'pra_exl_study_country_xl114-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_country_xl114-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_country_xl114-101")
union
select  'pra_exl_study_metric_xl114-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_metric_xl114-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_metric_xl114-101")
union
select  'pra_exl_study_milestone_xl114-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_milestone_xl114-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_milestone_xl114-101")
union
select  'pra_exl_study_site_monitor_xl114-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_monitor_xl114-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_monitor_xl114-101")
union
select  'pra_exl_study_site_xl114-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_site_xl114-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_site_xl114-101")
union
select  'pra_exl_study_subject_xl114-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."pra_exl_study_subject_xl114-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."pra_exl_study_subject_xl114-101")
union
select  'enroll_forecast_xl114-101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."enroll_forecast_xl114-101" where last_modified_date = (select max(last_modified_date) from  koios_raw."enroll_forecast_xl114-101")
union
select  'subject_summary_report_xl092-304' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."subject_summary_report_xl092-304" where last_modified_date = (select max(last_modified_date) from  koios_raw."subject_summary_report_xl092-304")
union
select  'xl092-002_missing_pages' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-002_missing_pages" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-002_missing_pages")
union
select  'site_contact_details_actual_xl092-304' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."site_contact_details_actual_xl092-304" where last_modified_date = (select max(last_modified_date) from  koios_raw."site_contact_details_actual_xl092-304")
union
select  'prancer_cohort_summary_report_xl092_002' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."prancer_cohort_summary_report_xl092_002" where last_modified_date = (select max(last_modified_date) from  koios_raw."prancer_cohort_summary_report_xl092_002")
union
select  'subject_visit_summary_report_xl092-304' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."subject_visit_summary_report_xl092-304" where last_modified_date = (select max(last_modified_date) from  koios_raw."subject_visit_summary_report_xl092-304")
union
select  'xl092-303_missing_pages' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-303_missing_pages" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-303_missing_pages")
union
select  'xl092-304_query_detail' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-304_query_detail" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-304_query_detail")
union
select  'manual_exl_study_xl092-009' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."manual_exl_study_xl092-009" where last_modified_date = (select max(last_modified_date) from  koios_raw."manual_exl_study_xl092-009")
union
select  'xb002-101_overall_edc_study_metrics' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xb002-101_overall_edc_study_metrics" where last_modified_date = (select max(last_modified_date) from  koios_raw."xb002-101_overall_edc_study_metrics")
union
select  'xl092-304_missing_pages' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-304_missing_pages" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-304_missing_pages")
union
select  'xb002-101_query_detail' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xb002-101_query_detail" where last_modified_date = (select max(last_modified_date) from  koios_raw."xb002-101_query_detail")
union
select  'site_contact_details_actual_xl092-009' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."site_contact_details_actual_xl092-009" where last_modified_date = (select max(last_modified_date) from  koios_raw."site_contact_details_actual_xl092-009")
union
select  'prancer_cohort_summary_report' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."prancer_cohort_summary_report" where last_modified_date = (select max(last_modified_date) from  koios_raw."prancer_cohort_summary_report")
union
select  'subject_summary_report_xl092_303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."subject_summary_report_xl092_303" where last_modified_date = (select max(last_modified_date) from  koios_raw."subject_summary_report_xl092_303")
union
select  'xb002-101_subject_summary' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xb002-101_subject_summary" where last_modified_date = (select max(last_modified_date) from  koios_raw."xb002-101_subject_summary")
union
select  'xl092-304_subject_summary_by_visit' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-304_subject_summary_by_visit" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-304_subject_summary_by_visit")
union
select  'subject_summary_report_xb002_101' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."subject_summary_report_xb002_101" where last_modified_date = (select max(last_modified_date) from  koios_raw."subject_summary_report_xb002_101")
union
select  'xb002-101_subject_summary_by_visit' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xb002-101_subject_summary_by_visit" where last_modified_date = (select max(last_modified_date) from  koios_raw."xb002-101_subject_summary_by_visit")
union
select  'cenduit_blinded_subject_visit_xl092-009' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."cenduit_blinded_subject_visit_xl092-009" where last_modified_date = (select max(last_modified_date) from  koios_raw."cenduit_blinded_subject_visit_xl092-009")
union
select  'xl092_304_query_detail' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092_304_query_detail" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092_304_query_detail")
union
select  'subject_visit_summary_report_xl092_303' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."subject_visit_summary_report_xl092_303" where last_modified_date = (select max(last_modified_date) from  koios_raw."subject_visit_summary_report_xl092_303")
union
select  'xb002_101_cohort_summary_report' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xb002_101_cohort_summary_report" where last_modified_date = (select max(last_modified_date) from  koios_raw."xb002_101_cohort_summary_report")
union
select  'cenduit_subject_detail_xl092-009' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."cenduit_subject_detail_xl092-009" where last_modified_date = (select max(last_modified_date) from  koios_raw."cenduit_subject_detail_xl092-009")
union
select  'site_contact_details_xl092-009' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."site_contact_details_xl092-009" where last_modified_date = (select max(last_modified_date) from  koios_raw."site_contact_details_xl092-009")
union
select  'xl092-009_missing_pages' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-009_missing_pages" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-009_missing_pages")
union
select  'xl092-009_overall_edc_study_metrics' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-009_overall_edc_study_metrics" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-009_overall_edc_study_metrics")
union
select  'xl092-009_query_detail' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-009_query_detail" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-009_query_detail")
union
select  'cohort_summary_mapping' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."cohort_summary_mapping" where last_modified_date = (select max(last_modified_date) from  koios_raw."cohort_summary_mapping")
union
select  'xl092-009_subject_summary' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-009_subject_summary" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-009_subject_summary")
union
select  'xl092-009_subject_summary_by_visit' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-009_subject_summary_by_visit" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-009_subject_summary_by_visit")
union
select  'xb002_101_earlyphase_subject_visit' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xb002_101_earlyphase_subject_visit" where last_modified_date = (select max(last_modified_date) from  koios_raw."xb002_101_earlyphase_subject_visit")
union
select  'xb002_101_earlyphase_subject_status' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xb002_101_earlyphase_subject_status" where last_modified_date = (select max(last_modified_date) from  koios_raw."xb002_101_earlyphase_subject_status")
union
select  'cenduit_blinded_subject_visit_xl092-305' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."cenduit_blinded_subject_visit_xl092-305" where last_modified_date = (select max(last_modified_date) from  koios_raw."cenduit_blinded_subject_visit_xl092-305")
union
select  'cenduit_subject_detail_xl092-305' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."cenduit_subject_detail_xl092-305" where last_modified_date = (select max(last_modified_date) from  koios_raw."cenduit_subject_detail_xl092-305")
union
select  'site_contact_details_actual_xl092-305' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."site_contact_details_actual_xl092-305" where last_modified_date = (select max(last_modified_date) from  koios_raw."site_contact_details_actual_xl092-305")
union
select  'site_contact_details_xl092-305' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."site_contact_details_xl092-305" where last_modified_date = (select max(last_modified_date) from  koios_raw."site_contact_details_xl092-305")
union
select  'xl092-305_missing_pages' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-305_missing_pages" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-305_missing_pages")
union
select  'xl092-305_overall_edc_study_metrics' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-305_overall_edc_study_metrics" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-305_overall_edc_study_metrics")
union
select  'xl092-305_query_detail' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-305_query_detail" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-305_query_detail")
union
select  'xl092-305_subject_summary' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-305_subject_summary" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-305_subject_summary")
union
select  'xl092-305_subject_summary_by_visit' as "Table Name" , count(*) as count, max(partition_date) as load_date, max(last_modified_date) as file_modified_date_s3, current_timestamp as counts_taken_as_of from  koios_raw."xl092-305_subject_summary_by_visit" where last_modified_date = (select max(last_modified_date) from  koios_raw."xl092-305_subject_summary_by_visit")
)
select * from raw_data_availability
