# src/gold/models/__init__.py
# =============================================================================
# Gold layer model registry.
#
# Every class listed here corresponds to one Gold object in
# common.gold_object_registry. The engine imports models via importlib
# using the class_path field in the registry, so this file is NOT required
# for runtime — it exists for discoverability and type-checking.
#
# Gold objects are split into two implementation styles:
#
#   SQL-based (strategy: FULL_REFRESH / VIEW_DDL / INCREMENTAL_APPEND)
#   ─────────────────────────────────────────────────────────────────────
#   The gold_object_registry sql_template drives execution.
#   Python model files hold class metadata only — no build() logic.
#   To change output: edit config/gold/gold_objects.yaml, re-run seed script.
#
#   PySpark-based (strategy: PYSPARK_MODEL)
#   ────────────────────────────────────────
#   Python model files hold full build() logic with DataFrame API.
#   These objects join common.* master tables (Option B — no study_id grain).
#   Master tables must be seeded by notebooks/onboarding/populate_*_master.py
#   BEFORE the Gold pipeline runs.
#
# Objects promoted to PYSPARK_MODEL:
#   • dim_participant            — joins participant_status_master (Option B)
#   • fct_study_site             — joins site_status_master (group_name drives boolean site flags)
#   • gold_v_cohort_enrollment_trend — replaces hardcoded study IN-list
# =============================================================================

from .dim_calendar import DimCalendarModel
from .dim_study import DimStudyModel
from .dim_study_country import DimStudyCountryModel
from .dim_study_account import DimStudyAccountModel
from .dim_study_address import DimStudyAddressModel
from .dim_study_contact import DimStudyContactModel
from .dim_missing_pages import DimMissingPagesModel
from .dim_site_address import DimSiteAddressModel
from .dim_queries import DimQueriesModel
from .dim_cohort_summary import DimCohortSummaryModel
from .dim_planisware_milestones import DimPlaniswareMilestonesModel
from .dim_planisware_daily_hist import DimPlaniswareDailyHistModel
from .dim_qc_report import DimQcReportModel
from .dim_participant import DimParticipantModel
from .site_info import SiteInfoModel
from .enrolled_participants import EnrolledParticipantsModel
from .final_monthly_screening_rate import FinalMonthlyScreeningRateModel
from .open_queries import OpenQueriesModel
from .query_resolution_time import QueryResolutionTimeModel
from .screen_failure_percentage import ScreenFailurePercentageModel
from .site_dates import SiteDatesModel
from .site_study import SiteStudyModel
from .dim_cc_report import DimCcReportModel
from .fct_study_milestone import FctStudyMilestoneModel
from .fct_participant_milestone import FctParticipantMilestoneModel
from .fct_site_milestone import FctSiteMilestoneModel
from .fct_queries import FctQueriesModel
from .fct_study_site import FctStudySiteModel
from .fct_cohort_details import FctCohortDetailsModel
from .fct_participant_visit import FctParticipantVisitModel
from .fct_study_country import FctStudyCountryModel
from .vw_participant_milestone import VwParticipantMilestoneModel
from .vw_site_milestone import VwSiteMilestoneModel
from .vw_study_milestone import VwStudyMilestoneModel
from .gold_v_cohort_enrollment_trend import GoldVCohortEnrollmentTrendModel
from .gold_v_cohort_summary import GoldVCohortSummaryModel
from .gold_v_cohort_summary_dashboard import GoldVCohortSummaryDashboardModel
from .gold_v_country_dashboard import GoldVCountryDashboardModel
from .gold_v_country_dashboard_custom import GoldVCountryDashboardCustomModel
from .gold_v_global_site_activation_trend import GoldVGlobalSiteActivationTrendModel
from .gold_v_drill_down_site_info import GoldVDrillDownSiteInfoModel

__all__ = [
    "DimCalendarModel",
    "DimStudyModel",
    "DimStudyCountryModel",
    "DimStudyAccountModel",
    "DimStudyAddressModel",
    "DimStudyContactModel",
    "DimMissingPagesModel",
    "DimSiteAddressModel",
    "DimQueriesModel",
    "DimCohortSummaryModel",
    "DimPlaniswareMilestonesModel",
    "DimPlaniswareDailyHistModel",
    "DimQcReportModel",
    "DimParticipantModel",
    "SiteInfoModel",
    "EnrolledParticipantsModel",
    "FinalMonthlyScreeningRateModel",
    "OpenQueriesModel",
    "QueryResolutionTimeModel",
    "ScreenFailurePercentageModel",
    "SiteDatesModel",
    "SiteStudyModel",
    "DimCcReportModel",
    "FctStudyMilestoneModel",
    "FctParticipantMilestoneModel",
    "FctSiteMilestoneModel",
    "FctQueriesModel",
    "FctStudySiteModel",
    "FctCohortDetailsModel",
    "FctParticipantVisitModel",
    "FctStudyCountryModel",
    "VwParticipantMilestoneModel",
    "VwSiteMilestoneModel",
    "VwStudyMilestoneModel",
    "GoldVCohortEnrollmentTrendModel",
    "GoldVCohortSummaryModel",
    "GoldVCohortSummaryDashboardModel",
    "GoldVCountryDashboardModel",
    "GoldVCountryDashboardCustomModel",
    "GoldVGlobalSiteActivationTrendModel",
    "GoldVDrillDownSiteInfoModel",
]
