from django.urls import path
from . import views

app_name = "bankruptcy"

urlpatterns = [
    path("", views.index, name="index"),
    path("cases/", views.case_list, name="case_list"),
    path("case/<int:case_id>/", views.case_detail, name="case_detail"),
    path("creditors/", views.creditors_list, name="creditors_list"),
    path("creditors-old/", views.creditor_statistics, name="creditor_statistics"),
    path("court-decisions/", views.court_decisions_dashboard, name="court_decisions_dashboard"),
    path("system-control/", views.system_control_panel, name="system_control_panel"),
    path("api/court-decisions/stats/", views.court_decisions_stats_api, name="court_decisions_stats_api"),
    path("api/court-decisions/search/", views.court_decisions_search_api, name="court_decisions_search_api"),
    path("api/court-decisions/hierarchical-stats/", views.hierarchical_court_decisions_stats_api, name="hierarchical_court_decisions_stats_api"),
    path("api/court-decisions/list/", views.court_decisions_list_api, name="court_decisions_list_api"),
    path("api/court-decisions/courts/", views.courts_list_api, name="courts_list_api"),
    path("api/monitoring-stats/", views.monitoring_stats_api, name="monitoring_stats_api"),
    path("api/start-forced-process/", views.start_forced_process, name="start_forced_process"),
    path("api/stop-forced-process/", views.stop_forced_process, name="stop_forced_process"),
    path("api/process-status/", views.process_status_api, name="process_status_api"),
    path("api/start-incremental-extraction/", views.start_incremental_extraction, name="start_incremental_extraction"),
    path("api/start-small-batch-extraction/", views.start_small_batch_extraction, name="start_small_batch_extraction"),
    path("api/start-ultra-fast-extraction/", views.start_ultra_fast_extraction, name="start_ultra_fast_extraction"),
    path("api/extraction-stats/", views.extraction_stats_api, name="extraction_stats_api"),
    path("api/start-creditor-extraction/", views.start_creditor_extraction, name="start_creditor_extraction"),
    path("api/stop-creditor-extraction/", views.stop_creditor_extraction, name="stop_creditor_extraction"),
    path("api/start-deduplication-process/", views.start_deduplication_process, name="start_deduplication_process"),
    path("api/stop-deduplication-process/", views.stop_deduplication_process, name="stop_deduplication_process"),
    path("api/deduplication-stats/", views.deduplication_stats_api, name="deduplication_stats_api"),
    path("deduplication-detail/", views.deduplication_detail, name="deduplication_detail"),
]