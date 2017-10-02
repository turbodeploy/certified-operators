
do_host_reports    = StandardReport.host_feature_enabled?
do_storage_reports = StandardReport.storage_feature_enabled?
do_planner_reports = StandardReport.planner_feature_enabled?

if do_host_reports || do_storage_reports || do_planner_reports then
  exit_code = 1
else
  exit_code = 0
end

exit exit_code
