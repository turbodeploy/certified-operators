package com.vmturbo.reports.component.data.pm;

import javax.annotation.Nonnull;

import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataContext;
import com.vmturbo.reports.component.data.vm.GroupsGenerator;
import com.vmturbo.sql.utils.DbException;

/**
 * Insert report data to vmtdb for Daily_cluster_30_days_avg_stats_vs_thresholds_grid template .
 */
public class Daily_cluster_30_days_avg_stats_vs_thresholds_grid extends GroupsGenerator implements ReportTemplate {

    public Daily_cluster_30_days_avg_stats_vs_thresholds_grid(@Nonnull final GroupGeneratorDelegate groupGeneratorDelegate) {
        super(groupGeneratorDelegate);
    }

    @Override
    public boolean generateData(@Nonnull final ReportsDataContext context) throws DbException {
        super.insertPMGroups(context);
        super.insertVMGroups(context);
        // TODO insert other missing data
        return true;
    }

}
