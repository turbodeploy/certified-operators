package com.vmturbo.reports.component.data.pm;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.GroupsGenerator;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataContext;
import com.vmturbo.sql.utils.DbException;

/**
 * Insert report data to vmtdb for Daily_pm_vm_utilization_heatmap_grid template.
 * This template doesn't need to generate data, skip registering it.
 */
public class Daily_pm_vm_utilization_heatmap_grid extends GroupsGenerator implements ReportTemplate {

    public Daily_pm_vm_utilization_heatmap_grid(@Nonnull final GroupGeneratorDelegate groupGeneratorDelegate) {
        super(groupGeneratorDelegate);
    }

    @Override
    public Optional<String> generateData(@Nonnull final ReportsDataContext context,
                                         @Nonnull Optional<Long> selectedGroup) throws DbException {
        return Optional.empty();
    }

}
