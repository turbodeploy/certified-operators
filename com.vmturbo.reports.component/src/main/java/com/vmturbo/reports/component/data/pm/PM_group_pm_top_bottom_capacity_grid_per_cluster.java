package com.vmturbo.reports.component.data.pm;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.GroupsGenerator;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataContext;
import com.vmturbo.sql.utils.DbException;

/**
 * For Host Top Bottom Capacity Grid Per Cluster
 */
public class PM_group_pm_top_bottom_capacity_grid_per_cluster extends GroupsGenerator implements ReportTemplate {
    public PM_group_pm_top_bottom_capacity_grid_per_cluster(@Nonnull final GroupGeneratorDelegate delegate) {
        super(delegate);
    }

    @Override
    public Optional<String> generateData(@Nonnull final ReportsDataContext context,
                                         @Nonnull Optional<Long> selectedGroup) throws DbException {
        if (selectedGroup.isPresent()) {
            return super.insertPMGroup(context, selectedGroup.get());
        }
        return Optional.empty();
    }
}
