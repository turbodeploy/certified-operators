package com.vmturbo.reports.component.data.pm;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.GroupsGenerator;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataContext;
import com.vmturbo.sql.utils.DbException;

/**
 * For Host/VM Group - Resource Utilization Heatmap
 */
public class PM_group_daily_pm_vm_utilization_heatmap_grid extends GroupsGenerator implements ReportTemplate {
    public PM_group_daily_pm_vm_utilization_heatmap_grid(@Nonnull final GroupGeneratorDelegate delegate) {
        super(delegate);
    }

    @Override
    public Optional<String> generateData(@Nonnull final ReportsDataContext context,
                                         @Nonnull Optional<Long> selectedGroup) throws DbException {
        if (selectedGroup.isPresent()) {
            // we hardcoded "fake_vm_group", so don' need to return name.
            super.insertPMGroupAndVMRelationships(context, selectedGroup.get());
            return super.insertPMGroup(context, selectedGroup.get());
        }
        return Optional.empty();
    }
}