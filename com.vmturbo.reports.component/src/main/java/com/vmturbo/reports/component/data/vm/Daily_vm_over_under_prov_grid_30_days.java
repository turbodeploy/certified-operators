package com.vmturbo.reports.component.data.vm;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.GroupsGenerator;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataContext;
import com.vmturbo.sql.utils.DbException;

/**
 * Insert report data to vmtdb for Daily_vm_over_under_prov_grid_30_days template .
 */
public class Daily_vm_over_under_prov_grid_30_days extends GroupsGenerator implements ReportTemplate {

    public Daily_vm_over_under_prov_grid_30_days(@Nonnull final GroupGeneratorDelegate groupGeneratorDelegate) {
        super(groupGeneratorDelegate);
    }

    @Override
    public Optional<String> generateData(@Nonnull final ReportsDataContext context,
                                         @Nonnull Optional<Long> selectedGroup) throws DbException {
        super.insertVMGroups(context);
        // TODO currently only stats under vm_groups table will show up
        return Optional.empty();
    }
}
