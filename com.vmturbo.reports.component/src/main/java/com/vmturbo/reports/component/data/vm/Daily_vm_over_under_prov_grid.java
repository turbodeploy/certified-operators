package com.vmturbo.reports.component.data.vm;

import javax.annotation.Nonnull;

import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataContext;
import com.vmturbo.sql.utils.DbException;

/**
 * Insert report data to vmtdb for Daily_vm_over_under_prov_grid template .
 */
public class Daily_vm_over_under_prov_grid extends GroupsGenerator implements ReportTemplate {

    public Daily_vm_over_under_prov_grid(@Nonnull final GroupGeneratorDelegate groupGeneratorDelegate) {
        super(groupGeneratorDelegate);
    }

    @Override
    public boolean generateData(@Nonnull final ReportsDataContext context) throws DbException {
        super.insertVMGroups(context);
        // TODO currently only stats under vm_groups table will show up
        return true;
    }
}
