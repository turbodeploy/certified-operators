package com.vmturbo.reports.component.data.pm;

import javax.annotation.Nonnull;

import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataContext;
import com.vmturbo.reports.component.data.vm.GroupsGenerator;
import com.vmturbo.sql.utils.DbException;

/**
 * Insert report data to vmtdb for Monthly_cluster_summary template .
 */
public class Monthly_cluster_summary extends GroupsGenerator implements ReportTemplate {

    public Monthly_cluster_summary(@Nonnull final GroupGeneratorDelegate groupGeneratorDelegate) {
        super(groupGeneratorDelegate);
    }

    @Override
    public boolean generateData(@Nonnull final ReportsDataContext context) throws DbException {
        super.insertPMGroups(context);
        // TODO insert other missing data
        return true;
    }

}
