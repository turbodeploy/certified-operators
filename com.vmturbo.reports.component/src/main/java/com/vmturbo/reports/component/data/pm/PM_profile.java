package com.vmturbo.reports.component.data.pm;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.GroupsGenerator;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataContext;
import com.vmturbo.sql.utils.DbException;

/**
 * For Host Group Summary on-demand report
 */
public class PM_profile extends GroupsGenerator implements ReportTemplate {
    public PM_profile(@Nonnull final GroupGeneratorDelegate delegate) {
        super(delegate);
    }

    @Override
    public Optional<String> generateData(@Nonnull final ReportsDataContext context,
                                         @Nonnull Optional<Long> selectedGroup) throws DbException {
        if (selectedGroup.isPresent()) {
            super.insertPMGroups(context);
            super.insertPMVMsRelationships(context, selectedGroup.get());
            return Optional.empty();
        }
        return Optional.empty();
    }
}