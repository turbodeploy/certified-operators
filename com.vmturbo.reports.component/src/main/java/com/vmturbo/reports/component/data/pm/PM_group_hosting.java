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
public class PM_group_hosting extends GroupsGenerator implements ReportTemplate {
    public PM_group_hosting(@Nonnull final GroupGeneratorDelegate delegate) {
        super(delegate);
    }

    @Override
    public Optional<String> generateData(@Nonnull final ReportsDataContext context,
                                         @Nonnull Optional<Long> selectedGroup) throws DbException {
        if (selectedGroup.isPresent()) {
            // this report requires an input parameter: "vm_group_name". It's used to retrieve member VMs.
            // Currently we hardcoded the "fake_vm_group" as group name.
            super.insertPMGroupAndVMRelationships(context, selectedGroup.get());
            return super.insertPMGroup(context, selectedGroup.get());
        }
        return Optional.empty();
    }
}
