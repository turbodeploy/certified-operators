package com.vmturbo.reports.component.data.vm;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.GroupsGenerator;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataContext;
import com.vmturbo.sql.utils.DbException;

public class VM_group_rightsizing_advice_grid extends GroupsGenerator implements ReportTemplate {
    public VM_group_rightsizing_advice_grid(@Nonnull final GroupGeneratorDelegate delegate) {
        super(delegate);
    }

    @Override
    public Optional<String> generateData(@Nonnull final ReportsDataContext context,
                                         @Nonnull Optional<Long> selectedGroup) throws DbException {
        if (selectedGroup.isPresent()) {
            (new Daily_vm_rightsizing_advice_grid(super.generatorDelegate))
                .populateRightSize(context);
            return super.insertVMGroup(context, selectedGroup.get());
        }
        return Optional.empty();
    }
}
