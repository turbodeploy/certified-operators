package com.vmturbo.reports.component.data.pm;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.GroupsGenerator;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataContext;
import com.vmturbo.sql.utils.DbException;

/**
 * A template that XL supports but doesn't need special data generation.
 */
public class NullReportTemplate extends GroupsGenerator implements ReportTemplate {

    public NullReportTemplate(@Nonnull final GroupGeneratorDelegate groupGeneratorDelegate) {
        super(groupGeneratorDelegate);
    }

    @Override
    public Optional<String> generateData(@Nonnull final ReportsDataContext context,
                                         @Nonnull Optional<Long> selectedGroup) throws DbException {
        return Optional.empty();
    }
}
