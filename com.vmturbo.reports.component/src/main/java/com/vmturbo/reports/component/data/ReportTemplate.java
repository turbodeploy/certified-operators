package com.vmturbo.reports.component.data;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.sql.utils.DbException;

/**
 * Interface for generating data to vmtdb to support classic reports.
 */
public interface ReportTemplate {
    /**
     * Generate data to vmtdb.
     *
     * @param context context to help connect to other components and interact with vmtdb.
     * @return report generated group id for on-demand reports.
     * @throws DbException when db exception encountered.
     */
  Optional<String> generateData(@Nonnull final ReportsDataContext context,
                                  @Nonnull Optional<Long> selectedGroup) throws DbException;
}
