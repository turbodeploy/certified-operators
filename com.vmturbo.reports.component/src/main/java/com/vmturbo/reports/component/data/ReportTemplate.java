package com.vmturbo.reports.component.data;

import com.vmturbo.sql.utils.DbException;

/**
 * Interface for generating data to vmtdb to support classic reports.
 */
public interface ReportTemplate {
    /**
     * Generate data to vmtdb.
     *
     * @param context context to help connect to other components and interact with vmtdb.
     * @return true if missing data are generated successfully.
     */
    boolean generateData(final ReportsDataContext context) throws DbException;
}
