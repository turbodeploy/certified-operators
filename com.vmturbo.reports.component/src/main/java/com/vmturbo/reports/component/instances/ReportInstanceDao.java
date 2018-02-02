package com.vmturbo.reports.component.instances;

import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.reports.component.db.tables.pojos.ReportInstance;
import com.vmturbo.sql.utils.DbException;

/**
 * Reports instance data access object. This DAO does not affect real reports files on the storage.
 * It is only manipulating DB records.
 */
public interface ReportInstanceDao {

    /**
     * Creates a new report record in the DB from the specified report template and returns the
     * newly created reporting instance id. Report instance is created as a dirty record in the
     * database and should be committed in order to make it visible to the users.
     *
     * @param reportType report type to query instances for
     * @param reportTemplateId template id to use
     * @param format format of the report instance to create
     * @return dirty record representation
     * @throws DbException if exception thrown while DB manipulations
     */
    @Nonnull
    ReportInstanceRecord createInstanceRecord(@Nonnull ReportType reportType, int reportTemplateId,
            @Nonnull ReportOutputFormat format) throws DbException;

    /**
     * Returns report instance record, if any. Only retrieves clean (committed) records.
     *
     * @param reportInstanceId report instance id to retrieve
     * @return report instance record or empty Optional
     * @throws DbException if DB exception occurred.
     */
    @Nonnull
    Optional<ReportInstance> getInstanceRecord(long reportInstanceId) throws DbException;

    /**
     * Retrieves all the report instance, that has been successfully generated. Dirty (not
     * committed) report instances are not fetched within this call.
     *
     * @return collection of report instances
     * @throws DbException if DB exception occurred.
     */
    @Nonnull
    Collection<ReportInstance> getAllInstances() throws DbException;

    /**
     * Retrieves all the finished (clean) report instances, referring to the specified template.
     *
     * @param reportType report type to query instances for
     * @param templateId template Id to fetch report instances for
     * @return collection of report instances
     * @throws DbException if DB exception occurred.
     */
    @Nonnull
    Collection<ReportInstance> getInstancesByTemplate(@Nonnull ReportType reportType,
            int templateId) throws DbException;
}
