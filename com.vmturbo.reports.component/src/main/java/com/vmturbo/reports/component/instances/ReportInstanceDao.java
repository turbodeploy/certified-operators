package com.vmturbo.reports.component.instances;

import javax.annotation.Nonnull;

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
     * @param reportTemplateId template id to use
     * @return dirty record representation
     * @throws DbException if exception thrown while DB manipulcations
     */
    @Nonnull
    ReportInstanceRecord createInstanceRecord(int reportTemplateId) throws DbException;
}
