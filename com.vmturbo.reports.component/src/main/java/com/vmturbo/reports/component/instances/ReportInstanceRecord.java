package com.vmturbo.reports.component.instances;

import com.vmturbo.sql.utils.DbException;

/**
 * DB record for the newly created report instance. It is initially created in dirty state, so
 * cannot be retrieved by ordinal calls. Call to {@link #commit()} make the record visible to users.
 * Call to {@link #rollback()} removes the dirty record from the db.
 */
public interface ReportInstanceRecord {

    /**
     * Returns report instance id.
     *
     * @return record id
     */
    long getId();

    /**
     * Makes the record publicly visible.
     *
     * @throws DbException if exception thrown while DB manipulcations
     */
    void commit() throws DbException;

    /**
     * Rolls back the record, so it is no longer stored in the database.
     *
     * @throws DbException if exception thrown while DB manipulcations
     */
    void rollback() throws DbException;
}
