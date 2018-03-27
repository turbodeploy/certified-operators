package com.vmturbo.reports.component.entities;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.sql.utils.DbException;

/**
 * DAO to access entities information.
 */
public interface EntitiesDao {
    /**
     * Returns entity name by oid.
     *
     * @param oid oid to query
     * @return entity name, if found, {@link Optional#empty()} otherwise
     * @throws DbException if error occurred operating with DB.
     */
    @Nonnull
    Optional<String> getEntityName(long oid) throws DbException;
}
