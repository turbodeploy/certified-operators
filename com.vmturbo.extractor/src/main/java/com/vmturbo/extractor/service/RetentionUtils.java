package com.vmturbo.extractor.service;

import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.exception.DataAccessException;

import com.vmturbo.extractor.schema.Extractor;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.sizemon.DbSizeAdapter;
import com.vmturbo.sql.utils.sizemon.PostgresSizeAdapter;

/**
 * Helper functions to set retention policy setting in extractor.
 */
public class RetentionUtils {
    /**
     * Private constructor for util class.
     */
    private RetentionUtils() {}

    /**
     * Sets the retention period for a particular hyper table.
     *
     * @param table Name of the hyper table.
     * @param endpoint DB Endpoint.
     * @param retentionDays Days to set retention to.
     * @throws InterruptedException When thread is interrupted.
     * @throws UnsupportedDialectException thrown on unknown SQL dialect.
     * @throws SQLException on SQL errors.
     * @throws DataAccessException thrown by JooQ.
     */
    static void updateRetentionPeriod(@Nonnull final String table, @Nonnull final DbEndpoint endpoint,
            int retentionDays) throws InterruptedException, UnsupportedDialectException,
            SQLException, DataAccessException {
        endpoint.getAdapter().setupRetentionPolicy(table, ChronoUnit.DAYS, retentionDays);
    }

    /**
     * Gets names of hyper tables.
     *
     * @param endpoint DB Endpoint.
     * @return Set of hyper table names, can be empty if not PG DB.
     * @throws InterruptedException When thread is interrupted.
     * @throws UnsupportedDialectException thrown on unknown SQL dialect.
     * @throws SQLException on SQL errors.
     * @throws DataAccessException thrown by JooQ.
     */
    @Nonnull
    static Set<String> getHypertables(@Nonnull final DbEndpoint endpoint)
            throws InterruptedException, UnsupportedDialectException, SQLException,
            DataAccessException {
        final DbSizeAdapter adapter = DbSizeAdapter.of(endpoint.dslContext(), Extractor.EXTRACTOR);
        if (!(adapter instanceof PostgresSizeAdapter)) {
            return Collections.emptySet();
        }
        final PostgresSizeAdapter pgAdapter = (PostgresSizeAdapter)adapter;
        return pgAdapter.getHypertables();
    }
}
