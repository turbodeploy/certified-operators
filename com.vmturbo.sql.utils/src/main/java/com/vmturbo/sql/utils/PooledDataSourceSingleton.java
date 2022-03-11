package com.vmturbo.sql.utils;

import java.sql.SQLException;

import javax.annotation.Nonnull;
import javax.inject.Singleton;
import javax.sql.DataSource;

import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Singleton for pooled data source.
 */
@Singleton
public class PooledDataSourceSingleton {

    private static DataSource instance;

    /**
     * PooledDataSourceSingleton constructor.
     */
    private PooledDataSourceSingleton() {}

    /**
     * Create a pooled data source.
     *
     * @param dbAdapter the dbAdapter to help create the datasource.
     * @return pooled data source
     * @throws SQLException if the datasource cannot be created.
     * @throws UnsupportedDialectException if unsupported dialect.
     */
    public static synchronized DataSource getInstance(@Nonnull DbAdapter dbAdapter)
            throws SQLException, UnsupportedDialectException {
        if (instance == null) {
            instance = dbAdapter.createDataSource(true);
        }
        return instance;
    }
}
