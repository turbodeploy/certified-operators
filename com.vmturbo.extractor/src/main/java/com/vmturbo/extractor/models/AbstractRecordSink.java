package com.vmturbo.extractor.models;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.tools.StringUtils;

import com.vmturbo.components.common.utils.ThrowingConsumer;
import com.vmturbo.extractor.models.Table.Record;

/**
 * Base class for record consumers with pre/post execution capabilities.
 */
public abstract class AbstractRecordSink implements ThrowingConsumer<Record, SQLException> {
    protected static final Logger logger = LogManager.getLogger();
    private static final int MAX_SQL_LOG_LENGTH = 1000;

    protected final DSLContext dsl;

    /**
     * Create a new record sink.
     *
     * @param dsl    jOOQ DSL Context that can be used for the COPY operation
     */
    public AbstractRecordSink(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Execute a series of commands with logging.
     *
     * @param conn connection
     * @param hook list of sqls
     * @param description description for log purposes
     * @throws SQLException when failed
     */
    protected static void runHook(Connection conn, List<String> hook, String description) throws SQLException {
        for (final String sql : hook) {
            logger.info("Executing {} hook SQL: {}",
                    description, StringUtils.abbreviate(sql, MAX_SQL_LOG_LENGTH));
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.execute();
            }
        }
    }

    /**
     * Compose sink-specific SQL and perform other operations required before beginning the writing.
     *
     * <p>Throwing an exception will cause the transaction to  roll back.</p>
     *
     * <p>Executing the SQL within this statement is perfectly suitable. The only downside is that
     * built-in logging of the SQL will not occur.</p>
     *
     * @param transConn database connection on which COPY will execute
     * @return list of SQL statements to execute in order
     * @throws SQLException if there's a DB error
     */
    protected List<String> getPreCopyHookSql(final Connection transConn) throws SQLException {
        return Collections.emptyList();
    }

    /**
     * Compose sink-specific SQL and perform other operations required after the data writing
     * has completed.
     *
     * <p>Throwing an exception will cause the transaction to  roll back.</p>
     *
     * <p>Executing the SQL within this statement is perfectly suitable. The only downside is that
     * built-in logging of the SQL will not occur.</p>
     *
     * @param transConn database connection on which COPY operation executed (still open)
     * @return SQL statements to execute
     * @throws SQLException if there's a problem
     */
    protected List<String> getPostCopyHookSql(final Connection transConn) throws SQLException {
        return Collections.emptyList();
    }
}
