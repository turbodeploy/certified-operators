package com.vmturbo.history.diagnostics;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Cursor;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.SelectSeekStep1;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.StringDiagnosable;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.pojos.ApplPerformance;
import com.vmturbo.history.schema.abstraction.tables.records.ApplPerformanceRecord;

/**
 * History diagnostics for aggregation performance.
 */
public class AggregationPerformanceDiagnostics implements StringDiagnosable {

    /**
     * File name for the aggregation performance. This is data about the last day of
     * aggregation operations and how well they performed.
     */
    private static final String AGGREGATION_PERFORMANCE = "AggregationPerformance.txt";

    private static final Logger logger = LogManager.getLogger();

    private final Clock clock;
    private final DSLContext dsl;

    /**
     * Constructs aggregated performance diagnostics.
     *
     * @param clock clock to use
     * @param dsl history DAO
     */
    public AggregationPerformanceDiagnostics(@Nonnull Clock clock, @Nonnull DSLContext dsl) {
        this.clock = Objects.requireNonNull(clock);
        this.dsl = dsl;
    }

    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender)
            throws DiagnosticsException {
        try {
            dsl.connection(conn -> {
                // postgres requires autocommit to be off for streaming to work
                conn.setAutoCommit(false);
                SelectSeekStep1<ApplPerformanceRecord, Timestamp> query = DSL.using(conn)
                        .selectFrom(Tables.APPL_PERFORMANCE)
                        .where(Tables.APPL_PERFORMANCE.START_TIME.gt(
                                Timestamp.from(clock.instant().minus(1, ChronoUnit.DAYS))))
                        // Latest start time first.
                        .orderBy(Tables.APPL_PERFORMANCE.START_TIME.desc());
                try (Cursor<ApplPerformanceRecord> cursor = query
                        // different required sizes for streaming in mariadb vs postgres
                        .fetchSize(dsl.dialect() == SQLDialect.POSTGRES ? 1000 : Integer.MIN_VALUE)
                        .fetchLazy()) {
                    final StringBuilder prefixBuilder = new StringBuilder().append("Fields : ");
                    for (Field<?> field : Tables.APPL_PERFORMANCE.fields()) {
                        prefixBuilder.append(field.getName()).append(" , ");
                    }
                    prefixBuilder.append("\n---------------------\n");
                    appender.appendString(prefixBuilder.toString());

                    // create a stream with the header (prefixBuilder) and performance info from the DB
                    for (ApplPerformance applPerformance : cursor.fetchInto(
                            ApplPerformance.class)) {
                        appender.appendString(applPerformance.toString());
                    }
                }
            });
        } catch (DataAccessException e) {
            logger.error("Failed to write aggregation performance rows due to error: {}",
                    e.getMessage());
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return AGGREGATION_PERFORMANCE;
    }
}
