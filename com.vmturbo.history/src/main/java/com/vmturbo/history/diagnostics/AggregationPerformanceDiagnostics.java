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
import org.jooq.exception.DataAccessException;

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
            try (Cursor<ApplPerformanceRecord> cursor = dsl.selectFrom(Tables.APPL_PERFORMANCE)
                    .where(Tables.APPL_PERFORMANCE.START_TIME.gt(
                            Timestamp.from(clock.instant().minus(1, ChronoUnit.DAYS))))
                    // Latest start time first.
                    .orderBy(Tables.APPL_PERFORMANCE.START_TIME.desc())
                    .fetchSize(Integer.MIN_VALUE)
                    .fetchLazy()) {
                final StringBuilder prefixBuilder = new StringBuilder().append("Fields : ");
                for (Field<?> field : Tables.APPL_PERFORMANCE.fields()) {
                    prefixBuilder.append(field.getName()).append(" , ");
                }
                prefixBuilder.append("\n---------------------\n");
                appender.appendString(prefixBuilder.toString());

                // create a stream with the header (prefixBuilder) and performance info from the DB
                for (ApplPerformance applPerformance : cursor.fetchInto(ApplPerformance.class)) {
                    appender.appendString(applPerformance.toString());
                }
            }
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
