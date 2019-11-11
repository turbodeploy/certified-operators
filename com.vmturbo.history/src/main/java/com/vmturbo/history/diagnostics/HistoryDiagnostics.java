package com.vmturbo.history.diagnostics;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import io.prometheus.client.CollectorRegistry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Cursor;
import org.jooq.Field;

import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagnosticsWriter;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.pojos.AggregationMetaData;
import com.vmturbo.history.schema.abstraction.tables.pojos.ApplPerformance;
import com.vmturbo.history.schema.abstraction.tables.records.ApplPerformanceRecord;

/**
 * Responsible for collecting additional diagnostics from the history component.
 *
 * <p>Unlike the other components, we don't dump the actual contents of the various stat tables.
 * It would be too expensive. As a result, there is no real "restore" for the diagnostics.
 */
public class HistoryDiagnostics {
    /**
     * File name for the aggregation performance. This is data about the last day of
     * aggregation operations and how well they performed.
     */
    private static final String AGGREGATION_PERFORMANCE = "AggregationPerformance.txt";

    /**
     * File name for the aggregation metadata - this is data about the duration and completion
     * time of the last aggregation for each latest table.
     */
    private static final String AGGREGATION_METADATA = "AggregationMetadata.txt";


    private static final Logger logger = LogManager.getLogger();

    private final Clock clock;

    private final DiagnosticsWriter diagnosticsWriter;

    private final HistorydbIO historydbIO;

    public HistoryDiagnostics(@Nonnull final Clock clock,
                              @Nonnull final HistorydbIO historydbIO,
                              @Nonnull final DiagnosticsWriter diagnosticsWriter) {
        this.clock = clock;
        this.historydbIO = historydbIO;
        this.diagnosticsWriter = diagnosticsWriter;
    }

    /**
     * Dump the diagnostics into a {@link ZipOutputStream}.
     *
     * @param zipOutputStream The target {@link ZipOutputStream}.
     */
    public void dump(@Nonnull final ZipOutputStream zipOutputStream) {

        List<String> diagErrors = Lists.newArrayList();

        try {
            diagnosticsWriter.writePrometheusMetrics(CollectorRegistry.defaultRegistry,
                zipOutputStream);
        } catch (DiagnosticsException e) {
            logger.error("Error writing prometheus metrics.", e);
            diagErrors.addAll(e.getErrors());
        }

        try {
            writeAggregationPerformance(zipOutputStream);
        } catch (DiagnosticsException e) {
            logger.error("Error writing aggregation performance.", e);
            diagErrors.addAll(e.getErrors());
        }

        try {
            writeAggregationMetadata(zipOutputStream);
        } catch (DiagnosticsException e) {
            logger.error("Error writing aggregation metadata.", e);
            diagErrors.addAll(e.getErrors());
        }

        if (!diagErrors.isEmpty()) {
            try {
                diagnosticsWriter.writeZipEntry("Diag Errors", diagErrors.stream(), zipOutputStream);
            } catch (DiagnosticsException e) {
                logger.error("Diagnostics errors: {}", diagErrors);
            }
        }
    }

    private void writeAggregationPerformance(@Nonnull final ZipOutputStream zipOutputStream)
        throws DiagnosticsException {
        try (Connection connection = historydbIO.connection()) {
            try (Cursor<ApplPerformanceRecord> cursor = historydbIO.using(connection)
                    .selectFrom(Tables.APPL_PERFORMANCE)
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

                // create a stream with the header (prefixBuilder) and performance info from the DB
                final Stream<String> performanceStream = Stream.concat(
                    Stream.of(prefixBuilder.toString()),
                    cursor.fetchInto(ApplPerformance.class)
                        .stream()
                        .map(ApplPerformance::toString));

                diagnosticsWriter.writeZipEntry(AGGREGATION_PERFORMANCE,
                    performanceStream, zipOutputStream);
            }
        } catch (RuntimeException | VmtDbException | SQLException e) {
            logger.error("Failed to write aggregation performance rows due to error: {}",
                e.getMessage());
        }
    }

    private void writeAggregationMetadata(@Nonnull final ZipOutputStream zipOutputStream) throws DiagnosticsException {
        try {

            final StringBuilder prefixBuilder = new StringBuilder().append("Fields : ");
            for (Field<?> field : Tables.AGGREGATION_META_DATA.fields()) {
                prefixBuilder.append(field.getName()).append(" , ");
            }
            prefixBuilder.append("\n---------------------\n");

            try (Connection connection = historydbIO.connection()) {
                Stream<String> metadataDumpStream = Stream.concat(
                    Stream.of("Current clock time: " + LocalDateTime.now(clock),
                        prefixBuilder.toString()),
                    historydbIO.using(connection)
                    .selectFrom(Tables.AGGREGATION_META_DATA)
                    .fetchInto(AggregationMetaData.class)
                    .stream()
                    .map(AggregationMetaData::toString));
                diagnosticsWriter.writeZipEntry(AGGREGATION_METADATA, metadataDumpStream,
                    zipOutputStream);
            }

        } catch (RuntimeException | VmtDbException | SQLException e) {
            logger.error("Failed to write aggregation metadata due to error.", e);
            throw new DiagnosticsException(e);
        }
    }

}
