package com.vmturbo.sql.utils.sizemon;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.google.common.base.Functions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * This class periodically collects size information for database tables and reports results via
 * prometheus metrics as well as logs. The logs will contain more detailed information if
 * available.
 */
public class DbSizeMonitor {
    private static final Logger logger = LogManager.getLogger();
    private final long freqMillis;
    private final long offsetMillis;
    private final Map<DbSizeReporter, Integer> reporters = new LinkedHashMap<>();
    private final ScheduledExecutorService executorService;
    private final Schema schema;
    private DataSource datasource;
    private SQLDialect dialect;
    private DbEndpoint endpoint;
    private DSLContext dsl;

    /**
     * Create a new monitoring instance.
     *
     * <p>The schedule frequency and offset are used to define a schedule for report generation
     * that includes the epoch-start time (`Instant.ofEpochMillis(0L)` = 1970-01-01 00:00:00), plus
     * the offset, and then repeating according ot the scheduling frequency. For example, specifying
     * a 1-day frequency and a 3-hour offset will result in report generation at 3am every day.</p>
     *
     * <p>Frequency and offset values should be in a form parsable by
     * {@link Duration#parse(CharSequence)}, e.g. "P1D" for 1 day, or "PT2M" for two minutes, or
     * "P1DT2H3M" for 1 day 2 hours 3 minutes.</p>
     *
     * @param endpoint        endpoint for database access
     * @param schema          schema to be handled
     * @param schedFrequency  schedule frequency
     * @param schedOffset     schedule offset
     * @param executorService service to execute scheduled monitor invocations
     */
    public DbSizeMonitor(DbEndpoint endpoint, Schema schema,
            String schedFrequency, String schedOffset, ScheduledExecutorService executorService) {
        this.endpoint = endpoint;
        this.schema = schema;
        this.freqMillis = Duration.parse(schedFrequency).toMillis();
        this.offsetMillis = Duration.parse(schedOffset).toMillis();
        this.executorService = executorService;
    }

    /**
     * Create a new monitoring instance.
     *
     * <p>The schedule frequency and offset are used to define a schedule for report generation
     * that includes the epoch-start time (`Instant.ofEpochMillis(0L)` = 1970-01-01 00:00:00), plus
     * the offset, and then repeating according ot the scheduling frequency. For example, specifying
     * a 1-day frequency and a 3-hour offset will result in report generation at 3am every day.</p>
     *
     * <p>Frequency and offset values should be in a form parsable by
     * {@link Duration#parse(CharSequence)}, e.g. "P1D" for 1 day, or "PT2M" for two minutes, or
     * "P1DT2H3M" for 1 day 2 hours 3 minutes.</p>
     *
     * @param dialect         {@link SQLDialect} value for this database
     * @param datasource      datasource to get connections
     * @param schema          schema to be handled
     * @param schedFrequency  schedule frequency
     * @param schedOffset     schedule offset
     * @param executorService service to execute scheduled monitor invocations
     */
    public DbSizeMonitor(SQLDialect dialect, DataSource datasource, Schema schema,
            String schedFrequency, String schedOffset, ScheduledExecutorService executorService) {
        this.dialect = dialect;
        this.datasource = datasource;
        this.schema = schema;
        this.freqMillis = Duration.parse(schedFrequency).toMillis();
        this.offsetMillis = Duration.parse(schedOffset).toMillis();
        this.executorService = executorService;
    }

    /**
     * Schedule this monitor for execution.
     *
     * <p>For {@link DbEndpoint}-based monitors, this should be executed after spring
     * configuration is completed.</p>
     *
     * @throws SQLException                if there's a problem setting up the DSLContext for this
     *                                     endpoint
     * @throws UnsupportedDialectException if the endpoint is creaed with an invalid dialect
     * @throws InterruptedException        if the thread is interrupted
     */
    public void activate() throws SQLException, UnsupportedDialectException, InterruptedException {
        this.dsl = endpoint != null ? endpoint.dslContext()
                : DSL.using(datasource, dialect);
        final long now = System.currentTimeMillis();
        final long millsToNext = freqMillis - (now % freqMillis) + offsetMillis;
        logger.info("Freq: {}, Offset: {}, TimeToNext: {}", freqMillis, offsetMillis, millsToNext);
        final Runnable runnable = () -> {
            try {
                runReport();
            } catch (Exception e) {
                logger.error("Failed to run size size report for schema {}", schema, e);
            }
        };
        executorService.scheduleAtFixedRate(runnable,
                millsToNext, freqMillis, TimeUnit.MILLISECONDS);
    }

    private DbSizeMonitor addReporter(final DbSizeReporter reporter, int frequency) {
        reporters.put(reporter, frequency);
        return this;
    }

    /**
     * Add a logging reporter to this monitor, to produce a size report in the logs at a given
     * frequency.
     *
     * <p>A given table is reported if it matches at least one of the `includes` patterns, and
     * none of the `excludes` patterns.</p>
     *
     * @param granularity granularity at which to report sizes
     * @param includes    comma-separated list of regular expressions for tables to be included or
     *                    null to include all tables
     * @param excludes    comma-separated list of regular expressions for tables to be excluded or
     *                    null to exclude no tables
     * @param frequency   frequency of log report, as a multiple of monitoring cycles (e.g. 2 for
     *                    every other cycle)
     * @return DbSizeMonitor instance, for further configuration
     */
    public DbSizeMonitor withLogging(
            Granularity granularity, String includes, String excludes, int frequency) {
        return addReporter(new LoggingReporter(logger,
                        schema, compilePatterns(includes), compilePatterns(excludes), granularity),
                frequency);
    }

    /**
     * Add a logging reporter to this monitor, to produce a size report in the logs at a given
     * frequency.
     *
     * <p>A given table is reported if it matches at least one of the `includes` patterns, and
     * none of the `excludes` patterns.</p>
     *
     * @param granularity granularity at which to report sizes
     * @param includes    comma-separated list of regular expressions for tables to be included or
     *                    null to include all tables
     * @param excludes    comma-separated list of regular expressions for tables to be excluded or
     *                    null to exclude no tables
     * @param frequency   frequency of log report, as a multiple of monitoring cycles (e.g. 2 for
     *                    every other cycle)
     * @return DbSizeMonitor instance, for further configuration
     */
    public DbSizeMonitor withPersisting(
            Granularity granularity, String includes, String excludes, int frequency) {
        return addReporter(new PersistingReporter(() -> dsl,
                        schema, compilePatterns(includes), compilePatterns(excludes), granularity),
                frequency);
    }

    private Map<Table<?>, DbSizeReport> createReport() {
        final DbSizeAdapter adapter = DbSizeAdapter.of(dsl, schema);
        return adapter.getTables().stream()
                .collect(Collectors.toMap(Functions.identity(), t -> new DbSizeReport(t, adapter)));
    }

    private void runReport() {
        final List<DbSizeReport> reports = createReport().entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getKey().getName()))
                .map(Entry::getValue)
                .collect(Collectors.toList());
        long runNumber = (System.currentTimeMillis() - offsetMillis) / freqMillis;
        reporters.forEach((reporter, frequency) -> {
            if (runNumber % frequency == 0) {
                reporter.process(reports);
            }
        });
    }

    private static Set<Pattern> compilePatterns(final String patterns) {
        if (patterns == null) {
            return null;
        } else {
            return Arrays.stream(patterns.split(","))
                    .map(String::trim)
                    .map(Pattern::compile)
                    .collect(Collectors.toSet());
        }
    }

    /**
     * Reporting granularity for size info.
     */
    public enum Granularity {
        // these should appear in order of coarser-to-finer granularity
        /** report total size of schema. */
        SCHEMA,
        /** report sizes for individual tables. */
        TABLE,
        /** report per-partition(/chunk/whatever) size info for tables that have them. */
        PARTITION
    }

    /**
     * Size report for a given table.
     */
    public static class DbSizeReport {
        private final Table<?> table;
        private final List<SizeItem> sizeItems;
        private final long totalBytes;

        /**
         * Create a new instance.
         *
         * @param table   table to be reported
         * @param adapter adapter that can be used to obtain size info
         */
        public DbSizeReport(final Table<?> table, final DbSizeAdapter adapter) {
            this.table = table;
            this.sizeItems = adapter.getSizeItems(table);
            this.totalBytes = sizeItems.stream()
                    .filter(i -> i.getGranularity() == Granularity.TABLE)
                    .mapToLong(SizeItem::getSize).sum();
        }

        public Table<?> getTable() {
            return table;
        }

        /**
         * Get the size items for this report, limited to those whose granularity is no greater than
         * the given max.
         *
         * @param maxGranularity granularity limit for returned items
         * @return size items
         */
        public List<SizeItem> getSizeItems(Granularity maxGranularity) {
            return sizeItems.stream()
                    .filter(si -> si.getGranularity().ordinal() <= maxGranularity.ordinal())
                    .collect(Collectors.toList());
        }

        public long getTotalBytes() {
            return totalBytes;
        }
    }
}
