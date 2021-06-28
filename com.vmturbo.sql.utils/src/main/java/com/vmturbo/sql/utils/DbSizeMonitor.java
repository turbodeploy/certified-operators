package com.vmturbo.sql.utils;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Functions;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Schema;
import org.jooq.Table;

import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * This class periodically collects size information for database tables and reports results via
 * prometheus metrics as well as logs. The logs will contain more detailed information if
 * available.
 */
public class DbSizeMonitor {
    private static final Logger logger = LogManager.getLogger();

    private DbSizeAdapter adapter;
    private final Set<Pattern> includes;
    private final Set<Pattern> excludes;
    private DbEndpoint endpoint;
    private Schema schema;
    private Set<Pattern> includes1;
    private Set<Pattern> excludes1;
    private Granularity granularity;
    private Logger logger1;
    private ScheduledExecutorService executorService;

    /**
     * Create a new monitoring instance.
     *
     * <p>The schedule frequency and offset are used to define a schedule for report generation
     * that includes the epoch-start time (`Instant.ofEpochMillis(0L)` = 1970-01-01 00:00:00), plus
     * the offset, and then repeating according ot the scheduling frequency. For example, specifying
     * a 1-day frequency and a 3-hour offset will result in report generation at 3am every day.</p>
     *
     * <p>For a table to included in the reports, its name must match one of the `includes`
     * patterns if given, and not match any of the `excludes` patterns.</p>
     *
     * @param endpoint        endpoint for database access
     * @param schema          schema to be handled
     * @param includes        regexes for names of tables to include, null to include all tables
     * @param excludes        regexes for names of tables to include, null for no exclusions
     * @param granularity     granularity at which to log size info
     * @param executorService service to execute scheduled monitor invocations
     */
    public DbSizeMonitor(DbEndpoint endpoint, Schema schema,
            Set<Pattern> includes, Set<Pattern> excludes, Granularity granularity,
            ScheduledExecutorService executorService) {
        this.endpoint = endpoint;
        this.schema = schema;
        this.includes = includes;
        this.excludes = excludes;
        this.granularity = granularity;
        this.executorService = executorService;
    }

    /**
     * Create a new instance with simple string parameters where suitable.
     *
     * @param endpoint        endpoint for database access
     * @param schema          schema to be monitored
     * @param includes        comma-separated list of regex strings, or null to include all tables
     * @param excludes        comma-separated list of regex strings, or null for no exclusions
     * @param granularity     granularity at which to log size info
     * @param executorService service to execute scheduled monitor invocations
     */
    public DbSizeMonitor(DbEndpoint endpoint, Schema schema,
            String includes, String excludes, Granularity granularity,
            ScheduledExecutorService executorService) {
        this(endpoint, schema, compilePatterns(includes), compilePatterns(excludes), granularity,
                executorService);
    }

    /**
     * Start the size monitor.
     *
     * @param schedFrequency how often to produce a report
     * @param schedOffset    how long after the start of each reporting interval to produce a
     *                       report
     * @throws SQLException                if there's a DB problem
     * @throws UnsupportedDialectException if the DB endpoint is misconfigured
     * @throws InterruptedException        if we're interrupted
     */
    public void activate(Duration schedFrequency, Duration schedOffset)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        this.adapter = DbSizeAdapter.of(endpoint.dslContext(), schema, granularity);
        final long freqMillis = schedFrequency.toMillis();
        final long offsetMillis = schedOffset.toMillis();
        final long now = System.currentTimeMillis();
        final long nextMillis = freqMillis - (now % freqMillis);
        executorService.scheduleAtFixedRate(this::runReport,
                nextMillis + offsetMillis, freqMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Start the size monitor, using string representations of frequency and offset durations. See
     * {@link Duration#parse(CharSequence)} for required format.
     *
     * @param schedFrequency how often to produce a report
     * @param schedOffset    how long after the start of each reporting interval to produce a
     *                       report
     * @throws SQLException                if there's a DB problem
     * @throws UnsupportedDialectException if the DB endpoint is misconfigured
     * @throws InterruptedException        if we're interrupted
     */
    public void activate(String schedFrequency, String schedOffset)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        activate(Duration.parse(schedFrequency), Duration.parse(schedOffset));
    }

    private Map<Table<?>, DbSizeReport> createReport() {
        final Map<Table<?>, DbSizeReport> result = adapter.getTables().stream()
                .filter(this::shouldIncludeTable)
                .collect(Collectors.toMap(Functions.identity(), t -> new DbSizeReport(t, adapter)));
        return result;
    }

    private void runReport() {
        logger.info("Generating DB Size Report");
        createReport().entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getKey().getName()))
                .forEach(e -> e.getValue().logTo(logger));
    }

    private boolean shouldIncludeTable(final Table<?> table) {
        final String t = table.getName();
        return (includes == null || includes.stream().anyMatch(p -> p.matcher(t).matches()))
                && (excludes == null || excludes.stream().noneMatch(p -> p.matcher(t).matches()));
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
        private long totalBytes;

        /**
         * Create a new instance.
         *
         * @param table   table to be reported
         * @param adapter adapter that can be used to obtain size info
         */
        public DbSizeReport(final Table<?> table, final DbSizeAdapter adapter) {
            this.table = table;
            this.sizeItems = adapter.getSizeItems(table);
            this.totalBytes = sizeItems.stream().mapToLong(SizeItem::getSize).sum();
        }

        /**
         * Log the report to the given logger.
         *
         * @param logger logger to use in report
         */
        public void logTo(Logger logger) {
            sizeItems.forEach(item ->
                    logger.info("{}{}: {}", indent(item.getLevel()), item.getDescription(),
                            FileUtils.byteCountToDisplaySize(item.getSize())));
        }

        private String indent(int n) {
            return StringUtils.repeat("  ", n);
        }
    }

    /**
     * POJO to keep line items of a size report.
     */
    public static class SizeItem {

        private final int level;
        private final long size;
        private final String description;

        /**
         * Create a new instance.
         *
         * @param level       level of this item, in hierarchy of all items; zero means top-level
         * @param size        byte count to be reported for this item
         * @param description text to include in the report for this item
         */
        public SizeItem(final int level, final long size, final String description) {
            this.level = level;
            this.size = size;
            this.description = description;
        }

        public int getLevel() {
            return level;
        }

        public long getSize() {
            return size;
        }

        public String getDescription() {
            return description;
        }
    }
}
