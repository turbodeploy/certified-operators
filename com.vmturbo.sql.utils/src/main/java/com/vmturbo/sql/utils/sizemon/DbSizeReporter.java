package com.vmturbo.sql.utils.sizemon;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.jooq.Schema;

import com.vmturbo.sql.utils.sizemon.DbSizeMonitor.DbSizeReport;
import com.vmturbo.sql.utils.sizemon.DbSizeMonitor.Granularity;

/**
 * Base class for size reporters, which are supposed to do something useful with a list of {@link
 * DbSizeReport}s.
 */
public abstract class DbSizeReporter {

    private final TableFilter tableFilter;
    protected final Granularity granularity;
    protected final Schema schema;

    /**
     * Create a new instance.
     *
     * @param schema      Schema on which report is based
     * @param includes    comma-separated list of regexes to match tables that are to be included,
     *                    or null to include all tables
     * @param excludes    comma-separated list of regexes to match tables that are to be excluded,
     *                    or null to exclude no tables
     * @param granularity granularity of size data to be processed by this reporter
     */
    public DbSizeReporter(Schema schema, Set<Pattern> includes, Set<Pattern> excludes, Granularity granularity) {
        this.schema = schema;
        this.tableFilter = new TableFilter(includes, excludes);
        this.granularity = granularity;
    }

    /**
     * Perform all processing for set of table reports.
     *
     * @param reports per-table reports to process
     */
    public void process(List<DbSizeReport> reports) {
        processStart();
        reports.stream()
                .filter(r -> tableFilter.shouldInclude(r.getTable()))
                .forEach(this::processTableReport);
        processSchemaTotal(reports.stream()
                .filter(r -> tableFilter.shouldInclude(r.getTable()))
                .mapToLong(DbSizeReport::getTotalBytes).sum());
    }

    /**
     * Perform any processing that may be required prior to processing all the per-table reports.
     */
    public void processStart() {
    }

    /**
     * Process the {@link DbSizeReport} instance for a single table.
     *
     * @param report the {@link DbSizeReport} instance
     */
    abstract void processTableReport(DbSizeReport report);

    /**
     * Perform any final processing after all per-table reports have been processed.
     *
     * @param totalBytes total # of bytes across all reported tables
     */
    public void processSchemaTotal(long totalBytes) {
    }
}

