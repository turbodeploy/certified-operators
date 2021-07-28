package com.vmturbo.sql.utils.sizemon;

import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.jooq.Schema;

import com.vmturbo.sql.utils.sizemon.DbSizeMonitor.DbSizeReport;
import com.vmturbo.sql.utils.sizemon.DbSizeMonitor.Granularity;

/**
 * {@link DbSizeReporter} that writes size information to the log.
 */
public class LoggingReporter extends DbSizeReporter {

    private final Logger logger;

    /**
     * Create a new instance.
     *
     * @param logger      logger to use for logging
     * @param schema      schema being reported
     * @param includes    regexes for tables to include
     * @param excludes    regexes for tables to exclude
     * @param granularity granularity at which to report size info
     */
    public LoggingReporter(final Logger logger, final Schema schema, final Set<Pattern> includes, final Set<Pattern> excludes, final Granularity granularity) {
        super(schema, includes, excludes, granularity);
        this.logger = logger;
    }

    @Override
    public void processStart() {
        logger.info("Database Size Report for Schema {}", schema.getName());
    }

    @Override
    void processTableReport(final DbSizeReport report) {
        report.getSizeItems(granularity).forEach(item ->
                logger.info("{}{}: {}", indent(item.getLevel()), item.getDescription(),
                        FileUtils.byteCountToDisplaySize(item.getSize())));
    }

    @Override
    public void processSchemaTotal(final long totalBytes) {
        logger.info("Schema {} total size: {}", schema.getName(),
                FileUtils.byteCountToDisplaySize(totalBytes));
    }

    private String indent(int n) {
        return StringUtils.repeat("  ", n);
    }
}
