package com.vmturbo.components.common.utils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.memory.MemoryMeasurer;
import com.vmturbo.components.common.utils.DataPacks.IDataPack;
import com.vmturbo.components.common.utils.MemReporter.MemReport.MemReportItem;

/**
 * Interface for classes that are capable of reporting their memory utilization.
 *
 * <p>Individual classes have a lot of flexibility in how they report their memory, e.g. omitting
 * large shared object, dealing with structures that have internal sharing, etc. The default is just
 * to report the size of the reporting instance.</p>
 *
 * <p>To report sizes of selected members rather than the size of the reporting instance as a
 * whole:</p>
 * <ul>
 *     <li>
 *        Override {@link #getMemSize()} to return null. This suppresses reporting the reporting
 *        instance size.
 *     </li>
 *     <li>
 *         Override {@link #getNestedMemReporters()} to return reports for members of interest. If
 *         a member is itself a {@link MemReporter}, the member can be listed as-is, else it can
 *         be wrapped in a {@link SimpleMemReporter}.
 *     </li>
 * </ul>
 */
public interface MemReporter {
    /**
     * Provide a description of this reporter, for use in reports.
     *
     * <p>Default is the final component of the fully-qualified class name, except that where the
     * class is a named inner class, "$" is replaced with "." (so e.g. MyClass.Builder, not
     * MyClass$Builder). For anonymous inner classes, the "$" separator is retained (these cases are
     * detected when the name following the separator is composed solely of digits).</p>
     *
     * @return description
     */
    default String getMemDescription() {
        String[] parts = getClass().getName().split("\\.");
        return parts[parts.length - 1].replaceAll("\\$([^$0-9]+)(\\$|$)", ".$1");
    }

    /**
     * Compute the memory size to report for this reporter.
     *
     * @return size to report, or null to suppress reporting a size for this reporter
     */
    default Long getMemSize() {
        final Set<Object> exclusions = ImmutableSet.builder()
                .addAll(getMemExclusions().stream().filter(Objects::nonNull).iterator())
                .build();
        return MemoryMeasurer.measure(this, exclusions).getTotalSizeBytes();
    }

    /**
     * Obtain the number of entries in the reporting object, if that makes sense and is feasible for
     * the reporting object.
     *
     * <p>By default, we report the size of a Java {@link Collection} or {@link Map}</p>
     *
     * @return number of entries, or null if not appropriate or not available
     */
    default Integer getMemItemCount() {
        return defaultItemCount(this);
    }

    /**
     * Method to obtain the entry count of a Java {@link Collection} or {@link Map}.
     *
     * @param o reporting object
     * @return entry count, or null if the reporting object is neither a {@link Collection} nor a
     * {@link Map}
     */
    static Integer defaultItemCount(Object o) {
        if (o instanceof Collection) {
            return ((Collection<?>)o).size();
        } else if (o instanceof Map) {
            return ((Map<?, ?>)o).size();
        } else if (o instanceof IDataPack) {
            return ((IDataPack<?>)o).size();
        } else if (o != null && o.getClass().isArray()) {
            return Array.getLength(o);
        } else {
            return null;
        }
    }

    /**
     * Provide a list of nested reporters that should be reported along with this reporter.
     *
     * <p>Nested reporters are reported in a way that shows the nesting relationship, e.g. via
     * indentation in log reports.</p>
     *
     * @return list of nested reporters
     */
    default List<MemReporter> getNestedMemReporters() {
        return Collections.emptyList();
    }

    /**
     * Provide a set of objects that should be excluded from measurement by this reporter.
     *
     * @return objects to be excluded
     */
    default Collection<Object> getMemExclusions() {
        return Collections.emptyList();
    }

    /**
     * Create a {@link MemReport} for this reporter.
     *
     * <p>A {@link MemReport} includes {@link MemReportItem}s for the reporting instance and all
     * its nested reporters (and theirs, etc.).</p>
     *
     * @param memReporter the reporting instance
     * @return the {@link MemReport}
     */
    static MemReport report(MemReporter memReporter) {
        try {
            return new MemReport(memReporter);
        } catch (Exception e) {
            LogManager.getLogger(MemReporter.class).warn("Failed to create memory report", e);
            return new DummyMemReport();
        }
    }

    /**
     * Log a memory report for this reporter.
     *
     * @param memReporter reporting instance
     * @param logger      logger to use for logging
     * @param logLevel    log level to log at
     */
    static void logReport(MemReporter memReporter, Logger logger, Level logLevel) {
        logReport(memReporter, logger, logLevel, null);
    }

    /**
     * Log a memory report with a tag added to the very first report item description.
     *
     * @param memReporter the reporting instance
     * @param logger      logger to use
     * @param logLevel    log level to log at
     * @param tag         string to add (in [] brackets) to the description of the first logged
     *                    {@link MemReportItem}
     */
    static void logReport(MemReporter memReporter, Logger logger, Level logLevel, String tag) {
        if (logger.isEnabled(logLevel)) {
            report(memReporter).logTo(logger, logLevel, tag);
        }
    }

    /**
     * A {@link MemReporter} that reports the measured size of a single object.
     *
     * <p>The reporter will have no nested reporters.</p>
     */
    class SimpleMemReporter implements MemReporter {

        private final String description;
        private final Object reportedObject;
        private final Set<Object> exclusions;

        /**
         * Create a new instance.
         *
         * @param description    description string
         * @param reportedObject object whose size is to be reported
         * @param exclusions     objects to be excluded from the report for this reporter
         */
        public SimpleMemReporter(String description, Object reportedObject,
                Collection<Object> exclusions) {
            this.description = description;
            this.reportedObject = reportedObject;
            this.exclusions = ImmutableSet.builder().addAll(exclusions).build();
        }

        /**
         * Create a new instance with no exclusions.
         *
         * @param description    description string
         * @param reportedObject object whose size is to be reported
         */
        public SimpleMemReporter(String description, Object reportedObject) {
            this(description, reportedObject, Collections.emptySet());
        }

        @Override
        public String getMemDescription() {
            return description;
        }

        @Override
        public Long getMemSize() {
            return reportedObject != null
                    ? MemoryMeasurer.measure(reportedObject, exclusions).getTotalSizeBytes()
                    : 0L;
        }

        @Override
        public Integer getMemItemCount() {
            return defaultItemCount(reportedObject);
        }
    }

    /**
     * A representation of data to be reported for a given reporter and its nested reporters.
     *
     * <p>Collecting all these before actually emitting a report reduces the likelihood that
     * in certain reporting mechanisms, the report will be broken into noncontiguous pieces. This
     * can easily happen, for example, with a log-based report, where other logging may become
     * intermingled with the memory report's individual log lines.</p>
     */
    class MemReport {
        private final List<MemReportItem> items;

        /**
         * Create a new report.
         *
         * @param memReporter The top-level memory reporter
         */
        public MemReport(MemReporter memReporter) {
            this.items = gatherReportItems(memReporter, 0);
        }

        private static List<MemReportItem> gatherReportItems(MemReporter memReporter, int indent) {
            List<MemReportItem> items = new ArrayList<>();
            items.add(new MemReportItem(memReporter, indent));
            memReporter.getNestedMemReporters().stream()
                    .filter(Objects::nonNull)
                    .forEach(nestedReporter ->
                            items.addAll(gatherReportItems(nestedReporter, indent + 1)));
            return items;
        }

        /**
         * Send the report to a logger, with each {@link MemReportItem} on its own log line.
         *
         * @param logger   logger to use
         * @param logLevel logging level to log at
         * @param tag      tag string to be added to the first log line
         */
        public void logTo(final Logger logger, final Level logLevel, String tag) {
            for (MemReportItem item : items) {
                item.logTo(logger, logLevel, tag);
                tag = null;
            }
        }

        /**
         * An item in a memory report.
         */
        static class MemReportItem {
            private final int indent;
            private final MemReporter memReporter;
            private final Long size;
            private final Integer itemCount;

            /**
             * Create a new instance.
             *
             * @param memReporter memory reporter for this item
             * @param indent      nesting level for this item
             */
            MemReportItem(MemReporter memReporter, int indent) {
                this.memReporter = memReporter;
                this.indent = indent;
                Long size;
                try {
                    size = memReporter != null ? memReporter.getMemSize() : Long.valueOf(0L);
                } catch (Exception e) {
                    LogManager.getLogger().warn("Failed to obtain memory size of {}; using -1",
                            memReporter.getMemDescription(), e);
                    size = -1L;
                }
                this.size = size;
                Integer itemCount;
                try {
                    itemCount = memReporter != null ? memReporter.getMemItemCount() : null;
                } catch (Exception e) {
                    LogManager.getLogger().warn("Failed to obtain item count of {}; using null", memReporter.getMemDescription(), e);
                    itemCount = null;
                }
                this.itemCount = itemCount;
            }

            /**
             * Log this item to a logger.
             *
             * @param logger   logger to log to
             * @param logLevel log level to log at
             * @param tag      tag to add in this log item, or null for none
             */
            public void logTo(final Logger logger, final Level logLevel, String tag) {
                logger.log(logLevel, "{}{}{}{}: {}{}{}",
                        indent(),
                        memReporter.getMemDescription(),
                        tag != null ? "[" + tag + "]" : "",
                        indent == 0 ? " Memory Report" : "",
                        renderSize(),
                        size != null && size != -1L ? String.format("(%,db)", size) : "",
                        itemCount != null ? ("[#" + itemCount + "]") : ""
                );
            }

            @NotNull
            private String renderSize() {
                if (size != null) {
                    return size != -1L ? FileUtils.byteCountToDisplaySize(size) : "unknown size";
                } else {
                    return "";
                }
            }

            private String indent() {
                return Strings.repeat(" ", 2 * indent);
            }
        }
    }

    /**
     * Dummy class to use use (for chained method calls) when a real {@link MemReport} fails
     * during construction.
     */
    class DummyMemReport extends MemReport {
        public DummyMemReport() {
            super(new SimpleMemReporter("dummy", null));
        }
    }
}
