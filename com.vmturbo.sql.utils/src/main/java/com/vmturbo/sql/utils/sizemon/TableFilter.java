package com.vmturbo.sql.utils.sizemon;

import java.util.Set;
import java.util.regex.Pattern;

import org.jooq.Table;

/**
 * Class to filter table names for a given {@link DbSizeReporter}.
 */
public class TableFilter {

    private final Set<Pattern> includes;
    private final Set<Pattern> excludes;

    /**
     * Create a new instance.
     *
     * @param includes patterns for tables to be included
     * @param excludes patterns for tables to be excluded
     */
    public TableFilter(Set<Pattern> includes, Set<Pattern> excludes) {
        this.includes = includes;
        this.excludes = excludes;
    }

    /**
     * Determine whether a given table should be included in a report.
     *
     * @param table table being considered
     * @return true to process the table
     */
    public boolean shouldInclude(Table<?> table) {
        final String t = table.getName();
        return (includes == null || includes.stream().anyMatch(p -> p.matcher(t).matches()))
                && (excludes == null || excludes.stream().noneMatch(p -> p.matcher(t).matches()));
    }
}
