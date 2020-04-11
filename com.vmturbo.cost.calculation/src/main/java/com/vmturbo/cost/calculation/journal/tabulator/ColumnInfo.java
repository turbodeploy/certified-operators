package com.vmturbo.cost.calculation.journal.tabulator;

/**
 * Stores information about a column in an ASCII table for the {@link JournalEntryTabulator}.
 */
class ColumnInfo {
    private final String heading;
    private final int maxWidth;
    private final int minWidth;

    /**
     * Constructor.
     *
     * @param heading the heading
     * @param minWidth the minimum width
     * @param maxWidth the maximum width
     */
    ColumnInfo(final String heading, final int minWidth, final int maxWidth) {
        this.heading = heading;
        this.minWidth = minWidth;
        this.maxWidth = maxWidth;
    }

    public String getHeading() {
        return heading;
    }

    public int getMaxWidth() {
        return maxWidth;
    }

    public int getMinWidth() {
        return minWidth;
    }
}
