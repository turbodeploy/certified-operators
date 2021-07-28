package com.vmturbo.sql.utils.sizemon;

import com.vmturbo.sql.utils.sizemon.DbSizeMonitor.Granularity;

/**
 * POJO to keep line items of a size report.
 */
public class SizeItem {

    private final Granularity granularity;
    private final long size;
    private final String description;

    /**
     * Create a new instance.
     *
     * @param granularity granularity of this item
     * @param size        byte count to be reported for this item
     * @param description text to include in the report for this item
     */
    public SizeItem(final Granularity granularity, final long size, final String description) {
        this.granularity = granularity;
        this.size = size;
        this.description = description;
    }

    public int getLevel() {
        return granularity.ordinal();
    }

    public long getSize() {
        return size;
    }

    public String getDescription() {
        return description;
    }

    public Granularity getGranularity() {
        return granularity;
    }
}
