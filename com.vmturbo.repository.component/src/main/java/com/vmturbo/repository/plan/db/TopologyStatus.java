package com.vmturbo.repository.plan.db;

/**
 * Enum to track the state of a topology.
 * This is recorded into the topology metadata in the database.
 * DO NOT CHANGE the numeric values associated with each status.
 */
enum TopologyStatus {
    /**
     * Topology ingestion has started, but not completed. This topology should not be
     * publically available.
     */
    INGESTION_STARTED(0),

    /**
     * Topology ingestion has completed, and is available for external viewing.
     */
    INGESTION_COMPLETED(1),

    /**
     * Topology deletion has started. This topology should no longer be publically
     * available.
     */
    DELETION_STARTED(2);

    /**
     * This is the numeric representation which gets put into the database column.
     */
    private final byte num;

    TopologyStatus(int i) {
        this.num = (byte)i;
    }

    public byte getNum() {
        return num;
    }
}
