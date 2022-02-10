package com.vmturbo.cost.component.rollup;

/**
 * Enumeration of base table names for rollup.
 */
public enum RolledUpTable {
    /**
     * Entity savings.
     */
    ENTITY_SAVINGS("entity_savings"),

    /**
     * Entity savings.
     */
    ENTITY_COST("entity_cost"),

    /**
     * Billed costs.
     */
    BILLED_COST("billed_cost"),

    /**
     * Reserved instance coverage.
     */
    RESERVED_INSTANCE_COVERAGE("reserved_instance_coverage"),

    /**
     * Reserved instance utilization.
     */
    RESERVED_INSTANCE_UTILIZATION("reserved_instance_utilization");

    private final String tableName;

    RolledUpTable(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Get base table name in the database.
     *
     * @return Table name.
     */
    public String getTableName() {
        return tableName;
    }
}
