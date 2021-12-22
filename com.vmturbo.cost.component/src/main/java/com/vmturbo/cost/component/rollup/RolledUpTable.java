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
     * Billed costs.
     */
    BILLED_COST("billed_cost");

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
