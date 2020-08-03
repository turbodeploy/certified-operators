package com.vmturbo.history.schema;

/**
 * Varieties of historical data.
 *
 * <p>Generally speaking, all data of a given variety should be modeled, processed and treated identically.
 * If that's not the case it probably means we should introduce a new variety.
 */
public enum HistoryVariety {
    /**
     * Attributes and bought/sold commodity information relating to entities in the supply chain.
     */
    ENTITY_STATS,
    /**
     * Price indexes assigned to bought/sold commodities by market analysis.
     */
    PRICE_DATA
}
