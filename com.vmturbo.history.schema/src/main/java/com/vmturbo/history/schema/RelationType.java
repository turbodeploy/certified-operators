package com.vmturbo.history.schema;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * Map between the Commodity Relation Types as stored in the DB, as tinyints, and
 * the character nomenclature used in the REST API. The "literal" value is returned from
 * the server to the REST API. The API_TO_RELATION_MAP translates from filter-request strings
 * to RelationType enums used in queries to filter stats.
 */
public enum RelationType {

    // In the Legacy nomenclature, "Commodities" === commodities sold
	COMMODITIES(0, "Commodities"),

	COMMODITIESBOUGHT(1, "CommoditiesBought"),

	// Derived values e.g. priceIndex, numVCPUs, etc.
	METRICS(-1, "CommoditiesFromAttributes");

    // map a string used in the REST API queries to a RelationType
    private static Map<String, RelationType> API_TO_RELATION_MAP =
            new ImmutableMap.Builder<String, RelationType>()
                // TODO: find a way to reference 'StringConstants.RELATION_SOLD', etc. instead of
                // hard-coding these Strings. Current maven dependencies don't allow this.
                    .put("bought", COMMODITIESBOUGHT)
                    .put("sold", COMMODITIES)
                    .put("metrics", METRICS)
                    .build();

	public static RelationType getApiRelationType(String literal) {
	    return API_TO_RELATION_MAP.get(literal.toLowerCase());
    }

	// the small integer value recorded in the DB
	private final int value;
	// the string value to be returned in /stats query responses
	private final String literal;

	RelationType(int value, String literal) {
		this.value = value;
		this.literal = literal;
	}

	public int getValue() {
	  return value;
	}

	public String getLiteral() {
	  return literal;
	}

	@Override
	public String toString() {
		return literal;
	}
}