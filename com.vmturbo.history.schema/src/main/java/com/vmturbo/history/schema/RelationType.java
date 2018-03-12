package com.vmturbo.history.schema;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

public enum RelationType {

	COMMODITIES(0, "COMMODITIES", "Commodities"),

	COMMODITIESBOUGHT(1, "COMMODITIESBOUGHT", "CommoditiesBought"),

	// Commodities neither bought nor sold; e.g. priceIndex, numVCPUs, etc.
	COMMODITIES_FROM_ATTRIBUTES(-1, "COMMODITIES_FROM_ATTRIBUTES", "CommoditiesFromAttributes");

	private static final int COMMODITIES_VALUE = 0;
	private static final int COMMODITIESBOUGHT_VALUE = 1;

	private static Map<String, RelationType> LITERAL_TO_RELATION_MAP =
			new ImmutableMap.Builder<String, RelationType>()
			.put("COMMODITIES", COMMODITIES)
			.put("COMMODITIESBOUGHT", COMMODITIESBOUGHT)
			.build();

	private static final RelationType[] VALUES_ARRAY =
		new RelationType[] {
			COMMODITIES,
			COMMODITIESBOUGHT,
		};
	public static final List<RelationType> VALUES = Collections.unmodifiableList(Arrays.asList(VALUES_ARRAY));

	public static RelationType get(String literal) {
		return LITERAL_TO_RELATION_MAP.get(literal.toUpperCase());
	}

	private static Map<String, RelationType> NAME_TO_RELATION_MAP =
			new ImmutableMap.Builder<String, RelationType>()
			.put("Commodities", COMMODITIES)
			.put("CommoditiesBought", COMMODITIESBOUGHT)
			.build();

	public static RelationType getByName(String name) {
		return NAME_TO_RELATION_MAP.get(name);
	}

	public static RelationType get(int value) {
		switch (value) {
			case COMMODITIES_VALUE: return COMMODITIES;
			case COMMODITIESBOUGHT_VALUE: return COMMODITIESBOUGHT;
		}
		return null;
	}

	private final int value;
	private final String name;
	private final String literal;

	private RelationType(int value, String name, String literal) {
		this.value = value;
		this.name = name;
		this.literal = literal;
	}

	public int getValue() {
	  return value;
	}

	public String getName() {
	  return name;
	}

	public String getLiteral() {
	  return literal;
	}

	@Override
	public String toString() {
		return literal;
	}
}