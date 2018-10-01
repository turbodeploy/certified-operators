package com.vmturbo.market.topology;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Any constants which need to be shared across the classes in TopologyConversions can be
 * here.
 */
public class TopologyConversionConstants {
    public static final Set<Integer> TIER_ENTITY_TYPES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(EntityType.COMPUTE_TIER_VALUE,
                    EntityType.STORAGE_TIER_VALUE, EntityType.DATABASE_TIER_VALUE)));
    public static final Set<Integer> COMPUTE_STORAGE_TIER_ENTITY_TYPES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(EntityType.COMPUTE_TIER_VALUE,
                    EntityType.STORAGE_TIER_VALUE)));

    public static final Set<Integer> CLOUD_ENTITY_TYPES_TO_SKIP_CONVERSION = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(EntityType.COMPUTE_TIER_VALUE,
                    EntityType.STORAGE_TIER_VALUE, EntityType.DATABASE_TIER_VALUE,
                    EntityType.REGION_VALUE, EntityType.AVAILABILITY_ZONE_VALUE,
                    EntityType.BUSINESS_ACCOUNT_VALUE)));

    public static final String COMMODITY_TYPE_KEY_SEPARATOR = "|";

    public static final float CAPACITY_FACTOR = 0.999999f;

    public static final String BICLIQUE = "BICLIQUE";
}
