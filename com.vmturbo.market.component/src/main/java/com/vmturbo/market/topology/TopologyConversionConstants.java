package com.vmturbo.market.topology;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Any constants which need to be shared across the classes in TopologyConversions can be
 * here.
 */
public class TopologyConversionConstants {
    /**
     * The tiers entity types.
     */
    public static final Set<Integer> TIER_ENTITY_TYPES = Collections.unmodifiableSet(
            Sets.newHashSet(EntityType.COMPUTE_TIER_VALUE, EntityType.STORAGE_TIER_VALUE,
                    EntityType.DATABASE_TIER_VALUE));
    /**
     * The primary tiers entity types. Cloud consumers like VMs and DBs can only consume from one
     * primary tier like compute / database tier. But they can consume from multiple
     * secondary tiers like storage tiers.
     */
    public static final Set<Integer> PRIMARY_TIER_ENTITY_TYPES = Collections.unmodifiableSet(
            Sets.newHashSet(EntityType.COMPUTE_TIER_VALUE, EntityType.DATABASE_TIER_VALUE));

    /**
     * These entity types are not sent for Analysis i.e. no traders are created for these entity
     * types
     */
    public static final Set<Integer> CLOUD_ENTITY_TYPES_TO_SKIP_CONVERSION = Collections.unmodifiableSet(
            Sets.newHashSet(EntityType.COMPUTE_TIER_VALUE,
                    EntityType.STORAGE_TIER_VALUE, EntityType.DATABASE_TIER_VALUE,
                    EntityType.REGION_VALUE, EntityType.AVAILABILITY_ZONE_VALUE,
                    EntityType.BUSINESS_ACCOUNT_VALUE));

    public static final String COMMODITY_TYPE_KEY_SEPARATOR = "|";

    /**
     * If used is greater than capacity for a few commodities, then we make the
     * used = CAPACITY_FACTOR * capacity
     */
    public static final float CAPACITY_FACTOR = 0.999999f;

    public static final String BICLIQUE = "BICLIQUE";
}
