package com.vmturbo.market.topology;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

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

    public static final float ACCESS_COMMODITY_CAPACITY = 1.0E9f;

    public static final String BICLIQUE = "BICLIQUE";

    // TODO: This information should not be stored here. It should come from outside market
    public static final Set<OSType> INSTANCE_SIZE_FLEXIBLE_OPERATING_SYSTEMS = ImmutableSet.of(
            OSType.LINUX, OSType.RHEL, OSType.SUSE);

    // a map for the type of the dependent commodity bought by an entity on the cloud to the type of
    // the resizable commodity sold
    public static ImmutableMap<Integer, Integer> commDependancyMapForCloudResize = ImmutableMap.<Integer, Integer>builder()
                    .put(CommodityType.MEM_PROVISIONED_VALUE, CommodityType.VMEM_VALUE)
                    .put(CommodityType.CPU_PROVISIONED_VALUE, CommodityType.VCPU_VALUE)
                    .put(CommodityType.MEM_VALUE, CommodityType.VMEM_VALUE)
                    .put(CommodityType.CPU_VALUE, CommodityType.VCPU_VALUE)
                    // mapping for AWS DatabaseServer commodities
                    .put(CommodityType.VMEM_VALUE, CommodityType.VMEM_VALUE)
                    .put(CommodityType.VCPU_VALUE, CommodityType.VCPU_VALUE)
                    // mapping for Azure Database commodities
                    .put(CommodityType.DB_MEM_VALUE, CommodityType.VMEM_VALUE)
                    .put(CommodityType.TRANSACTION_VALUE, CommodityType.VCPU_VALUE).build();
    // TODO: the following constants will be from user settings once UI supports it
    public static final float RESIZE_AVG_WEIGHT =  0.1f;
    public static final float RESIZE_MAX_WEIGHT =  0.9f;
    public static final float RESIZE_PEAK_WEIGHT =  0.0f;
    public static final float RESIZE_TARGET_UTILIZATION_VM_VCPU = 0.7f;
    public static final float RESIZE_TARGET_UTILIZATION_VM_VMEM = 0.9f;
    public static final float FLOAT_COMPARISON_DELTA = 0.0000001f;
}
