package com.vmturbo.market.topology;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Any constants which need to be shared across the classes in TopologyConversions can be
 * here.
 */
public class TopologyConversionConstants {

    /**
     * These entity types are not sent for Analysis i.e. no traders are created for these entity
     * types
     */
    public static final Set<Integer> ENTITY_TYPES_TO_SKIP_TRADER_CREATION = Collections.unmodifiableSet(
            Sets.newHashSet(EntityType.COMPUTE_TIER_VALUE,
                    EntityType.STORAGE_TIER_VALUE, EntityType.DATABASE_TIER_VALUE,
                    EntityType.DATABASE_SERVER_TIER_VALUE,
                    EntityType.REGION_VALUE, EntityType.AVAILABILITY_ZONE_VALUE,
                    EntityType.BUSINESS_ACCOUNT_VALUE, EntityType.VIRTUAL_VOLUME_VALUE));

    /**
     * Character (as opposed to string) separator in commodity key.
     */
    public static final char COMMODITY_TYPE_KEY_SEPARATOR_CHAR = '|';

    /**
     * If used is greater than capacity for a few commodities, then we make the
     * used = CAPACITY_FACTOR * capacity.
     */
    public static final float CAPACITY_FACTOR = 0.999999f;

    public static final float ACCESS_COMMODITY_CAPACITY = 1.0E9f;

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
                    .put(CommodityType.TRANSACTION_VALUE, CommodityType.VCPU_VALUE)
                    .put(CommodityType.DTU_VALUE, CommodityType.DTU_VALUE)
                    .put(CommodityType.STORAGE_AMOUNT_VALUE, CommodityType.STORAGE_AMOUNT_VALUE)
            // mapping for cloud volume commodities
                    .put(CommodityType.STORAGE_ACCESS_VALUE, CommodityType.STORAGE_ACCESS_VALUE)
                    .put(CommodityType.IO_THROUGHPUT_VALUE, CommodityType.IO_THROUGHPUT_VALUE)
                    .build();

    /**
     * Map for the type of the commodity in an entity on the cloud to the type of
     * the commodity sold in a cloud tier.
     **/
    public static Map<Integer, ImmutableMap<Integer, Integer>> entityCommTypeToTierCommType
        = ImmutableMap.<Integer, ImmutableMap<Integer, Integer>>builder()
            .put(EntityType.COMPUTE_TIER.getNumber(),
                    ImmutableMap.<Integer, Integer>builder()
                            .put(CommodityType.VMEM_VALUE, CommodityType.MEM_VALUE)
                            .put(CommodityType.VCPU_VALUE, CommodityType.CPU_VALUE)
                            // retrieve the old capacity when converted
                            .put(CommodityType.STORAGE_ACCESS_VALUE, CommodityType.STORAGE_ACCESS_VALUE).build())
            .put(EntityType.DATABASE_SERVER_TIER.getNumber(),
                    ImmutableMap.<Integer, Integer>builder()
                            .put(CommodityType.VMEM_VALUE, CommodityType.VMEM_VALUE)
                            .put(CommodityType.VCPU_VALUE, CommodityType.VCPU_VALUE).build())
            .put(EntityType.DATABASE_TIER.getNumber(),
                    ImmutableMap.<Integer, Integer>builder()
                            .put(CommodityType.VMEM_VALUE, CommodityType.DB_MEM_VALUE)
                            .put(CommodityType.VCPU_VALUE, CommodityType.TRANSACTION_VALUE).build())
            .build();

    // TODO: the following constants will be from user settings once UI supports it
    public static final double RESIZE_AVG_WEIGHT = 0.1f;
    public static final double RESIZE_MAX_WEIGHT = 0.9f;
    public static final double RESIZE_PEAK_WEIGHT = 0.0f;
    public static final float FLOAT_COMPARISON_DELTA = 0.0001f;

    /**
     * A set of cloud tier types.
     */
    public static final Set<Integer> cloudTierTypes = ImmutableSet.of(EntityType.COMPUTE_TIER_VALUE, EntityType.STORAGE_TIER_VALUE, EntityType.DATABASE_TIER_VALUE, EntityType.DATABASE_SERVER_TIER_VALUE);

    /**
     * These are the commodities for which utlizations are analysed at multiple time slots.
     */
    public static final Set<Integer> TIMESLOT_COMMODITIES =
            ImmutableSet.of(CommodityType.POOL_CPU_VALUE,
                    CommodityType.POOL_MEM_VALUE,
                    CommodityType.POOL_STORAGE_VALUE);

    /**
     * Commodity types whose category need to be stored.
     */
    public static final Set<Integer> ONPREM_BOUGHT_COMMODITIES_TO_TRACK =
            ImmutableSet.of(CommodityDTO.CommodityType.IMAGE_CPU_VALUE, CommodityDTO.CommodityType.IMAGE_MEM_VALUE, CommodityType.IMAGE_STORAGE_VALUE);
    /**
     * Commodity types whose category need to be stored if the SE is cloud SE.
     */
    public static final Set<Integer> CLOUD_BOUGHT_COMMODITIES_RESIZED =
            ImmutableSet.of(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE, CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE, CommodityType.STORAGE_ACCESS_VALUE);
}
