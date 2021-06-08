package com.vmturbo.extractor.models;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Constants related to the application of the DB models.
 */
public class Constants {
    private Constants() {
    }

    /** Valid DB timestamp value that's far in the future, treated as infinity (and beyond!).
     *
     * <p>We're specifying a day before end of 9999, since Postgres doesn't deal with larger years.
     * The one-day gap ensures if jOOQ uses this value in a literal and expresses it in local
     * time zone it won't get bumped into year-10000 in that literal.</p>
     */
    public static final OffsetDateTime MAX_TIMESTAMP = OffsetDateTime.parse("9999-12-31T00:00:00Z");

    /**
     * Default whitelist of comodities that we track for before/after metrics in action attributes.
     * See: {@link com.vmturbo.extractor.schema.json.common.ActionImpactedEntity}.
     *
     * <p/>Hand-picked by looking through the commodities the UI requests for different action types.
     */
    public static final Set<CommodityType> REPORTING_ACTION_COMMODITY_TYPES_WHITELIST =
        ImmutableSet.<CommodityType>builder()
                .add(CommodityType.COOLING)
                .add(CommodityType.CPU)
                .add(CommodityType.CPU_PROVISIONED)
                .add(CommodityType.DB_CACHE_HIT_RATE)
                .add(CommodityType.DB_MEM)
                .add(CommodityType.FLOW)
                .add(CommodityType.FLOW_ALLOCATION)
                .add(CommodityType.HEAP)
                .add(CommodityType.IO_THROUGHPUT)
                .add(CommodityType.MEM)
                .add(CommodityType.MEM_PROVISIONED)
                .add(CommodityType.NET_THROUGHPUT)
                .add(CommodityType.POOL_CPU)
                .add(CommodityType.POOL_MEM)
                .add(CommodityType.POOL_STORAGE)
                .add(CommodityType.PORT_CHANEL)
                .add(CommodityType.POWER)
                .add(CommodityType.STORAGE_ACCESS)
                .add(CommodityType.STORAGE_ALLOCATION)
                .add(CommodityType.STORAGE_AMOUNT)
                .add(CommodityType.STORAGE_PROVISIONED)
                .add(CommodityType.VCPU)
                .add(CommodityType.VCPU_LIMIT_QUOTA)
                .add(CommodityType.VCPU_REQUEST)
                .add(CommodityType.VCPU_REQUEST_QUOTA)
                .add(CommodityType.VMEM)
                .add(CommodityType.VMEM_LIMIT_QUOTA)
                .add(CommodityType.VMEM_REQUEST)
                .add(CommodityType.VMEM_REQUEST_QUOTA)
                .add(CommodityType.VSTORAGE)
                .add(CommodityType.DTU)
                .build();

    /**
     * Whitelist of bought commodities that we track for before/after metrics in action attributes,
     * See: {@link com.vmturbo.extractor.schema.json.common.ActionImpactedEntity}, grouped by
     * entity type and provider type.
     *
     * <p/>Hand-picked by looking through the commodities the UI requests for different action types.
     * Currently only cloud scale action shows affected bought commodities.
     */
    public static final Map<Integer, Map<Integer, Set<Integer>>>
            ACTION_IMPACT_BOUGHT_COMMODITIES_WHITELIST = ImmutableMap.of(
                    EntityType.VIRTUAL_MACHINE_VALUE, ImmutableMap.of(
                            EntityType.COMPUTE_TIER_VALUE, ImmutableSet.of(
                                    CommodityType.STORAGE_ACCESS_VALUE,
                                    CommodityType.STORAGE_AMOUNT_VALUE,
                                    CommodityType.IO_THROUGHPUT_VALUE,
                                    CommodityType.NET_THROUGHPUT_VALUE,
                                    CommodityType.NUM_DISK_VALUE
                            )
                    )
    );

    /**
     * Default whitelisted commodity types for reporting.
     *
     * <p>Commodity metrics for other types are not recorded.</p>
     */
    public static final Set<CommodityType> REPORTING_DEFAULT_COMMODITY_TYPES_WHITELIST =
            ImmutableSet.<CommodityType>builder()
                    .add(CommodityType.ACTIVE_SESSIONS)
                    .add(CommodityType.BALLOONING)
                    .add(CommodityType.BUFFER_COMMODITY)
                    .add(CommodityType.CONNECTION)
                    .add(CommodityType.COOLING)
                    .add(CommodityType.CPU)
                    .add(CommodityType.CPU_ALLOCATION)
                    .add(CommodityType.CPU_PROVISIONED)
                    .add(CommodityType.DB_CACHE_HIT_RATE)
                    .add(CommodityType.DB_MEM)
                    .add(CommodityType.FLOW)
                    .add(CommodityType.FLOW_ALLOCATION)
                    .add(CommodityType.HEAP)
                    .add(CommodityType.IMAGE_CPU)
                    .add(CommodityType.IMAGE_MEM)
                    .add(CommodityType.IMAGE_STORAGE)
                    .add(CommodityType.IO_THROUGHPUT)
                    .add(CommodityType.MEM)
                    .add(CommodityType.MEM_ALLOCATION)
                    .add(CommodityType.MEM_PROVISIONED)
                    .add(CommodityType.NET_THROUGHPUT)
                    .add(CommodityType.POOL_CPU)
                    .add(CommodityType.POOL_MEM)
                    .add(CommodityType.POOL_STORAGE)
                    .add(CommodityType.PORT_CHANEL)
                    .add(CommodityType.POWER)
                    .add(CommodityType.Q1_VCPU)
                    .add(CommodityType.Q2_VCPU)
                    .add(CommodityType.Q3_VCPU)
                    .add(CommodityType.Q4_VCPU)
                    .add(CommodityType.Q5_VCPU)
                    .add(CommodityType.Q6_VCPU)
                    .add(CommodityType.Q7_VCPU)
                    .add(CommodityType.Q8_VCPU)
                    .add(CommodityType.Q16_VCPU)
                    .add(CommodityType.Q32_VCPU)
                    .add(CommodityType.Q64_VCPU)
                    .add(CommodityType.QN_VCPU)
                    .add(CommodityType.REMAINING_GC_CAPACITY)
                    .add(CommodityType.RESPONSE_TIME)
                    .add(CommodityType.SLA_COMMODITY)
                    .add(CommodityType.STORAGE_ACCESS)
                    .add(CommodityType.STORAGE_ALLOCATION)
                    .add(CommodityType.STORAGE_AMOUNT)
                    .add(CommodityType.STORAGE_LATENCY)
                    .add(CommodityType.STORAGE_PROVISIONED)
                    .add(CommodityType.SWAPPING)
                    .add(CommodityType.THREADS)
                    .add(CommodityType.TRANSACTION)
                    .add(CommodityType.TRANSACTION_LOG)
                    .add(CommodityType.VCPU)
                    .add(CommodityType.VCPU_LIMIT_QUOTA)
                    .add(CommodityType.VCPU_REQUEST)
                    .add(CommodityType.VCPU_REQUEST_QUOTA)
                    .add(CommodityType.VMEM)
                    .add(CommodityType.VMEM_LIMIT_QUOTA)
                    .add(CommodityType.VMEM_REQUEST)
                    .add(CommodityType.VMEM_REQUEST_QUOTA)
                    .add(CommodityType.VSTORAGE)
                    .add(CommodityType.TOTAL_SESSIONS)
                    .add(CommodityType.DTU)
                    .build();

    /**
     * Commodity types for which we write a separate metric record for each provided commodity key,
     * as opposed to summing metric values across all commodity keys for the same commodity type
     * sold by a given seller.
     *
     * <p>In some cases, the treatment is dependent on the entity type that is  selling the
     * commodity. This map lists all the entity types for which a given commodity type should not be
     * aggregated. When a commodity is un-aggregated for all selling entity types, use
     * `EntityType.values()` in the builder row for that commodity type.</p>
     */
    public static final Multimap<CommodityType, EntityType> UNAGGREGATED_KEYED_COMMODITY_TYPES =
            ImmutableSetMultimap.<CommodityType, EntityType>builder()
                    .putAll(CommodityType.IO_THROUGHPUT, EntityType.values())
                    .putAll(CommodityType.NET_THROUGHPUT, EntityType.values())
                    .putAll(CommodityType.THREADS, EntityType.values())
                    .build();
}
