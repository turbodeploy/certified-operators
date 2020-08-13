package com.vmturbo.topology.processor.topology;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.Units;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * This helper class contains logic related to adjusting values of storage access (IOPS) and
 * storage amount commodities to ensure volumes to be placed in the desired storage tiers.
 * The logic is ported from classic opsmgr.
 * TODO: There will be upcoming enhancements that improve how the storage amount restriction is
 * determined. Percentile value will also be used for for IOPS and peak value won't be used.
 * When that happens, some logic in this class will become obsolete.
 */
public class CloudStorageMigrationHelper {
    private static final Logger logger = LogManager.getLogger();

    private static final int MARKET_IOPS_AMOUNT_MIN_CAPACITY_GP2 = 100;

    /**
     * List of all storage tiers.
     */
    private enum StorageTier {
        GP2,
        IO1,
        SC1,
        ST1,
        STANDARD,
        MANAGED_PREMIUM,
        MANAGED_STANDARD,
        MANAGED_STANDARD_SSD,
        UNMANAGED_PREMIUM,
        UNMANAGED_STANDARD,
        MANAGED_ULTRA_SSD
    }

    private static final int IO1_IOPS_TO_STORAGE_AMOUNT_RATIO = 50;

    private static final int GP2_IOPS_TO_STORAGE_AMOUNT_RATIO = 3;

    private static final int IO1_IOPS_AMOUNT_MAX_CAPACITY = 64000;

    private static final int GP2_IOPS_AMOUNT_MAX_CAPACITY = 16000;

    private static final int SC1_IOPS_AMOUNT_MAX_CAPACITY = 250;

    private static final int ST1_IOPS_AMOUNT_MAX_CAPACITY = 500;

    private static final int STANDARD_IOPS_AMOUNT_MAX_CAPACITY = 200;

    /*
     * Azure Min/Max IOPS per storage tier taken from:
     * https://docs.microsoft.com/en-us/azure/virtual-machines/windows/disks-types
     */

    /** Minimum amount of IOPS that any Managed Standard storage size supports. */
    private static final int MANAGED_STANDARD_IOPS_AMOUNT_MIN_CAPACITY = 500;
    /** Maximum amount of IOPS that any Managed Standard storage size supports. */
    private static final int MANAGED_STANDARD_IOPS_AMOUNT_MAX_CAPACITY = 2000;
    /** Estimated ratio of IOPS per GiB of capacity for Managed Standard storage. */
    private static final int MANAGED_STANDARD_IOPS_RATIO = 1;

    /** Minimum amount of IOPS that any Unmanaged Standard storage size supports. */
    private static final int UNMANAGED_STANDARD_IOPS_AMOUNT_MIN_CAPACITY = 500;
    /** Maximum amount of IOPS that any Unmanaged Standard storage size supports. */
    private static final int UNMANAGED_STANDARD_IOPS_AMOUNT_MAX_CAPACITY = 2000;
    /** Estimated ratio of IOPS per GiB of capacity for Unmanaged Standard storage. */
    private static final int UNMANAGED_STANDARD_IOPS_RATIO = 1;

    /** Minimum amount of IOPS that any Managed Standard SSD storage size supports. */
    private static final int MANAGED_STANDARD_SSD_IOPS_AMOUNT_MIN_CAPACITY = 120;
    /** Maximum amount of IOPS that any Managed Standard SSD storage size supports. */
    private static final int MANAGED_STANDARD_SSD_IOPS_AMOUNT_MAX_CAPACITY = 6000;
    /** Estimated ratio of IOPS per GiB of capacity for Managed Standard SSD storage. */
    private static final int MANAGED_STANDARD_SSD_IOPS_RATIO = 2;

    /** Minimum amount of IOPS that any Managed Premium storage size supports. */
    private static final int MANAGED_PREMIUM_IOPS_AMOUNT_MIN_CAPACITY = 120;
    /** Maximum amount of IOPS that any Managed Premium storage size supports. */
    private static final int MANAGED_PREMIUM_IOPS_AMOUNT_MAX_CAPACITY = 20000;
    /** Estimated ratio of IOPS per GiB of capacity for Managed Premium storage. */
    private static final int MANAGED_PREMIUM_IOPS_RATIO = 4;

    /** Minimum amount of IOPS that any Unmanaged Premium storage size supports. */
    private static final int UNMANAGED_PREMIUM_IOPS_AMOUNT_MIN_CAPACITY = 120;
    /** Maximum amount of IOPS that any Unmanaged Premium storage size supports. */
    private static final int UNMANAGED_PREMIUM_IOPS_AMOUNT_MAX_CAPACITY = 20000;
    /** Estimated ratio of IOPS per GiB of capacity for Unmanaged Premium storage. */
    private static final int UNMANAGED_PREMIUM_IOPS_RATIO = 4;

    /** The smallest number of IOPS that can be provisioned on an Azure Ultra SSD disk. */
    private static final int MANAGED_ULTRA_SSD_IOPS_AMOUNT_MIN_CAPACITY = 100;
    /** The largest number of IOPS that can be provisioned on an Azure Ultra SSD disk. */
    private static final int MANAGED_ULTRA_SSD_IOPS_AMOUNT_MAX_CAPACITY = 160000;
    /** The largest number of IOPS that can be provisioned per GB on an Azure Ultra SSD disk. */
    private static final int MANAGED_ULTRA_SSD_IOPS_RATIO = 300;
    /**
     * The smallest number of IOPS that can be provisioned per GB on an Azure Ultra SSD disk,
     * (but not less than the hard minimum of MANAGED_ULTRA_SSD_IOPS_AMOUNT_MIN_CAPACITY).
     */
    private static final int MANAGED_ULTRA_SSD_IOPS_MIN_RATIO = 2;
    /** The smallest number of MBps throughput that can be provisioned on an Azure Ultra SSD disk. */
    private static final int MANAGED_ULTRA_SSD_MBPS_AMOUNT_MIN_CAPACITY = 1;

    private CloudStorageMigrationHelper() {
        // This class should not be instantiated.
    }

    /**
     * Get the historical max IOPS. If it is not available, fallback to use peak value.
     *
     * @param commodityBoughtDTO the storage access commodity DTO
     * @param histMaxIOPS the maximum StorageAccess bought value of the last 30 days
     * @return the updated storage access commodity DTO
     */
    static CommodityBoughtDTO.Builder getHistoricalMaxIOPS(
            @Nonnull CommodityBoughtDTO commodityBoughtDTO,
            double histMaxIOPS) {
        CommodityBoughtDTO.Builder commodityBoughtBuilder = commodityBoughtDTO.toBuilder();
        commodityBoughtBuilder.setUsed(histMaxIOPS);
        commodityBoughtBuilder.setPeak(histMaxIOPS);
        commodityBoughtBuilder.setHistoricalUsed(HistoricalValues.newBuilder().setHistUtilization(histMaxIOPS).build());
        commodityBoughtBuilder.setHistoricalPeak(HistoricalValues.newBuilder().setHistUtilization(histMaxIOPS).build());
        return commodityBoughtBuilder;
    }

    /**
     * Initialize the data structure IopsToStorageRatios which will be used when checking if
     * storage amount need to be adjusted based on IOPS.
     *
     * @param inputGraph input graph
     * @return IopsToStorageRatios
     */
    static IopsToStorageRatios populateMaxIopsRatioAndCapacity(@Nonnull final TopologyGraph<TopologyEntity> inputGraph) {
        int maxRatio = 1;
        int maxCapacity = 0;
        // maxRatioOnNonExpensiveTier and maxCapacityOnNonExpensiveTier is populated for storages that
        // are not IO1 or Ultra SSD. This is based on empirical experience that those two are the most
        // expensive tiers.
        int maxRatioOnNonExpensiveTier = 1;
        int maxCapacityOnNonExpensiveTier = 0;

        Set<TopologyEntityDTO> storageTiers = inputGraph.entitiesOfType(EntityType.STORAGE_TIER)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .map(TopologyEntityDTO.Builder::build)
                .collect(Collectors.toSet());

        for (TopologyEntityDTO st : storageTiers) {
            if (st.getDisplayName().equalsIgnoreCase(StorageTier.GP2.name())) {
                maxRatio = Math.max(maxRatio, GP2_IOPS_TO_STORAGE_AMOUNT_RATIO);
                maxCapacity = Math.max(maxCapacity, GP2_IOPS_AMOUNT_MAX_CAPACITY);
                maxRatioOnNonExpensiveTier = Math.max(maxRatioOnNonExpensiveTier,
                        GP2_IOPS_TO_STORAGE_AMOUNT_RATIO);
                maxCapacityOnNonExpensiveTier = Math.max(maxCapacityOnNonExpensiveTier,
                        GP2_IOPS_AMOUNT_MAX_CAPACITY);
            } else if (st.getDisplayName().equalsIgnoreCase(StorageTier.IO1.name())) {
                maxRatio = Math.max(maxRatio, IO1_IOPS_TO_STORAGE_AMOUNT_RATIO);
                maxCapacity = Math.max(maxCapacity, IO1_IOPS_AMOUNT_MAX_CAPACITY);
            } else if (st.getDisplayName().equalsIgnoreCase(StorageTier.SC1.name())) {
                maxRatio = Math.max(maxRatio, 1);
                maxCapacity = Math.max(maxCapacity, SC1_IOPS_AMOUNT_MAX_CAPACITY);
                maxRatioOnNonExpensiveTier = Math.max(maxRatioOnNonExpensiveTier, 1);
                maxCapacityOnNonExpensiveTier = Math.max(maxCapacityOnNonExpensiveTier,
                        SC1_IOPS_AMOUNT_MAX_CAPACITY);
            } else if (st.getDisplayName().equalsIgnoreCase(StorageTier.ST1.name())) {
                maxRatio = Math.max(maxRatio, 1);
                maxCapacity = Math.max(maxCapacity, ST1_IOPS_AMOUNT_MAX_CAPACITY);
                maxRatioOnNonExpensiveTier = Math.max(maxRatioOnNonExpensiveTier, 1);
                maxCapacityOnNonExpensiveTier = Math.max(maxCapacityOnNonExpensiveTier,
                        ST1_IOPS_AMOUNT_MAX_CAPACITY);
            } else if (st.getDisplayName().equalsIgnoreCase(StorageTier.STANDARD.name())) {
                maxRatio = Math.max(maxRatio, 1);
                maxCapacity = Math.max(maxCapacity, STANDARD_IOPS_AMOUNT_MAX_CAPACITY);
                maxRatioOnNonExpensiveTier = Math.max(maxRatioOnNonExpensiveTier, 1);
                maxCapacityOnNonExpensiveTier = Math.max(maxCapacityOnNonExpensiveTier,
                        STANDARD_IOPS_AMOUNT_MAX_CAPACITY);
            } else if (st.getDisplayName().equalsIgnoreCase(StorageTier.MANAGED_PREMIUM.name())) {
                maxRatio = Math.max(maxRatio, MANAGED_PREMIUM_IOPS_RATIO);
                maxCapacity = Math.max(maxCapacity, MANAGED_PREMIUM_IOPS_AMOUNT_MAX_CAPACITY);
                maxRatioOnNonExpensiveTier = Math.max(maxRatioOnNonExpensiveTier,
                        MANAGED_PREMIUM_IOPS_RATIO);
                maxCapacityOnNonExpensiveTier = Math.max(maxCapacityOnNonExpensiveTier,
                        MANAGED_PREMIUM_IOPS_AMOUNT_MAX_CAPACITY);
            } else if (st.getDisplayName().equalsIgnoreCase(StorageTier.MANAGED_STANDARD.name())) {
                maxRatio = Math.max(maxRatio, MANAGED_STANDARD_IOPS_RATIO);
                maxCapacity = Math.max(maxCapacity, MANAGED_STANDARD_IOPS_AMOUNT_MAX_CAPACITY);
                maxRatioOnNonExpensiveTier = Math.max(maxRatioOnNonExpensiveTier,
                        MANAGED_STANDARD_IOPS_RATIO);
                maxCapacityOnNonExpensiveTier = Math.max(maxCapacityOnNonExpensiveTier,
                        MANAGED_STANDARD_IOPS_AMOUNT_MAX_CAPACITY);
            } else if (st.getDisplayName().equalsIgnoreCase(StorageTier.MANAGED_STANDARD_SSD.name())) {
                maxRatio = Math.max(maxRatio, MANAGED_STANDARD_SSD_IOPS_RATIO);
                maxCapacity = Math.max(maxCapacity, MANAGED_STANDARD_SSD_IOPS_AMOUNT_MAX_CAPACITY);
                maxRatioOnNonExpensiveTier = Math.max(maxRatioOnNonExpensiveTier,
                        MANAGED_STANDARD_SSD_IOPS_RATIO);
                maxCapacityOnNonExpensiveTier = Math.max(maxCapacityOnNonExpensiveTier,
                        MANAGED_STANDARD_SSD_IOPS_AMOUNT_MAX_CAPACITY);
            } else if (st.getDisplayName().equalsIgnoreCase(StorageTier.UNMANAGED_PREMIUM.name())) {
                maxRatio = Math.max(maxRatio, UNMANAGED_PREMIUM_IOPS_RATIO);
                maxCapacity = Math.max(maxCapacity, UNMANAGED_PREMIUM_IOPS_AMOUNT_MAX_CAPACITY);
                maxRatioOnNonExpensiveTier = Math.max(maxRatioOnNonExpensiveTier,
                        UNMANAGED_PREMIUM_IOPS_RATIO);
                maxCapacityOnNonExpensiveTier = Math.max(maxCapacityOnNonExpensiveTier,
                        UNMANAGED_PREMIUM_IOPS_AMOUNT_MAX_CAPACITY);
            } else if (st.getDisplayName().equalsIgnoreCase(StorageTier.UNMANAGED_STANDARD.name())) {
                maxRatio = Math.max(maxRatio, UNMANAGED_STANDARD_IOPS_RATIO);
                maxCapacity = Math.max(maxCapacity, UNMANAGED_STANDARD_IOPS_AMOUNT_MAX_CAPACITY);
                maxRatioOnNonExpensiveTier = Math.max(maxRatioOnNonExpensiveTier,
                        UNMANAGED_STANDARD_IOPS_RATIO);
                maxCapacityOnNonExpensiveTier = Math.max(maxCapacityOnNonExpensiveTier,
                        UNMANAGED_STANDARD_IOPS_AMOUNT_MAX_CAPACITY);
            } else if (st.getDisplayName().equalsIgnoreCase(StorageTier.MANAGED_ULTRA_SSD.name())) {
                maxRatio = Math.max(maxRatio, MANAGED_ULTRA_SSD_IOPS_RATIO);
                maxCapacity = Math.max(maxCapacity, MANAGED_ULTRA_SSD_IOPS_AMOUNT_MAX_CAPACITY);
            }
        }
        return new IopsToStorageRatios(maxRatio, maxCapacity, maxRatioOnNonExpensiveTier, maxCapacityOnNonExpensiveTier);
    }

    /**
     * Get storage provisioned used in GiB.
     *
     * @param commBoughtGroupingForSL commodity bought grouping for shopping list
     * @return storage provisioned used in GiB
     */
    private static float getStorageProvisionedAmount(
            @Nonnull final CommoditiesBoughtFromProvider commBoughtGroupingForSL) {
        List<CommodityBoughtDTO> commodityList = commBoughtGroupingForSL.getCommodityBoughtList();
        Optional<CommodityBoughtDTO> storageProvisionCommodityBoughtOpt = commodityList.stream()
                .filter(s -> s.getCommodityType().getType() == CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE)
                .findAny();

        return storageProvisionCommodityBoughtOpt.map(
                storageProvisioned -> (float)(storageProvisioned.getUsed())).orElse(0f);
    }

    /**
     * Assign storage provisioned "used" value to storage amount bought.
     *
     * @param storageAmountCommodity storage amount commodity
     * @param commBoughtGroupingForSL commodity bought list
     * @return udpated storage amount commodity
     */
    static CommodityBoughtDTO updateStorageAmountCommodityBought(
            @Nonnull final CommodityBoughtDTO storageAmountCommodity,
            @Nonnull final CommoditiesBoughtFromProvider commBoughtGroupingForSL) {
        float diskSizeInMB = getStorageProvisionedAmount(commBoughtGroupingForSL);
        Optional<CommodityBoughtDTO> iopsCommodityBoughtOpt =
                commBoughtGroupingForSL.getCommodityBoughtList().stream()
                        .filter(s -> s.getCommodityType().getType() == CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE)
                        .findAny();
        if (diskSizeInMB > 0 && iopsCommodityBoughtOpt.isPresent()) {
            return storageAmountCommodity.toBuilder()
                    .setUsed(diskSizeInMB)
                    .setPeak(diskSizeInMB)
                    .build();
        }
        return storageAmountCommodity;
    }

    /**
     * Use storage provision used value as storage amount.
     * Convert storage amount unit to GiB.
     * Adjust the storage amount commodity based on IOPS.
     * This method is used by MPC Optimize plan.
     *
     * @param storageAmountCommodity storage amount commodity
     * @param commBoughtGroupingForSL commodity bought grouping for shopping list
     * @param iopsToStorageRatios iopsToStorageRatios
     * @param entityOid OID of the entity that owns the commodity list
     * @param historicalMaxIops historical max IOPS used in the last 30 days
     * @return updated CommodityBoughtDTO
     */
    static CommodityBoughtDTO adjustStorageAmountForCloudMigration(
            @Nonnull final CommodityBoughtDTO storageAmountCommodity,
            @Nonnull final CommoditiesBoughtFromProvider commBoughtGroupingForSL,
            @Nonnull final IopsToStorageRatios iopsToStorageRatios,
            @Nonnull final Long entityOid,
            @Nonnull final Double historicalMaxIops) {

        // Optimization plan (a.k.a. Consumption plan): Adjust storage amount commodity
        // value based on IOPs
        // Use storage provisioned "used" value for storage amount.
        float diskSizeInGB = (float)(getStorageProvisionedAmount(commBoughtGroupingForSL) / Units.KIBI);

        if (diskSizeInGB > 0
                && storageAmountCommodity.getCommodityType().getType() == CommodityType.STORAGE_AMOUNT_VALUE) {
            if ((historicalMaxIops < iopsToStorageRatios.getMaxCapacity())
                    && (historicalMaxIops > iopsToStorageRatios.getMaxCapacityOnNonExpensiveTier())
                    && (diskSizeInGB * iopsToStorageRatios.getMaxRatio() < historicalMaxIops)) {
                // when iops is more than the capacity of non-expensive tiers,
                // but less than the most expensive tier, and if the disk size
                // limits the iops availability, we should try to resize up disk
                // to match iops demand so that it will be placed on the most
                // expensive tier.
                diskSizeInGB = (float)Math.ceil(historicalMaxIops / iopsToStorageRatios.getMaxRatio());
                logger.info("Entity {} has a commodity list with provider {} that has storage "
                                + "Amount commodity value is set to {}GB because of high IOPS value {}."
                                + "IOPS value is above non-expensive max.", entityOid,
                        commBoughtGroupingForSL.getProviderId(), diskSizeInGB, historicalMaxIops);
            } else if ((historicalMaxIops <= iopsToStorageRatios.getMaxCapacityOnNonExpensiveTier())
                    && (diskSizeInGB * iopsToStorageRatios.getMaxRatioOnNonExpensiveTier() < historicalMaxIops)) {
                // when iops is less than the capacity of non-expensive tiers,
                // and if the disk size limits the iops availability,
                // we should try to resize the disk so that it will be placed
                // on non-expensive tiers. maxIopsRatioOnNonExpensiveTier but
                // not minIopsRatioOnNonExpensiveTier is used because iops
                // to disk size ratio in Azure is a rough average value,
                // for many tiers, the small disk doesnt really comply with
                // the ratio, so resize using minIopsRatioOnNonExpensiveTier
                // can lead to incorrect result.
                diskSizeInGB = (float)Math.ceil(historicalMaxIops / iopsToStorageRatios.getMaxRatioOnNonExpensiveTier());
                logger.info("Entity {} has a commodity list with provider {} that has storage "
                                + "Amount commodity value is set to {}GB because of high IOPS value {}."
                                + "IOPS value is below non-expensive max.", entityOid,
                        commBoughtGroupingForSL.getProviderId(), diskSizeInGB, historicalMaxIops);
            } else {
                // When iops is more than capacity of iops available, resize doesn't help.
                logger.debug("Entity {} has a commodity list with provider {} is using storage "
                        + "provisioned used value as storage amount for MPC plan.");
            }
        }
        float diskSizeInMB = (float)(diskSizeInGB * Units.KIBI);
        return storageAmountCommodity.toBuilder()
                .setUsed(diskSizeInMB)
                .setPeak(diskSizeInMB)
                .build();
    }

    /**
     * A simple data structure to hold the ratio between storage amount and IOPS, and the maximum
     * IOPS capacity.
     */
    static class IopsToStorageRatios {
        private int maxRatio = 1;
        private int maxCapacity = 0;

        // maxRatioOnNonExpensiveTier and maxCapacityOnNonExpensiveTier is populated for storages that
        // are not IO1 or Ultra SSD. This is based on empirical experience that those two are the most
        // expensive tiers.
        private int maxRatioOnNonExpensiveTier = 1;
        private int maxCapacityOnNonExpensiveTier = 0;

        IopsToStorageRatios(int maxRatio, int maxCapacity, int maxRatioOnNonExpensiveTier,
                                   int maxCapacityOnNonExpensiveTier) {
            this.maxRatio = maxRatio;
            this.maxCapacity = maxCapacity;
            this.maxRatioOnNonExpensiveTier = maxRatioOnNonExpensiveTier;
            this.maxCapacityOnNonExpensiveTier = maxCapacityOnNonExpensiveTier;
        }

        public int getMaxRatio() {
            return maxRatio;
        }

        public int getMaxCapacity() {
            return maxCapacity;
        }

        public int getMaxRatioOnNonExpensiveTier() {
            return maxRatioOnNonExpensiveTier;
        }

        public int getMaxCapacityOnNonExpensiveTier() {
            return maxCapacityOnNonExpensiveTier;
        }
    }
}
