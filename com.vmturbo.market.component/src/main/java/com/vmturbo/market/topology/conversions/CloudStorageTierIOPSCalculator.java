package com.vmturbo.market.topology.conversions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.RangeTuple;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This class is used for calculating the IOPS capacity of cloud tiers given the tier and the storage
 * amount.
 */
public class CloudStorageTierIOPSCalculator {
    private static final Logger logger = LogManager.getLogger();

    private final Map<Long, TopologyEntityDTO> topology;

    // Azure "managed" storage tiers
    private static final Set<String> AZURE_STORAGE_TIERS_WITH_IOPS_TABLE = ImmutableSet.of(
            "MANAGED_STANDARD",
            "MANAGED_STANDARD_SSD",
            "MANAGED_PREMIUM"
    );

    // storage tier name -> disk size -> IOPS capacity
    private static Map<String, TreeMap<Double, Double>> azureIopsCapacityMap = new HashMap<>();

    /**
     * Constructor.
     *
     * @param topology the topologyEntityDTOs which came into market-component
     */
    public CloudStorageTierIOPSCalculator(final Map<Long, TopologyEntityDTO> topology) {
        this.topology = topology;
        initializeAzureIOPSCapacityTable();
    }

    private void initializeAzureIOPSCapacityTable() {
        List<TopologyEntityDTO> azureManagedStorageTiers = this.topology.values().stream()
                .filter(e -> e.getEntityType() == EntityType.STORAGE_TIER_VALUE)
                .filter(e -> AZURE_STORAGE_TIERS_WITH_IOPS_TABLE.contains(e.getDisplayName()))
                .collect(Collectors.toList());
        for (TopologyEntityDTO tier : azureManagedStorageTiers) {
            final CommoditySoldDTO iopsSoldComm = tier.getCommoditySoldListList().stream()
                    .filter(c -> c.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE)
                    .findFirst()
                    .orElse(null);
            if (iopsSoldComm == null || !iopsSoldComm.hasRangeDependency()) {
                // should never happen.
                continue;
            }
            List<RangeTuple> rangeTuples = iopsSoldComm.getRangeDependency().getRangeTupleList();
            TreeMap<Double, Double> iopsTable = new TreeMap<>();
            for (RangeTuple tuple : rangeTuples) {
                double diskSize = tuple.getBaseMaxAmountForConsumer();
                double iopsCapacity = tuple.getDependentMaxAmountForConsumer();
                iopsTable.put(diskSize, iopsCapacity);
            }
            azureIopsCapacityMap.put(tier.getDisplayName(), iopsTable);
        }
    }


    /**
     * Determine the IOPS capacity value given the tier and the commodity bought list of a VM.
     *
     * @param commList commodity bought list
     * @param tier storage tier
     * @return IOPS capacity value
     */
    Optional<Double> getIopsCapacity(@Nonnull final List<TopologyDTO.CommodityBoughtDTO> commList,
                                     @Nonnull final TopologyEntityDTO tier) {
        if (tier.getEntityType() != EntityType.STORAGE_TIER_VALUE) {
            return Optional.empty();
        }
        final String tierName = tier.getDisplayName();
        Double iops = null;
        switch (tierName) {
            case "IO1":
                iops = getIO1OrUltraIops(commList, tier);
                break;
            case "GP2":
                iops = getGP2Iops(commList, tier);
                break;
            case "ST1":
                // same logic as SC1
            case "SC1":
                iops = getSC1ST1Iops(commList, tier);
                break;
            case "STANDARD":
                iops = getAwsStandardIops(tier);
                break;
            case "MANAGED_STANDARD":
                // Same as Managed Premium
            case "MANAGED_STANDARD_SSD":
                // Same as Managed Premium
            case "MANAGED_PREMIUM":
                iops = getAzureIops(commList, tier);
                break;
            case "MANAGED_ULTRA_SSD":
                iops = getIO1OrUltraIops(commList, tier);
                break;
            default:
        }
        return iops == null ? Optional.empty() : Optional.of(iops);
    }

    private Double getIO1OrUltraIops(@Nonnull final List<TopologyDTO.CommodityBoughtDTO> commList,
                                     @Nonnull final TopologyEntityDTO tier) {
        Double iops = null;
        final CommodityBoughtDTO storageAmountComm = commList.stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.STORAGE_AMOUNT_VALUE)
                .findFirst()
                .orElse(null);
        final CommodityBoughtDTO storageAccessComm = commList.stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE)
                .findFirst()
                .orElse(null);
        final CommoditySoldDTO iopsSoldComm = tier.getCommoditySoldListList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE)
                .findFirst()
                .orElse(null);
        if (storageAmountComm != null && iopsSoldComm != null && storageAccessComm != null) {
            final double diskSizeInGB = storageAmountComm.getUsed();
            final double maxRatio = iopsSoldComm.getRatioDependency().getMaxRatio();
            final double minRatio = iopsSoldComm.getRatioDependency().getMinRatio();
            final double maxIopsForTier = iopsSoldComm.getMaxAmountForConsumer();
            final double minIopsForTier = iopsSoldComm.getMinAmountForConsumer();
            final double histMaxIopsUsed = storageAccessComm.getUsed();

            iops = Math.min(diskSizeInGB * maxRatio, histMaxIopsUsed);
            iops = Math.max(diskSizeInGB * minRatio, iops);
            if (iops > maxIopsForTier) {
                iops = maxIopsForTier;
            } else if (iops < minIopsForTier) {
                iops = minIopsForTier;
            }
        }
        return iops;
    }

    private Double getGP2Iops(@Nonnull final List<TopologyDTO.CommodityBoughtDTO> commList,
                              @Nonnull final TopologyEntityDTO tier) {
        Double iops = null;
        final CommodityBoughtDTO storageAmountComm = commList.stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.STORAGE_AMOUNT_VALUE)
                .findFirst()
                .orElse(null);
        final CommoditySoldDTO iopsSoldComm = tier.getCommoditySoldListList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE)
                .findFirst()
                .orElse(null);
        if (storageAmountComm != null && iopsSoldComm != null) {
            final double diskSizeInGB = storageAmountComm.getUsed();
            final double maxRatio = iopsSoldComm.getRatioDependency().getMaxRatio();
            final double maxIopsForTier = iopsSoldComm.getMaxAmountForConsumer();
            final double minIopsForTier = iopsSoldComm.getMinAmountForConsumer();

            iops = diskSizeInGB * maxRatio;
            if (iops > maxIopsForTier) {
                iops = maxIopsForTier;
            } else if (iops < minIopsForTier) {
                iops = minIopsForTier;
            }
        }
        return iops;
    }

    private Double getAwsStandardIops(@Nonnull final TopologyEntityDTO tier) {
        Double iops = null;
        final CommoditySoldDTO iopsSoldComm = tier.getCommoditySoldListList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE)
                .findFirst()
                .orElse(null);
        if (iopsSoldComm != null) {
            iops = iopsSoldComm.getMaxAmountForConsumer();
        }
        return iops;
    }

    private Double getSC1ST1Iops(@Nonnull final List<TopologyDTO.CommodityBoughtDTO> commList,
                                 @Nonnull final TopologyEntityDTO tier) {
        Double iops = null;
        final CommodityBoughtDTO storageAmountComm = commList.stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.STORAGE_AMOUNT_VALUE)
                .findFirst()
                .orElse(null);
        final CommoditySoldDTO iopsSoldComm = tier.getCommoditySoldListList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE)
                .findFirst()
                .orElse(null);
        if (storageAmountComm != null && iopsSoldComm != null) {
            final double diskSizeInGB = storageAmountComm.getUsed();
            final double maxIopsForTier = iopsSoldComm.getMaxAmountForConsumer();
            iops = diskSizeInGB * iopsSoldComm.getRatioDependency().getMaxRatio();
            if (iops > maxIopsForTier) {
                iops = maxIopsForTier;
            }
        }
        return iops;
    }

    private Double getAzureIops(@Nonnull final List<TopologyDTO.CommodityBoughtDTO> commList,
                                @Nonnull final TopologyEntityDTO tier) {
        final CommodityBoughtDTO storageAmountComm = commList.stream()
                .filter(c -> c.getCommodityType().getType() == CommodityType.STORAGE_AMOUNT_VALUE)
                .findFirst()
                .orElse(null);
        Double iops = null;
        if (storageAmountComm != null) {
            final double diskSizeInGB = storageAmountComm.getUsed();
            TreeMap<Double, Double> iopsTable = azureIopsCapacityMap.get(tier.getDisplayName());
            if (iopsTable == null) {
                logger.warn("Azure IOPS capacity map is missing for tier {}.", tier.getDisplayName());
            } else {
                Entry<Double, Double> iopsTableEntry = iopsTable.ceilingEntry(diskSizeInGB);

                if (iopsTableEntry != null) {
                    iops = iopsTableEntry.getValue();
                } else {
                    iops = iopsTable.get(iopsTable.lastKey());
                }
            }
        }
        return iops;
    }
}
