package com.vmturbo.mediation.conversion.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DerivedTargetSpecificationDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.ProfileDTO.CommodityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.DBProfileDTO;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainNodeBuilder;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Helper class containing constants, util functions, etc, which are used by different converters.
 */
public class ConverterUtils {

    /**
     * Used to specify default access commodity capacity. As required by market, this needs to be
     * set to a very large number ("infinity"). This is used for setting the capacity of access
     * commodities (license, application, etc.) and some commodities (like: DBCacheHitRate) when
     * capacity is not available.
     */
    public static final float DEFAULT_ACCESS_COMMODITY_CAPACITY = 1.0E9f;

    // the prefix for the key of DataCenter access commodity
    public static final String DATACENTER_ACCESS_COMMODITY_PREFIX = "DataCenter::";

    /**
     * Create sold commodities based on CommodityProfile list in EntityProfileDTO.
     *
     * @param commodityProfiles list of {@link CommodityProfileDTO}s to create commodities based on
     * @param soldCommToCommProfileMapping the mapping from sold commodities types (that need to be
     * created) to commodity types in the commodity profile where the capacity comes from
     * @return list of sold commodities created from the commodityProfiles
     */
    public static List<CommodityDTO> createSoldCommoditiesFromCommodityProfiles(
            @Nonnull List<CommodityProfileDTO> commodityProfiles,
            @Nonnull Map<CommodityType, CommodityType> soldCommToCommProfileMapping) {
        final Map<CommodityType, Float> capacityMap = commodityProfiles.stream()
                .collect(Collectors.toMap(CommodityProfileDTO::getCommodityType,
                        CommodityProfileDTO::getCapacity));
        return soldCommToCommProfileMapping.entrySet().stream()
                .map(entry -> {
                    final CommodityType destCommType = entry.getKey();
                    final CommodityType srcCommType = entry.getValue();
                    CommodityDTO.Builder commBuilder = CommodityDTO.newBuilder()
                            .setCommodityType(destCommType);
                    // set capacity if there is CommodityProfile for this CommodityTypes
                    if (capacityMap.containsKey(srcCommType)) {
                        commBuilder.setCapacity(capacityMap.get(srcCommType));
                    }
                    return commBuilder.build();
                }).collect(Collectors.toList());
    }

    /**
     * Create sold commodities based on dbProfileDTO in EntityProfileDTO. Currently the following
     * commodities are created:
     *     APPLICATION, LICENSE_ACCESS
     *
     * @param dbProfileDTO the {@link DBProfileDTO} to create commodities based on
     * @return list of sold commodities created from the dbProfileDTO
     */
    public static List<CommodityDTO> createSoldCommoditiesFromDBProfileDTO(@Nonnull DBProfileDTO dbProfileDTO) {
        final List<CommodityDTO> soldCommodities = Lists.newArrayList();
        dbProfileDTO.getLicenseList().stream()
                .flatMap(licenseMapEntry -> licenseMapEntry.getLicenseNameList().stream())
                .distinct()
                .forEach(license ->
                        // create license access commodity for each distinct license
                        soldCommodities.add(CommodityDTO.newBuilder()
                                .setCommodityType(CommodityType.LICENSE_ACCESS)
                                .setKey(license)
                                .setCapacity(DEFAULT_ACCESS_COMMODITY_CAPACITY)
                                .build())
                );
        return soldCommodities;
    }

    /**
     * Given a CommodityBought, remove the Application commodity from the bought list.
     *
     * @param commodityBought the CommodityBought for which to remove Application commodity
     */
    public static void removeApplicationCommodity(@Nonnull CommodityBought.Builder commodityBought) {
        List<CommodityDTO> newBoughtCommodities = commodityBought.getBoughtList().stream()
                .filter(commodityDTO -> commodityDTO.getCommodityType() != CommodityType.APPLICATION)
                .collect(Collectors.toList());
        commodityBought.clear();
        commodityBought.addAllBought(newBoughtCommodities);
    }

    /**
     * Take a {@link DiscoveryResponse} and remove any {@link DerivedTargetSpecificationDTO} that
     * match the given {@link SDKProbeType}.
     *
     * @param rawDiscoveryResponse the DiscoveryResponse to filter
     * @param derivedProbeType the type of probe whose derived targets will be removed
     * @return {@link DiscoveryResponse} without any derived targets of the given probe type
     */
    public static DiscoveryResponse removeDerivedTargets(@Nonnull DiscoveryResponse rawDiscoveryResponse,
                                                         @Nonnull SDKProbeType derivedProbeType) {
        List<DerivedTargetSpecificationDTO> targetsToKeep =
            rawDiscoveryResponse.getDerivedTargetList().stream()
                .filter(derivedTargetSpec -> derivedTargetSpec.hasProbeType() &&
                    !derivedTargetSpec.getProbeType()
                        .equals(derivedProbeType.getProbeType()))
                .collect(Collectors.toList());
        return rawDiscoveryResponse.toBuilder().clearDerivedTarget()
            .addAllDerivedTarget(targetsToKeep).build();
    }

    /**
     * Add a basic TemplateDTO to the existing set if it doesn't exist. This is used to complete
     * the supply chain definition so it doesn't throw supply chain validation warnings in TP.
     *
     * @param templateDTOs the existing TemplateDTOs
     * @param entityType the entity type for which to add TemplateDTO
     * @return set of TemplateDTOs combining existing and the new one
     */
    public static Set<TemplateDTO> addBasicTemplateDTO(@Nonnull Set<TemplateDTO> templateDTOs,
                                                       @Nonnull EntityType entityType) {
        final Set<TemplateDTO> sc = Sets.newHashSet(templateDTOs);
        // create supply chain node for the given entity type if not existing
        final boolean hasTemplate = sc.stream()
            .anyMatch(template -> template.getTemplateClass() == entityType);
        if (!hasTemplate) {
            sc.add(new SupplyChainNodeBuilder().entity(entityType).buildEntity());
        }
        return sc;
    }

    // a wrapper class to represent the storage amount and storage access commodity's min and max capacity
    public static class CommodityCapacityWrapper {
        public final double storageAmountMaxCapacity;
        public final double storageAmountMinCapacity;
        public final double storageAccessMaxCapacity;
        public final double storageAccessMinCapacity;
        public CommodityCapacityWrapper(double stAmtMax, double stAmtMin, double stAccMax, double stAccMin) {
            storageAmountMaxCapacity = stAmtMax;
            storageAmountMinCapacity = stAmtMin;
            storageAccessMaxCapacity = stAccMax;
            storageAccessMinCapacity = stAccMin;
        }
    }
    // a utility map used for keeping the capacity values for cloud storage entity
    public static ImmutableMap<String, CommodityCapacityWrapper> cloudStorageCapacityMap = ImmutableMap
            .<String, CommodityCapacityWrapper>builder()
            .put("IO1", new CommodityCapacityWrapper(16*1024, 4, 20000, 100))
            .put("GP2", new CommodityCapacityWrapper(16*1024, 1, 10000, 100))
            .put("SC1", new CommodityCapacityWrapper(16*1024, 500, 250, 0))
            .put("ST1", new CommodityCapacityWrapper(16*1024, 500, 500, 0))
            .put("STANDARD", new CommodityCapacityWrapper(16*1024, 1, 200, 40))
            .put("MANAGED_STANDARD", new CommodityCapacityWrapper(4*1024, 1, 500, 0))
            .put("UNMANAGED_STANDARD", new CommodityCapacityWrapper(5000*1024, 1, 500, 0))
            .put("MANAGED_PREMIUM", new CommodityCapacityWrapper(4*1024, 1, 7500, 120))
            .put("UNMANAGED_PREMIUM", new CommodityCapacityWrapper(8*1024, 1, 7500, 500))
            .build();
    public static final int IO1_IOPS_TO_STORAGE_AMOUNT_RATIO = 50;
    public static final int GP2_IOPS_TO_STORAGE_AMOUNT_RATIO = 3;
    public static final String GP2 = "GP2";
    public static final String IO1 = "IO1";

    /**
     * Gets set of {@link CloudService}s filtered by a given {@link SDKProbeType}.
     *
     * @param probeType A {@link SDKProbeType} for filtration requirements.
     * @return Set of {@link CloudService}s filtered by a given {@link SDKProbeType}.
     */
    public static Set<CloudService> getCloudServicesByProbeType(SDKProbeType probeType) {
        return Arrays.stream(CloudService.values())
                .filter(cs -> cs.getProbeType() == probeType)
                .collect(Collectors.toSet());
    }
}
