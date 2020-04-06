package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Applicator for vSAN commodities.
 */
@ThreadSafe
public class VsanStorageApplicator implements SettingApplicator {
    private static final Logger logger = LogManager.getLogger();

    private final TopologyGraph<TopologyEntity> graph;

    /**
     * Constructor for the applicator.
     * @param graphWithSettings - topology graph with settings.
     */
    public VsanStorageApplicator(@Nonnull final GraphWithSettings graphWithSettings)  {
        this.graph = graphWithSettings.getTopologyGraph();
    }

    @Override
    public void apply(@Nonnull Builder storage,
            @Nonnull Map<EntitySettingSpecs, Setting> settings) {
        if (storage.getEntityType() != EntityType.STORAGE_VALUE) {
            return;
        }

        StorageInfo storageInfo = storage.getTypeSpecificInfo().getStorage();
        if (storageInfo.getStorageType() != StorageType.VSAN) {
            return;
        }

        try {
            applyToStorageAmount(storage, settings, storageInfo);
            applyToStorageAccess(storage, settings);
        } catch (EntityApplicatorException e) {
            logger.error("Error applying settings to Storage commodities for vSAN storage "
                    + storage.getDisplayName(), e);
        }
    }

    private void applyToStorageAmount(@Nonnull Builder storage,
                    @Nonnull Map<EntitySettingSpecs, Setting> settings,
                    @Nonnull StorageInfo storageInfo) throws EntityApplicatorException {
        CommoditySoldDTO.Builder storageAmount = getSoldStorageCommodityBuilder(
                        storage, CommodityType.STORAGE_AMOUNT);
        double effectiveCapacity = storageAmount.getCapacity();

        // subtract off space reserved for unavailable hosts, either failed
        // or in maintenance
        double hostCapacityReservation = getNumericSetting(settings,
                EntitySettingSpecs.HciHostCapacityReservation);
        double largestCapacity = getLargestHciHostRawDiskCapacity(storage);
        // TODO Host capacities needs to be adjusted by RAID factor
        largestCapacity *= storageInfo.getPolicy().getRaidFactor();
        effectiveCapacity -= largestCapacity * hostCapacityReservation;

        // reduce by requested slack space
        double slackSpacePercentage = getNumericSetting(settings,
                EntitySettingSpecs.HciSlackSpacePercentage);
        double slackSpaceRatio = (100.0 - slackSpacePercentage) / 100.0;
        effectiveCapacity *= slackSpaceRatio;

        // Adjust by compression ratio
        boolean useCompressionSetting = getBooleanSetting(settings,
                EntitySettingSpecs.HciUseCompression);
        double compressionRatio = useCompressionSetting
                ? getNumericSetting(settings, EntitySettingSpecs.HciCompressionRatio)
                : 1.0;
        effectiveCapacity *= compressionRatio;

        storageAmount.setUsed(storageAmount.getUsed() * compressionRatio);
        storageAmount.setCapacity(effectiveCapacity);
    }

    private void applyToStorageAccess(@Nonnull Builder storage,
                    @Nonnull Map<EntitySettingSpecs, Setting> settings)
                    throws EntityApplicatorException {
        CommoditySoldDTO.Builder storageAccess = getSoldStorageCommodityBuilder(
                        storage, CommodityType.STORAGE_ACCESS);
        final double iopsCapacityPerHost = getNumericSetting(settings,
                        EntitySettingSpecs.HciHostIopsCapacity);
        final double reservedHostCount = getNumericSetting(settings,
                        EntitySettingSpecs.HciHostCapacityReservation);
        final long hciHostCount = countHCIHosts(storage);

        final double totalIOPSCapacity = (hciHostCount - reservedHostCount) * iopsCapacityPerHost;
        logger.trace("Set sold StorageAccess for {} capacity to {}, host count {}",
                        storage.getDisplayName(), totalIOPSCapacity, hciHostCount);
        storageAccess.setCapacity(totalIOPSCapacity);

        for (CommoditiesBoughtFromProvider.Builder boughtBuilder :
                storage.getCommoditiesBoughtFromProvidersBuilderList())    {
            if (!boughtBuilder.hasProviderEntityType() || boughtBuilder
                            .getProviderEntityType() != EntityType.PHYSICAL_MACHINE_VALUE)   {
                continue;
            }
            setAccessCapacitiesForProvider(boughtBuilder.getProviderId(), iopsCapacityPerHost);
        }
    }

    @Nonnull
    private static CommoditySoldDTO.Builder getSoldStorageCommodityBuilder(
                    @Nonnull Builder storage, @Nonnull CommodityType type)
                                    throws EntityApplicatorException {
        Collection<CommoditySoldDTO.Builder> comms = SettingApplicator
                .getCommoditySoldBuilders(storage, type);
        if (CollectionUtils.isEmpty(comms)) {
            throw new EntityApplicatorException("vSAN storage '" + storage.getDisplayName()
                    + "' is missing " + type + " commodity");
        }

        if (comms.size() > 1) {
            throw new EntityApplicatorException(
                    "vSAN storage '" + storage.getDisplayName() + "' has " + comms.size()
                            + " " + type + " commodities, but should have one.");
        }

        return comms.iterator().next();
    }

    /**
     * Get the disk capacity of the vSAN physical machine with the largest disk
     * capacity.
     *
     * @param storage The VSAN storage which sells the StorageAmount.
     * @return The largest physical machine's disk capacity.
     */
    private static double getLargestHciHostRawDiskCapacity(@Nonnull Builder storage) {
        double result = 0;

        for (CommoditiesBoughtFromProvider.Builder commBought : storage
                .getCommoditiesBoughtFromProvidersBuilderList()) {
            for (CommodityBoughtDTO comm : commBought.getCommodityBoughtList()) {
                if (comm.getCommodityType().getType() == CommodityType.STORAGE_AMOUNT.getNumber()) {
                    // Used is always equals to Capacity for vSAN hosts
                    result = Math.max(result, comm.getUsed());
                }
            }
        }

        return result;
    }

    private static long countHCIHosts(@Nonnull Builder storage)  {
        return storage.getCommoditiesBoughtFromProvidersBuilderList()
                        .stream()
                        .flatMap(cblb ->
                            cblb.getCommodityBoughtList().stream()
                            .filter(comm -> comm.getCommodityType().getType() ==
                                CommodityType.STORAGE_ACCESS.getNumber()))
                        .count();
    }

    private static double getNumericSetting(@Nonnull Map<EntitySettingSpecs, Setting> settings,
            @Nonnull EntitySettingSpecs spec) throws EntityApplicatorException {
        Setting setting = settings.get(spec);

        if (setting == null || !setting.hasNumericSettingValue()) {
            throw new EntityApplicatorException(
                    "The numeric setting " + spec.getDisplayName() + " is missing");
        }

        return setting.getNumericSettingValue().getValue();
    }

    private static boolean getBooleanSetting(@Nonnull Map<EntitySettingSpecs, Setting> settings,
            @Nonnull EntitySettingSpecs spec) throws EntityApplicatorException {
        Setting setting = settings.get(spec);

        if (setting == null || !setting.hasBooleanSettingValue()) {
            throw new EntityApplicatorException(
                    "The boolean setting " + spec.getDisplayName() + " is missing");
        }

        return setting.getBooleanSettingValue().getValue();
    }


    private void setAccessCapacitiesForProvider(long providerId, double iopsCapacityPerHost)
                    throws EntityApplicatorException {
        Optional<TopologyEntity> provider = graph.getEntity(providerId);
        if (!provider.isPresent())  {
            throw new EntityApplicatorException("No provider with ID " + providerId + " in Topology Graph.");
        }
        for (CommoditySoldDTO.Builder soldBuilder :
                provider.get().getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList())  {
            if (soldBuilder.getCommodityType().getType() ==
                            CommodityType.STORAGE_ACCESS.getNumber())    {
                soldBuilder.setCapacity(iopsCapacityPerHost);
            }
        }
    }
}
