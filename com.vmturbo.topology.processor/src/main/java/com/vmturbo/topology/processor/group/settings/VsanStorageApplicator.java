package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.common.protobuf.utils.HCIUtils;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData.StoragePolicy;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageRedundancyMethod;
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
        if (!HCIUtils.isVSAN(storage))  {
            return;
        }

        try {
            final double iopsCapacityPerHost = getNumericSetting(settings,
                            EntitySettingSpecs.HciHostIopsCapacity);
            final boolean useCompressionSetting = getBooleanSetting(settings,
                            EntitySettingSpecs.HciUseCompression);
            final double compressionRatio = useCompressionSetting
                            ? getNumericSetting(settings, EntitySettingSpecs.HciCompressionRatio)
                            : 1.0;
            final double slackSpaceRatio = getSlackSpaceRatio(settings);
            final double raidFactor = computeRaidFactor(storage);
            double hciUsablePercentage = raidFactor * slackSpaceRatio * compressionRatio * 100;

            applyToStorage(storage, settings, raidFactor, slackSpaceRatio,
                            compressionRatio, iopsCapacityPerHost);
            applyToStorageHosts(storage, settings, iopsCapacityPerHost, hciUsablePercentage);
        } catch (EntityApplicatorException e) {
            logger.error("Error applying settings to Storage commodities for vSAN storage {}",
                    storage.getDisplayName(), e);
        }
    }

    private double getSlackSpaceRatio(@Nonnull Map<EntitySettingSpecs, Setting> settings)
                    throws EntityApplicatorException {
        double slackSpacePercentage = getNumericSetting(settings,
                        EntitySettingSpecs.HciSlackSpacePercentage);
        return (100.0 - slackSpacePercentage) / 100.0;
    }

    /**
     * Compute the factor representing the RAID overhead based on the discovered
     * failures to tolerate and redundancy method. RAID 5 is selected if
     * failures to tolerate == 1, RAID 6 if failures to tolerate == 2.
     *
     * @param storage vSAN storage
     * @return a factor <= 1.0 by which the raw storage amount is reduced to get
     *         usable space
     * @throws EntityApplicatorException conversion error
     */
    private static double computeRaidFactor(@Nonnull Builder storage)
            throws EntityApplicatorException {
        if (!storage.hasTypeSpecificInfo()) {
            throw new EntityApplicatorException(
                    "Storage '" + storage.getDisplayName() + "' does not have Type Specific Info");
        }

        TypeSpecificInfo info = storage.getTypeSpecificInfo();

        if (!info.hasStorage()) {
            throw new EntityApplicatorException(
                    "Storage '" + storage.getDisplayName() + "' does not have Storage Info");
        }

        StorageInfo storageInfo = info.getStorage();

        if (!storageInfo.hasPolicy()) {
            throw new EntityApplicatorException(
                    "Storage '" + storage.getDisplayName() + "' does not have Storage Policy");
        }

        StoragePolicy policy = storageInfo.getPolicy();

        if (!policy.hasRedundancy()) {
            throw new EntityApplicatorException(
                    "Storage '" + storage.getDisplayName() + "' does not have Redundancy");
        }

        StorageRedundancyMethod raidType = policy.getRedundancy();

        if (!policy.hasFailuresToTolerate()) {
            throw new EntityApplicatorException("Storage '" + storage.getDisplayName()
                    + "' does not have Failures To Tolerate");
        }

        int failuresToTolerate = policy.getFailuresToTolerate();

        if (raidType == StorageRedundancyMethod.RAID0) {
            return 1;
        } else if (raidType == StorageRedundancyMethod.RAID1) {
            return 1.0 / (failuresToTolerate + 1);
        } else if (raidType == StorageRedundancyMethod.RAID5
                || raidType == StorageRedundancyMethod.RAID6) {
            return 1.0 / (1.0 + failuresToTolerate / (2.0 + failuresToTolerate));
        }

        throw new EntityApplicatorException(
                "Invalid RAID type " + raidType + ", while computing the RAID factor for Storage "
                        + storage.getDisplayName());
    }

    private void applyToStorage(Builder storage, Map<EntitySettingSpecs, Setting> settings,
                    double raidFactor, double slackSpaceRatio,
                    double compressionRatio, double iopsCapacityPerHost)
                                    throws EntityApplicatorException {
        final double hostCapacityReservation = getNumericSetting(settings,
                        EntitySettingSpecs.HciHostCapacityReservation);

        CommoditySoldDTO.Builder storageAmount = getSoldStorageCommodityBuilder(
                        storage, CommodityType.STORAGE_AMOUNT);
        double effectiveSACapacity = storageAmount.getCapacity();

        // subtract off space reserved for unavailable hosts, either failed
        // or in maintenance
        double largestCapacity = getLargestHciHostRawDiskCapacity(storage);
        effectiveSACapacity -= largestCapacity * hostCapacityReservation;

        if (effectiveSACapacity < 0) {
            throw new EntityApplicatorException(
                    "The effective capacity is negative or zero. hostCapacityReservation: "
                            + hostCapacityReservation);
        }

        // reduce by requested slack space
        effectiveSACapacity *= slackSpaceRatio;

        // reduce by RAID factor
        effectiveSACapacity *= raidFactor;

        // Adjust by compression ratio
        effectiveSACapacity *= compressionRatio;

        // Set capacity and used for Storage Amount.
        storageAmount.setUsed(storageAmount.getUsed() * compressionRatio);
        storageAmount.setCapacity(effectiveSACapacity);

        //Compute Storage Access capacity.
        final long hciHostCount = countHCIHosts(storage);
        final double totalIOPSCapacity = (hciHostCount - hostCapacityReservation) * iopsCapacityPerHost;

        //Set capacity for sold Storage Access.
        CommoditySoldDTO.Builder storageAccess = getSoldStorageCommodityBuilder(
                        storage, CommodityType.STORAGE_ACCESS);
        logger.trace("Set sold StorageAccess for {} capacity to {}, host count {}",
                storage.getDisplayName(), totalIOPSCapacity, hciHostCount);
        storageAccess.setCapacity(totalIOPSCapacity);
    }

    private void applyToStorageHosts(Builder storage, Map<EntitySettingSpecs, Setting> settings,
                    double iopsCapacityPerHost, double hciUsablePercentage)
                                    throws EntityApplicatorException {
        final double storageOverprovisioningCoefficient = getNumericSetting(settings,
                        EntitySettingSpecs.StorageOverprovisionedPercentage) / 100;

        //Set values for providers.
        for (CommoditiesBoughtFromProvider.Builder boughtBuilder :
                storage.getCommoditiesBoughtFromProvidersBuilderList())    {
            if (!boughtBuilder.hasProviderEntityType() || boughtBuilder
                            .getProviderEntityType() != EntityType.PHYSICAL_MACHINE_VALUE)   {
                continue;
            }
            Optional<TopologyEntity> provider = graph.getEntity(boughtBuilder.getProviderId());
            if (provider.isPresent())  {
                setAccessCapacityForProvider(provider.get(), iopsCapacityPerHost);
                setProvisionedCapacityForProvider(provider.get(), storageOverprovisioningCoefficient);
                setUtilizationThresholdForProvider(provider.get(), hciUsablePercentage);
            } else {
                logger.error("No provider with ID {} in Topology Graph.",
                                boughtBuilder.getProviderId());
            }
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

    private void setAccessCapacityForProvider(TopologyEntity provider,
                    double iopsCapacityPerHost)  {
        provider.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList()
            .stream().filter(soldBuilder -> soldBuilder.getCommodityType().getType()
                            == CommodityType.STORAGE_ACCESS_VALUE)
            .forEach(soldBuilder -> soldBuilder.setCapacity(iopsCapacityPerHost));
    }

    private void setProvisionedCapacityForProvider(TopologyEntity provider,
                    double storageOverprovisioningCoefficient)  {
        List<Double> storageAmountCapacities = provider.getTopologyEntityDtoBuilder()
            .getCommoditySoldListBuilderList().stream()
            .filter(soldCommodityBuilder -> soldCommodityBuilder.getCommodityType().getType()
                            == CommodityType.STORAGE_AMOUNT_VALUE
                        && soldCommodityBuilder.hasCapacity())
            .map(CommoditySoldDTO.Builder::getCapacity)
            .collect(Collectors.toList());

        if (storageAmountCapacities.size() != 1)  {
            logger.error("Wrong number of StorageAmount commodities sold "
                + "by provider {}: {} commodities",
                provider.getDisplayName(), storageAmountCapacities.size());
            return;
        }
        Double amountCapacity = storageAmountCapacities.iterator().next();

        provider.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
            .filter(soldBuilder -> soldBuilder.getCommodityType().getType()
                            == CommodityType.STORAGE_PROVISIONED_VALUE)
            .forEach(soldBuilder -> soldBuilder.setCapacity(amountCapacity *
                            storageOverprovisioningCoefficient));
    }

    private void setUtilizationThresholdForProvider(TopologyEntity provider, double value)   {
        provider.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
            .filter(soldBuilder -> soldBuilder.getCommodityType().getType()
                            == CommodityType.STORAGE_PROVISIONED_VALUE
                        || soldBuilder.getCommodityType().getType()
                            == CommodityType.STORAGE_AMOUNT_VALUE)
            .forEach(soldBuilder -> soldBuilder.setEffectiveCapacityPercentage(value));
    }
}
