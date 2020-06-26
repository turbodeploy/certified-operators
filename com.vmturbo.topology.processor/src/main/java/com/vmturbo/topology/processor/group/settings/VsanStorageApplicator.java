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

    private static final double THRESHOLD_EFFECTIVE_CAPACITY = 1;
    private static final double RAID_FACTOR_DEFAULT = 1.0d;

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

        final double slackSpaceRatio = getSlackSpaceRatio(settings);
        final double iopsCapacityPerHost =  getNumericSetting(settings,
                EntitySettingSpecs.HciHostIopsCapacity);
        final boolean useCompressionSetting = getBooleanSetting(settings,
                EntitySettingSpecs.HciUseCompression);
        final double compressionRatio = useCompressionSetting
                ? getNumericSetting(settings, EntitySettingSpecs.HciCompressionRatio)
                : 1.0;
        // hostCapacityReservcation is handled in HCIPhysicalMachineEntityConstructor for Plans
        final double hostCapacityReservation = storage.getOrigin().hasPlanScenarioOrigin()
                || storage.getOrigin().hasReservationOrigin() ? 0
                : getNumericSetting(settings,
                EntitySettingSpecs.HciHostCapacityReservation);
        final double storageOverprovisioningCoefficient =
                getNumericSetting(settings,
                        EntitySettingSpecs.StorageOverprovisionedPercentage) / 100;
        final double raidFactor = computeRaidFactor(storage);
        final double hciUsablePercentage = raidFactor * slackSpaceRatio * compressionRatio * 100;
        try {
            applyToStorage(storage, raidFactor, slackSpaceRatio,
                    compressionRatio, iopsCapacityPerHost, hostCapacityReservation);
        } catch (EntityApplicatorException e) {
            logger.error("Error applying settings to Storage commodities for vSAN storage "
                    + storage.getDisplayName(), e);
        }
        applyToStorageHosts(storage, iopsCapacityPerHost,
                hciUsablePercentage, storageOverprovisioningCoefficient);
    }

    /**
     * Get the slack space ratio.
     *
     * @param settings setting map
     * @return Slack space ratio
     */
    private double getSlackSpaceRatio(@Nonnull Map<EntitySettingSpecs, Setting> settings) {
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
     */
    private static double computeRaidFactor(@Nonnull Builder storage) {
        if (!storage.hasTypeSpecificInfo()) {
            logger.error("Storage '{}' does not have Type Specific Info.  "
                    + "Assuming RAID0. ", storage.getDisplayName());
            return RAID_FACTOR_DEFAULT;
        }

        TypeSpecificInfo info = storage.getTypeSpecificInfo();

        if (!info.hasStorage()) {
            logger.error("Storage '{}' does not have Storage Info. "
                    + "Assuming RAID0. ", storage.getDisplayName() );
            return RAID_FACTOR_DEFAULT;
        }

        StorageInfo storageInfo = info.getStorage();

        if (!storageInfo.hasPolicy()) {
            logger.error("Storage '{}' does not have Storage Policy. "
                    + "Assuming RAID0. ", storage.getDisplayName() );
            return RAID_FACTOR_DEFAULT;
        }

        StoragePolicy policy = storageInfo.getPolicy();

        if (!policy.hasRedundancy()) {
            logger.error("Storage '{}' does not have Redundancy. "
                    + "Assuming RAID0. ", storage.getDisplayName() );
            return RAID_FACTOR_DEFAULT;
        }

        StorageRedundancyMethod raidType = policy.getRedundancy();

        if (!policy.hasFailuresToTolerate()) {
            logger.error("Storage '{}' does not have Failures To Tolerate. Assuming RAID0. ",
                    storage.getDisplayName() );
            return RAID_FACTOR_DEFAULT;
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

        logger.error("Invalid RAID type " + raidType + ", while computing the RAID factor for Storage "
                        + storage.getDisplayName() + " Assuming RAID0. ");
        return RAID_FACTOR_DEFAULT;
    }

    private void applyToStorage(Builder storage,
                    double raidFactor, double slackSpaceRatio,
                    double compressionRatio, double iopsCapacityPerHost,
                                double hostCapacityReservation)
                                    throws EntityApplicatorException {
        final long hciHostCount = countHCIHosts(storage);

        CommoditySoldDTO.Builder storageAmount = getSoldStorageCommodityBuilder(
                        storage, CommodityType.STORAGE_AMOUNT);
        double effectiveSACapacity = storageAmount.getCapacity();

        // subtract off space reserved for unavailable hosts, either failed
        // or in maintenance
        double largestCapacity = getLargestHciHostRawDiskCapacity(storage);
        effectiveSACapacity -= largestCapacity * hostCapacityReservation;

        // reduce by requested slack space
        effectiveSACapacity *= slackSpaceRatio;

        // reduce by RAID factor
        effectiveSACapacity *= raidFactor;

        // Adjust by compression ratio
        effectiveSACapacity *= compressionRatio;

        effectiveSACapacity = checkCapacityAgainstThreshold(effectiveSACapacity,
                CommodityType.STORAGE_AMOUNT, storage, hostCapacityReservation, hciHostCount);

        // Set capacity and used for Storage Amount.
        storageAmount.setUsed(storageAmount.getUsed() * compressionRatio * raidFactor);
        storageAmount.setCapacity(effectiveSACapacity);

        //Compute Storage Access capacity.
        double totalIOPSCapacity = (hciHostCount - hostCapacityReservation) * iopsCapacityPerHost;
        totalIOPSCapacity = checkCapacityAgainstThreshold(totalIOPSCapacity,
                CommodityType.STORAGE_ACCESS, storage, hostCapacityReservation, hciHostCount);

        //Set capacity for sold Storage Access.
        CommoditySoldDTO.Builder storageAccess = getSoldStorageCommodityBuilder(
                        storage, CommodityType.STORAGE_ACCESS);
        logger.trace("Set sold StorageAccess for {} capacity to {}, host count {}",
                storage.getDisplayName(), totalIOPSCapacity, hciHostCount);
        storageAccess.setCapacity(totalIOPSCapacity);

        //Set Storage Latency to 1 if Host Capacity reservation takes all hosts present.
        if (hostCapacityReservation >= hciHostCount) {
            CommoditySoldDTO.Builder storageLatency = getSoldStorageCommodityBuilder(
                            storage, CommodityType.STORAGE_LATENCY);
            logger.error("Setting sold Storage Latency capacity for {} to 1.0 because"
                            + " no hosts are available. Host capacity reservation: {}."
                            + " Number of hosts: {}", storage.getDisplayName(),
                            hostCapacityReservation, hciHostCount);
            storageLatency.setCapacity(THRESHOLD_EFFECTIVE_CAPACITY);
        }
    }

    private void applyToStorageHosts(Builder storage,
                    double iopsCapacityPerHost, double hciUsablePercentage,
                                     double storageOverprovisioningCoefficient) {

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

    /**
     * Checks the computed capacity against the threshold value.
     * @param capacity  computed capacity
     * @param commodityType type of commodity in question
     * @param storage   vSAN storage
     * @param hostCapacityReservation   Host Capacity Reservation setting value
     * @param hciHostCount  number of hosts selling commodities to vSAN storage
     * @return  the computed capacity if it's above the threshold or the threshold value
     */
    private double checkCapacityAgainstThreshold(double capacity, CommodityType commodityType,
                    Builder storage, double hostCapacityReservation, long hciHostCount)  {
        if (capacity < THRESHOLD_EFFECTIVE_CAPACITY) {
            logger.warn(
                    "Setting sold {} capacity for '{}' to 1.0 because computed capacity"
                            + " was {}. Host capacity reservation: {}. Number of hosts: {}",
                    commodityType, storage.getDisplayName(), capacity, hostCapacityReservation,
                    hciHostCount);
            return THRESHOLD_EFFECTIVE_CAPACITY;
        }
        return capacity;
    }

    private static double getNumericSettingDefault(EntitySettingSpecs settingSpecs) {
        return settingSpecs.getSettingSpec().getNumericSettingValueType().getDefault();
    }

    private static boolean getBooleanSettingDefault(EntitySettingSpecs settingSpecs) {
        return settingSpecs.getSettingSpec().getBooleanSettingValueType().getDefault();
    }

    private static double getNumericSetting(@Nonnull Map<EntitySettingSpecs, Setting> settings,
            @Nonnull EntitySettingSpecs spec) {
        Setting setting = settings.get(spec);

        if (setting == null || !setting.hasNumericSettingValue()) {
            logger.error("The numeric setting " + spec.getDisplayName() + " is missing.  Using defaults. ");
            return getNumericSettingDefault(spec);
        }

        return setting.getNumericSettingValue().getValue();
    }

    private static boolean getBooleanSetting(@Nonnull Map<EntitySettingSpecs, Setting> settings,
            @Nonnull EntitySettingSpecs spec) {
        Setting setting = settings.get(spec);

        if (setting == null || !setting.hasBooleanSettingValue()) {
            logger.error("The boolean setting " + spec.getDisplayName() + " is missing. Using defaults. ");
            return getBooleanSettingDefault(spec);
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
