package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.Map;

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

/**
 * Applicator for vSAN commodities.
 */
@ThreadSafe
public class VsanStorageApplicator implements SettingApplicator {

    private static final Logger logger = LogManager.getLogger();

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
            CommoditySoldDTO.Builder storageAmount = getStorageAmount(storage);
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
        } catch (EntityApplicatorException e) {
            logger.error("Error applying settings to Storage Amount commodity for vSAN storage "
                    + storage.getDisplayName(), e);
        }
    }

    @Nonnull
    private static CommoditySoldDTO.Builder getStorageAmount(@Nonnull Builder storage)
            throws EntityApplicatorException {
        Collection<CommoditySoldDTO.Builder> comms = SettingApplicator
                .getCommoditySoldBuilders(storage, CommodityType.STORAGE_AMOUNT);
        if (CollectionUtils.isEmpty(comms)) {
            throw new EntityApplicatorException("vSAN storage '" + storage.getDisplayName()
                    + "' is missing Storage Amount commodity");
        }

        if (comms.size() > 1) {
            throw new EntityApplicatorException(
                    "vSAN storage '" + storage.getDisplayName() + "' has " + comms.size()
                            + " Storage Amount commodities, but should have one.");
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
}
