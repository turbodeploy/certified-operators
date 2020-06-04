package com.vmturbo.stitching.poststitching;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTOOrBuilder;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeIopsData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskArrayData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.LogicalPoolData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageControllerData;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Post-stitching operation for the purpose of setting Storage Access capacities in Disk Array,
 * Storage Controller, and Logical Pool entities.
 *
 * If a user setting exists for an entity, that entity's Storage Access capacity is set accordingly.
 * Otherwise, if a probe has already supplied a capacity, that capacity is kept. Otherwise, a
 * capacity is calculated using whatever information is available about the entity's disk counts.
 * If there is no information available or the calculated capacity is zero, the Storage Access
 * capacity is set using a default setting if one is available.
 */
public class StorageAccessCapacityPostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    /* The constant Strings and regexes below are used to retrieve information about the number
       and type of disks in the entity. All of this information is currently in the form of a
       String within the entity property map. An example string might look like this:

        |"hybrid: false
        |flashAvailable: false
        |iopsItems {
        |  iopsItemName: \"NUM_SSD\"
        |  iopsItemValue: 0
        |}
        |iopsItems {
        |  iopsItemName: \"NUM_10K_DISKS\"
        |  iopsItemValue: 5
        |}"
    */

    // map from entity type to the diskCounts key (which will be used to look up property string
    // value from the entity properties map), for example: for storage controller the key is:
    // "common_dto.EntityDTO.StorageControllerData.diskCounts"
    public static final Map<Integer, String> DISK_COUNTS_KEY_BY_ENTITY_TYPE = ImmutableMap.of(
            EntityType.DISK_ARRAY_VALUE, DiskArrayData.getDefaultInstance().getDescriptorForType()
                    .findFieldByNumber(DiskArrayData.IOPSCOMPUTEDATA_FIELD_NUMBER).getFullName(),
            EntityType.LOGICAL_POOL_VALUE, LogicalPoolData.getDefaultInstance().getDescriptorForType()
                    .findFieldByNumber(LogicalPoolData.IOPSCOMPUTEDATA_FIELD_NUMBER).getFullName(),
            EntityType.STORAGE_CONTROLLER_VALUE, StorageControllerData.getDefaultInstance().getDescriptorForType()
                    .findFieldByNumber(StorageControllerData.IOPSCOMPUTEDATA_FIELD_NUMBER).getFullName()
    );

    private Predicate<CommoditySoldDTOOrBuilder> IS_STORAGE_ACCESS = commodity ->
        commodity.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE;

    private final EntityType scopeType;
    private final DiskCapacityCalculator diskCapacityCalculator;

    public StorageAccessCapacityPostStitchingOperation(@Nonnull final EntityType scopeType,
                                                       @Nonnull final DiskCapacityCalculator diskCapacityCalculator) {
        this.scopeType = Objects.requireNonNull(scopeType);
        this.diskCapacityCalculator = Objects.requireNonNull(diskCapacityCalculator);
    }

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(scopeType);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(@Nonnull final Stream<TopologyEntity> entities,
                             @Nonnull final EntitySettingsCollection settingsCollection,
                             @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities.forEach(entity -> {
            final Optional<Setting> userSetting =
                settingsCollection.getEntityUserSetting(entity, EntitySettingSpecs.IOPSCapacity);
            if (userSetting.isPresent()) {
                queueSingleUpdate(userSetting.get().getNumericSettingValue().getValue(), entity, resultBuilder);
            } else if (hasCommoditiesUnset(entity)) {
                final double calculatedCapacity = shouldCalculateFromHostedEntities(entity)
                        ? calculateCapacityFromHostedEntities(entity)
                        : calculateCapacityFromDisks(entity, settingsCollection);

                if (calculatedCapacity > 0) {
                    queueSingleUpdate(calculatedCapacity, entity, resultBuilder);
                } else {
                    final Optional<Setting> defaultSetting =
                        settingsCollection.getEntitySetting(entity, EntitySettingSpecs.IOPSCapacity);
                    if (defaultSetting.isPresent()) {
                        final double defaultValue =
                            defaultSetting.get().getNumericSettingValue().getValue();
                        queueSingleUpdate(defaultValue, entity, resultBuilder);
                    } else {
                        // TODO switched this from warn to debug to reduce logging load - does it need more visibility?
                        logger.debug("Could not set Storage Access capacity for entity {} ({}) " +
                                "because no valid IOPS Capacity settings were found",
                            entity.getOid(), entity.getDisplayName());
                    }
                }
            }
        });

        return resultBuilder.build();
    }

    @Nonnull
    @Override
    public String getOperationName() {
        return getClass().getSimpleName() + "_" + scopeType;
    }

    /**
     * Checks whether the storage access capacity for this entity should be calculated based on
     * its hosted entities.
     *
     * @param entity the TopologyEntity to check for
     * @return whether capacity should be calculated from hosted entities
     */
    private boolean shouldCalculateFromHostedEntities(@Nonnull final TopologyEntity entity) {
        String diskCountsKey = DISK_COUNTS_KEY_BY_ENTITY_TYPE.get(entity.getEntityType());
        if (diskCountsKey == null) {
            return false;
        }

        String diskCountProperty = entity.getTopologyEntityDtoBuilder()
                .getEntityPropertyMapMap().get(diskCountsKey);
        if (diskCountProperty == null) {
            return false;
        }
        try {
            // parse the text-format message "ComputeIopsData" into "ComputeIopsData" proto java object
            // for example: parse text "calculateFromHostedEntities: true\n" to ComputeIopsData
            // java object with a field "calculateFromHostedEntities" which is set to true
            ComputeIopsData.Builder diskCountData = ComputeIopsData.newBuilder();
            TextFormat.getParser().merge(diskCountProperty, diskCountData);
            return diskCountData.getCalculateFromHostedEntities();
        } catch (ParseException e) {
            logger.error("Invalid string value for ComputeIopsData: {}", diskCountProperty);
            return false;
        }
    }

    /**
     * Calculate storage access capacity for the given entity by summing the capacities of all the
     * consumers' sold StorageAccess commodity.
     *
     * @param entity the TopologyEntity to calculate storage access capacity for
     * @return the calculated storage access capacity for given entity
     */
    private double calculateCapacityFromHostedEntities(@Nonnull final TopologyEntity entity) {
        return entity.getConsumers().stream()
                .mapToDouble(consumer -> consumer.getTopologyEntityDtoBuilder()
                        .getCommoditySoldListList().stream()
                        .filter(IS_STORAGE_ACCESS)
                        .filter(comm -> comm.hasCapacity() && comm.getCapacity() > 0)
                        .map(CommoditySoldDTO::getCapacity)
                        .findAny().orElse(0d))
                .sum();
    }

    /**
     * Use entity properties to determine the Storage Access capacity based on the
     * number of disks there are of each type and what capacity each type has.
     *
     * @param entity Entity to examine
     * @param settingsCollection helper to fetch settings from.
     * @return the calculated capacity, or 0 if there is not enough information to determine.
     *         Since the capacity should never be 0, it serves as an error flag.
     */
    private double calculateCapacityFromDisks(@Nonnull final TopologyEntity entity,
                    EntitySettingsCollection settingsCollection) {
        String disksKey = DISK_COUNTS_KEY_BY_ENTITY_TYPE.get(entity.getEntityType());
        if (disksKey == null) {
            return 0;
        }

        String diskProperty = entity.getTopologyEntityDtoBuilder()
                .getEntityPropertyMapMap().get(disksKey);
        if (diskProperty == null) {
            return 0;
        }

        return diskCapacityCalculator.calculateCapacity(diskProperty,
                        settingsCollection, entity);
    }

    /**
     * Determine if an entity has any Storage Access commodities with capacity unset.
     *
     * @param entity the entity to check commodities from
     * @return true if the entity has any Storage Access commodities without capacity set.
     */
    private boolean hasCommoditiesUnset(@Nonnull final TopologyEntity entity) {
        return entity.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
            .filter(IS_STORAGE_ACCESS)
            .anyMatch(commodity -> !commodity.hasCapacity() || commodity.getCapacity() == 0);
    }

    /**
     * Set all the Storage Access commodities sold by an entity to a certain capacity.
     *
     * @param capacity the capacity to set the commodities
     * @param entity the entity supplying the commodities
     * @param resultBuilder the resultBuilder that will queue the change
     */
    private void queueSingleUpdate(final double capacity, @Nonnull final TopologyEntity entity,
                              @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        resultBuilder.queueUpdateEntityAlone(entity, entityForUpdate -> {
            logger.debug("Setting Storage Access capacity to {} on entity {}", capacity,
                entityForUpdate.getOid());
            entityForUpdate.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
                .filter(IS_STORAGE_ACCESS)
                .forEach(commodity -> commodity.setCapacity(capacity)
            );
        });
    }

}
