package com.vmturbo.stitching.poststitching;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTOOrBuilder;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskArrayData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Post-stitching operation for the purpose of setting Storage Access capacities.
 *
 * If possible, the correct Storage Access capacity is calculated based on the number of disks of
 * each type multiplied by the capacity of each disk, as retrieved from settings. If this cannot
 * be done because there aren't settings for capacity by disk type or counts of each type of disk,
 * then the Storage Access capacity is set to the overall IOPS capacity as retrieved from settings.
 * If this setting is not available, the Storage Access capacity cannot be set.
 *
 * An entity may have related downstream entities. These entities' Storage Access capacities
 * should be set to be the same as the calculated capacity for the base entity.
 */
public abstract class StorageAccessPostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    /* The constant Strings and regexes below are used to retrieve information about the number
       and type of disks in the entity. All of this information is currently in the form of a
       String within the entity property map. An example string might look like this:

        |"hybrid: false
        |flashAvailable: false
        |disks {
        |  numDiskName: \"NUM_SSD\"
        |  numDisks: 0
        |}
        |disks {
        |  numDiskName: \"NUM_10K_DISKS\"
        |  numDisks: 5
        |}"
    */

    private static final String NUM_DISKS_KEY = DiskArrayData.newBuilder().getDescriptorForType()
        .findFieldByNumber(DiskArrayData.DISKCOUNTS_FIELD_NUMBER).getFullName();

    private static final Pattern DISK_COUNT_PATTERN = Pattern.compile(
        "\\s*\\{\\s*numDiskName: \\\"(NUM_[\\d\\w_]+)\\\"\\s*numDisks: ([\\d]+)[\\s\\}]*"
    );
    private static final Pattern FLAG_PATTERN =
        Pattern.compile("hybrid: (\\w+)\\s*flashAvailable: (\\w+)\\s*");

    private static final String NUM_DISKS_VSERIES_KEY = "NUM_VSERIES_DISKS";
    private static final String NUM_DISKS_SSD_KEY = "NUM_SSD";
    private static final String NUM_DISKS_10K_KEY = "NUM_10K_DISKS";
    private static final String NUM_DISKS_15K_KEY = "NUM_15K_DISKS";
    private static final String NUM_DISKS_7200_KEY = "NUM_7200_DISKS";

    private static final ImmutableMap<String, EntitySettingSpecs> DISK_TYPE_MAP = ImmutableMap.of(
        NUM_DISKS_SSD_KEY, EntitySettingSpecs.DiskCapacitySsd,
        NUM_DISKS_VSERIES_KEY, EntitySettingSpecs.DiskCapacityVSeries,
        NUM_DISKS_7200_KEY, EntitySettingSpecs.DiskCapacity7200,
        NUM_DISKS_10K_KEY, EntitySettingSpecs.DiskCapacity10k,
        NUM_DISKS_15K_KEY, EntitySettingSpecs.DiskCapacity15k
    );

    //factors to multiply capacity based on whether the entity is hybrid or has flash available.
    private static final double HYBRID_FACTOR = 1.5;
    private static final double FLASH_AVAILABLE_FACTOR = 1.3;

    /**
     * If the commodity is of type STORAGE_ACCESS and has unset capacity (which sometimes
     * presents as capacity == 0)
     */
    private static final Predicate<CommoditySoldDTOOrBuilder> COMMODITY_CAN_UPDATE = commodity ->
        commodity.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE &&
            (!commodity.hasCapacity() || commodity.getCapacity() == 0);

    @Nonnull
    @Override
    public TopologicalChangelog performOperation(@Nonnull final Stream<TopologyEntity> entities,
                             @Nonnull final EntitySettingsCollection settingsCollection,
                             @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities.filter(this::hasCommoditiesForUpdate).forEach(entity -> {
            final double calculatedFromDisks =
                calculateCapacityFromDisks(entity, settingsCollection);
            if (calculatedFromDisks > 0) {
                queueAllUpdates(calculatedFromDisks, entity, resultBuilder);
            } else {
                final Optional<Setting> mainIopsSetting =
                    settingsCollection.getEntitySetting(entity, EntitySettingSpecs.IOPSCapacity);
                if (mainIopsSetting.isPresent()) {
                    final double mainIopsValue =
                        mainIopsSetting.get().getNumericSettingValue().getValue();
                    queueAllUpdates(mainIopsValue, entity, resultBuilder);
                } else {
                    logger.warn("Could not calculate Storage Access for entity {} because " +
                        "no valid IOPS Capacity settings were found", entity.getOid());
                }
            }
        });

        return resultBuilder.build();
    }

    /**
     * Use settings and entity properties to determine the Storage Access capacity based on the
     * number of disks there are of each type and what capacity each type has.
     *
     * @param entity Entity to examine properties and settings of
     * @param settingsCollection Settings to use
     * @return the calculated capacity, or 0 if there is not enough information to determine.
     *         Since the capacity should never be 0, it serves as an error flag.
     */
    private double calculateCapacityFromDisks(@Nonnull final TopologyEntity entity,
                                      @Nonnull final EntitySettingsCollection settingsCollection) {
        final String diskProperty =
            entity.getTopologyEntityDtoBuilder().getEntityPropertyMapMap().get(NUM_DISKS_KEY);

        if (diskProperty == null) {
            return 0;
        }

        final Map<EntitySettingSpecs, Integer> diskSettingsCounts = parseDiskCounts(diskProperty);
        final double flagFactor = parseFlagFactor(diskProperty);

        final double baseCapacity = diskSettingsCounts.entrySet().stream()
            .filter(entry -> entry.getValue() > 0)
            .mapToDouble(entry -> {
                final Optional<Setting> setting =
                    settingsCollection.getEntitySetting(entity, entry.getKey());
                return (double) setting.map(numeric ->
                    numeric.getNumericSettingValue().getValue() * entry.getValue()).orElse(0f);
            }).sum();

        return baseCapacity * flagFactor;
    }

    /**
     * Get commodities to be updated for an entity. Filter those that are of type Storage Access
     * and have unset capacity.
     *
     * @param entity The entity to filter
     * @return a stream of commodity builders that are eligible for update
     */
    private Stream<CommoditySoldDTO.Builder> getCommoditiesForUpdate(
                                                            @Nonnull final TopologyEntity entity) {
        return entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
            .filter(COMMODITY_CAN_UPDATE);
    }

    /**
     * Determine if an entity has any commodities available for update.
     *
     * @param entity the entity to check commodities from
     * @return true if the entity has any updateable commodities, or false if it doesn't and
     *         therefore should not be processed.
     */
    boolean hasCommoditiesForUpdate(@Nonnull final TopologyEntity entity) {
        return entity.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream()
            .anyMatch(COMMODITY_CAN_UPDATE);
    }

    /**
     * Retrieve disk counts for each type of disk from the property string retrieved from the
     * entity property map.
     *
     * @param property The string containing information about the number of disks of each type
     * @return a map with EntitySettingSpecs applying to each type of disk as the key and the
     *          number of disks of each type as the value.
     */
    private Map<EntitySettingSpecs, Integer> parseDiskCounts(@Nonnull final String property) {

        return Stream.of(property.split("disks"))
            .filter(str -> str.contains("{"))
            .map(DISK_COUNT_PATTERN::matcher)
            .filter(Matcher::matches)
            .collect(Collectors.toMap(matcher -> DISK_TYPE_MAP.get(matcher.group(1)),
                matcher -> Integer.parseInt(matcher.group(2)))
            );
    }

    /**
     * Retrieve the factor for multiplying the capacity, based on flag values parsed from a
     * property string from the entity properties map.
     *
     * @param property The string containing information about flags that apply to the entity.
     * @return the factor by which to multiply the final capacity
     */
    private double parseFlagFactor(@Nonnull final String property) {

        final String relevantSegment = property.split("disks")[0];
        final Matcher patternMatcher = FLAG_PATTERN.matcher(relevantSegment);
        if (patternMatcher.matches()) {
            if (Boolean.parseBoolean(patternMatcher.group(1))) {
                return HYBRID_FACTOR;
            } else if (Boolean.parseBoolean(patternMatcher.group(2))) {
                return FLASH_AVAILABLE_FACTOR;
            }
        }
        return 1;
    }

    /**
     * For all the commodities eligible for updates on an entity, set the capacities of each of
     * them to a certain capacity. Then do the same for each related entity to which the base
     * entity's Storage Access capacity should propagate.
     *
     * @param capacity the capacity to set the commodities
     * @param entity the entity supplying the commodities
     * @param resultBuilder the resultBuilder that will queue all changes
     */
    private void queueAllUpdates(final double capacity, @Nonnull final TopologyEntity entity,
                            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        queueSingleUpdate(capacity, entity, resultBuilder);
        getRelatedEntities(entity).forEach(related ->
            queueSingleUpdate(capacity, related, resultBuilder)
        );
    }

    /**
     * For all the commodities eligible for updates on an entity, set the capacities of each of
     * them to a certain capacity.
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
            getCommoditiesForUpdate(entityForUpdate).forEach(commodity ->
                commodity.setCapacity(capacity)
            );
        });
    }

    /**
     * If an entity's Storage Access capacity should propagate to other entities' Storage Access
     * capacities, retrieve a stream of the entities affected.
     *
     * @param baseEntity the entity that will be supplying its capacity to the others
     * @return a stream of entities that depend on the base entity's capacity
     */
    abstract Stream<TopologyEntity> getRelatedEntities(@Nonnull final TopologyEntity baseEntity);

    /**
     * Post-stitching operation for the purpose of setting Storage Access capacities for Storage
     * Controller entities.
     *
     * If possible, the correct Storage Access capacity is calculated based on the number of disks
     * of each type multiplied by the capacity of each disk, as retrieved from settings. If this
     * cannot be done because there aren't settings for capacity by disk type or counts of each
     * type of disk, then the Storage Access capacity is set to the overall IOPS capacity as
     * retrieved from settings. If this setting is not available, the Storage Access capacity
     * cannot be set.
     *
     * Storage Controller entities do not propagate their Storage Access capacities to
     * any other entities.
     */
    public static class StorageControllerStorageAccessPostStitchingOperation extends
                                                            StorageAccessPostStitchingOperation {

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
                    @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.STORAGE_CONTROLLER);
        }

        @Override
        Stream<TopologyEntity> getRelatedEntities(@Nonnull final TopologyEntity baseEntity) {
            return Stream.empty();
        }
    }

    /**
     * Post-stitching operation for the purpose of setting Storage Access capacities in Disk Array
     * entities.
     *
     * If possible, the correct Storage Access capacity is calculated based on the number of disks
     * of each type multiplied by the capacity of each disk, as retrieved from settings. If this
     * cannot be done because there aren't settings for capacity by disk type or counts of each
     * type of disk, then the Storage Access capacity is set to the overall IOPS capacity as
     * retrieved from settings. If this setting is not available, the Storage Access capacity
     * cannot be set.
     *
     * A Disk Array entity should propagate its Storage Access capacity to any Storage entities
     * consuming from it. This operation must occur before the independent storage access
     * post-stitching operation.
     */
    public static class DiskArrayStorageAccessPostStitchingOperation extends
                                                            StorageAccessPostStitchingOperation {

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
                    @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.DISK_ARRAY);
        }

        @Override
        Stream<TopologyEntity> getRelatedEntities(@Nonnull final TopologyEntity baseEntity) {
            return baseEntity.getConsumers().stream()
                .filter(consumer -> consumer.getEntityType() == EntityType.STORAGE_VALUE &&
                    hasCommoditiesForUpdate(consumer));
        }
    }

    /**
     * Post-stitching operation for the purpose of setting Storage Access capacities in Logical
     * Pool entities with Disk Array providers.
     *
     * If possible, the correct Storage Access capacity is calculated based on the number of disks
     * of each type multiplied by the capacity of each disk, as retrieved from settings. If this
     * cannot be done because there aren't settings for capacity by disk type or counts of each
     * type of disk, then the Storage Access capacity is set to the overall IOPS capacity as
     * retrieved from settings. If this setting is not available, the Storage Access capacity
     * cannot be set.
     *
     * A Logical Pool entity should propagate its Storage Access capacity to any Storage entities
     * consuming from it. This operation must occur before the independent storage access
     * post-stitching operation.
     */
    public static class LogicalPoolStorageAccessPostStitchingOperation extends
                                                            StorageAccessPostStitchingOperation {
        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
                    @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.LOGICAL_POOL);
        }

        @Override
        Stream<TopologyEntity> getRelatedEntities(@Nonnull final TopologyEntity baseEntity) {
            return baseEntity.getConsumers().stream()
                .filter(consumer -> consumer.getEntityType() == EntityType.STORAGE_VALUE &&
                    hasCommoditiesForUpdate(consumer));
        }

        @Nonnull
        @Override
        public TopologicalChangelog performOperation(@Nonnull final Stream<TopologyEntity> entities,
                               @Nonnull final EntitySettingsCollection settingsCollection,
                               @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
            /* This operation should only be performed on entities with a Disk Array provider.
               Therefore, we filter out entities without this provider, then perform the operation
               in the same way as in the base class. */
            return super.performOperation(filterEntitiesByProvider(entities), settingsCollection,
                resultBuilder);
        }

        /**
         * Filter a stream of topology entities by whether each has at least one provider of
         * entity type Disk Array.
         *
         * @param entities the stream of entities to filter
         * @return the filtered stream of entities containing only those with at least one
         *          Disk Array provider
         */
        private Stream<TopologyEntity> filterEntitiesByProvider(
                                                @Nonnull final Stream<TopologyEntity> entities) {
            return entities.filter(entity -> entity.getProviders().stream().anyMatch(provider ->
                provider.getEntityType() == EntityType.DISK_ARRAY_VALUE));

        }
    }

}
