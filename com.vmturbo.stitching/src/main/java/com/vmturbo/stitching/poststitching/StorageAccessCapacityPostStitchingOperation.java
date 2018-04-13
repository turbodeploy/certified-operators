package com.vmturbo.stitching.poststitching;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTOOrBuilder;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskArrayData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.NumDiskNames;
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

    private static final String NUM_DISKS_VSERIES_KEY = NumDiskNames.NUM_VSERIES_DISKS.name();
    private static final String NUM_DISKS_SSD_KEY = NumDiskNames.NUM_SSD.name();
    private static final String NUM_DISKS_10K_KEY = NumDiskNames.NUM_10K_DISKS.name();
    private static final String NUM_DISKS_15K_KEY = NumDiskNames.NUM_15K_DISKS.name();
    private static final String NUM_DISKS_7200_KEY = NumDiskNames.NUM_7200_DISKS.name();

    private static final ImmutableMap<String, EntitySettingSpecs> DISK_TYPE_MAP = ImmutableMap.of(
        NUM_DISKS_SSD_KEY, EntitySettingSpecs.DiskCapacitySsd,
        NUM_DISKS_VSERIES_KEY, EntitySettingSpecs.DiskCapacityVSeries,
        NUM_DISKS_7200_KEY, EntitySettingSpecs.DiskCapacity7200,
        NUM_DISKS_10K_KEY, EntitySettingSpecs.DiskCapacity10k,
        NUM_DISKS_15K_KEY, EntitySettingSpecs.DiskCapacity15k
    );

    //factors to multiply capacity based on whether the entity is hybrid or has flash available.
    //todo: OM-34198 these will be set externally just like disk-specific capacities
    private static final double HYBRID_FACTOR = 1.5;
    private static final double FLASH_AVAILABLE_FACTOR = 1.3;

    private Predicate<CommoditySoldDTOOrBuilder> IS_STORAGE_ACCESS = commodity ->
        commodity.getCommodityType().getType() == CommodityType.STORAGE_ACCESS_VALUE;

    private final EntityType scopeType;

    public StorageAccessCapacityPostStitchingOperation(@Nonnull final EntityType scopeType) {
        this.scopeType = Objects.requireNonNull(scopeType);
    }

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(scopeType);
    }

    @Nonnull
    @Override
    public TopologicalChangelog performOperation(@Nonnull final Stream<TopologyEntity> entities,
                             @Nonnull final EntitySettingsCollection settingsCollection,
                             @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities.forEach(entity -> {
            final Optional<Setting> userSetting =
                settingsCollection.getEntityUserSetting(entity, EntitySettingSpecs.IOPSCapacity);
            if (userSetting.isPresent()) {
                queueSingleUpdate(userSetting.get().getNumericSettingValue().getValue(), entity, resultBuilder);
            } else if (hasCommoditiesUnset(entity)){
                final double calculatedFromDisks =
                    calculateCapacityFromDisks(entity, settingsCollection);

                if (calculatedFromDisks > 0) {
                    queueSingleUpdate(calculatedFromDisks, entity, resultBuilder);
                } else {
                    final Optional<Setting> defaultSetting =
                        settingsCollection.getEntitySetting(entity, EntitySettingSpecs.IOPSCapacity);
                    if (defaultSetting.isPresent()) {
                        final double defaultValue =
                            defaultSetting.get().getNumericSettingValue().getValue();
                        queueSingleUpdate(defaultValue, entity, resultBuilder);
                    } else {
                        logger.warn("Could not set Storage Access capacity for entity {} ({}) " +
                                "because no valid IOPS Capacity settings were found",
                            entity.getOid(), entity.getDisplayName());
                    }
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
                return setting.map(numeric -> numeric.getNumericSettingValue().getValue() *
                    entry.getValue().doubleValue()).orElse(0.0);
            }).sum();

        return baseCapacity * flagFactor;
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
     * Retrieve disk counts for each type of disk from the property string retrieved from the
     * entity property map.
     *
     * @param property The string containing information about the number of disks of each type
     * @return a map with EntitySettingSpecs applying to each type of disk as the key and the
     *          number of disks of each type as the value.
     */
    private Map<EntitySettingSpecs, Integer> parseDiskCounts(@Nonnull final String property) {
        final Map<EntitySettingSpecs, Integer> result = new HashMap<>();

        Stream.of(property.split("disks"))
            .filter(str -> str.contains("{"))
            .map(DISK_COUNT_PATTERN::matcher)
            .filter(Matcher::matches)
            .forEach(matcher -> {
                final EntitySettingSpecs key = DISK_TYPE_MAP.get(matcher.group(1));
                final int value = Integer.parseInt(matcher.group(2)) + result.getOrDefault(key, 0);
                result.put(key, value);
            });
        return result;
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
