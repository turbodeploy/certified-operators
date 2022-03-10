package com.vmturbo.stitching.poststitching;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.IopsItemNames;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;

/**
 * This class is used to calculate the IOPS capacity of a Logical Pool, Storage Controller, or
 * Disk Array using information about the number and type of disks within the entity as well as
 * configured IOPS capacities for each type of disk.
 *
 * This calculation is done when the user or the probe does not supply a suitable IOPS capacity.
 * The values used for the calculation (the IOPS capacities of each specific disk type, as well as
 * the factors to multiply capacity in the case of hybrid or flashAvailable storage) are stored in
 * consul, which is not user-modifiable. However, the master IOPS Capacity setting can be set by
 * the user, in which case it will override this calculation.
 *
 * For more information about Storage Access (IOPS) Capacity, as well as Storage Latency, see the
 * documentation at https://vmturbo.atlassian.net/wiki/x/DYEKFw
 */
public class DiskCapacityCalculator {

    private static final Logger logger = LogManager.getLogger();

    /* The constant Strings and regexes below are used to retrieve information about the number
       and type of iopsItems in an entity. All of this information is currently in the form of a
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

    private static final Pattern DISK_COUNT_PATTERN = Pattern.compile(
        "\\s*\\{\\s*iopsItemName: \\\"(NUM_[\\d\\w_]+)\\\"\\s*iopsItemValue: ([\\d]+)[\\s\\}]*"
    );

    private static final String HYBRID_PATTERN = "hybrid: true";
    private static final String FLASH_PATTERN = "flashAvailable: true";

    private final double hybridDiskIopsFactor;
    private final double flashAvailableDiskIopsFactor;

    private final ImmutableMap<IopsItemNames, Double> diskTypeMap;

    private static final ImmutableBiMap<IopsItemNames, EntitySettingSpecs> iopsItemNamesToIopsSettingsSpecs =
                    new ImmutableBiMap.Builder<IopsItemNames, EntitySettingSpecs>()
                        .put(IopsItemNames.NUM_SSD, EntitySettingSpecs.DiskCapacitySsd)
                        .put(IopsItemNames.NUM_7200_DISKS, EntitySettingSpecs.DiskCapacity7200)
                        .put(IopsItemNames.NUM_10K_DISKS, EntitySettingSpecs.DiskCapacity10k)
                        .put(IopsItemNames.NUM_15K_DISKS, EntitySettingSpecs.DiskCapacity15k)
                        .put(IopsItemNames.NUM_VSERIES_DISKS, EntitySettingSpecs.DiskCapacityVSeries)
                        .build();

    public DiskCapacityCalculator(final double diskIopsCapacitySsd,
                                  final double diskIopsCapacity7200Rpm,
                                  final double diskIopsCapacity10kRpm,
                                  final double diskIopsCapacity15kRpm,
                                  final double diskIopsCapacityVseriesLun,
                                  final double arrayIopsCapacityFactor,
                                  final double hybridDiskIopsFactor,
                                  final double flashAvailableDiskIopsFactor) {
        if (hybridDiskIopsFactor > 0) {
            this.hybridDiskIopsFactor = hybridDiskIopsFactor;
        } else {
            logger.warn("hybridDiskIopsFactor must be greater than 0. Ignoring invalid value " +
                hybridDiskIopsFactor + " and using 1.0 instead.");
            this.hybridDiskIopsFactor = 1;
        }
        if (flashAvailableDiskIopsFactor > 0) {
            this.flashAvailableDiskIopsFactor = flashAvailableDiskIopsFactor;
        } else {
            logger.warn("flashAvailableDiskIopsFactor must be greater than 0. Ignoring invalid " +
                "value " + flashAvailableDiskIopsFactor + " and using 1.0 instead.");
            this.flashAvailableDiskIopsFactor = 1;
        }

        diskTypeMap = ImmutableMap.<IopsItemNames, Double>builder()
            .put(IopsItemNames.NUM_10K_DISKS, diskIopsCapacity10kRpm)
            .put(IopsItemNames.NUM_15K_DISKS, diskIopsCapacity15kRpm)
            .put(IopsItemNames.NUM_7200_DISKS, diskIopsCapacity7200Rpm)
            .put(IopsItemNames.NUM_SSD, diskIopsCapacitySsd)
            .put(IopsItemNames.NUM_VSERIES_DISKS, diskIopsCapacityVseriesLun)
            .put(IopsItemNames.NUM_IOPS_SUPPORTED, arrayIopsCapacityFactor)
            .build();
    }

    /**
     * Calculate the IOPS capacity for an entity using the information contained in that entity's
     * DiskData property string. This includes counts for various types of disks as well as flags
     * that may be set.
     *
     * @param diskProperty the string to parse for information. For an example, see above.
     * @param settingsCollection helper to fetch settings from.
     * @param entity for which iops capacity is to be calculated.
     * @return the calculated capacity, which may be 0 or more
     */
    public double calculateCapacity(@Nonnull final String diskProperty,
                    @Nonnull EntitySettingsCollection settingsCollection,
                    @Nonnull TopologyEntity entity) {
        final Map<IopsItemNames, Integer> diskSettingsCounts = parseIopsItemData(diskProperty);
        final double flagFactor = parseFlagFactor(diskProperty);

        final double baseCapacity = diskSettingsCounts.entrySet().stream()
            .filter(entry -> entry.getValue() > 0)
            .mapToDouble(entry -> {
                double iopsValue = diskTypeMap.get(entry.getKey());
                // Fetch iops related settings as set from UI for Disk Array.
                if (entity.getEntityType() == EntityType.DISK_ARRAY_VALUE
                        && iopsItemNamesToIopsSettingsSpecs.containsKey(entry.getKey())) {
                    Optional<Setting> iopsValueOptional = settingsCollection.getEntitySetting(entity.getOid(),
                            iopsItemNamesToIopsSettingsSpecs.get(entry.getKey()));
                    if (iopsValueOptional.isPresent()
                            && iopsValueOptional.get().hasNumericSettingValue()) {
                        iopsValue = iopsValueOptional.get().getNumericSettingValue().getValue();
                    }
                }
                return iopsValue * entry.getValue().doubleValue();
            }
            ).sum();

        return baseCapacity * flagFactor;
    }

    /**
     * Retrieve disk counts for each type of disk from the property string retrieved from the
     * entity property map.
     *
     * @param property The string containing information about the number of disks of each type
     * @return a map with EntitySettingSpecs applying to each type of disk as the key and the
     *          number of disks of each type as the value.
     */
    private Map<IopsItemNames, Integer> parseIopsItemData(@Nonnull final String property) {

        return Stream.of(property.split("iopsItems"))
            .map(DISK_COUNT_PATTERN::matcher)
            .filter(Matcher::matches)
            .collect(Collectors.toMap(
                matcher -> IopsItemNames.valueOf(matcher.group(1)),
                matcher -> Integer.parseInt(matcher.group(2)),
                (a, b) -> a + b)
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

        for (String segment : property.split("iopsItems")) {
            if (segment.contains(HYBRID_PATTERN)) {
                return hybridDiskIopsFactor;
            }
            if (segment.contains(FLASH_PATTERN)) {
                return flashAvailableDiskIopsFactor;
            }
        }

        return 1;
    }
}
