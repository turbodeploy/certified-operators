package com.vmturbo.components.common.setting;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.AllEntityType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Enumeration for all the pre-built entity settings.
 */
public enum EntitySettingSpecs {

    /**
     * Move action automation mode.
     */
    Move("move", "Move", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), actionExecutionMode(), true),
    /**
     * Resize action automation mode.
     */
    Resize("resize", "Resize", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), actionExecutionMode(), true),
    /**
     * Suspend action automation mode.
     */
    Suspend("suspend", "Suspend", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE), actionExecutionMode(),
            true),
    /**
     * Provision action automation mode.
     */
    Provision("provision", "Provision", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE), actionExecutionMode(),
            true),
    /**
     * Reconfigure action automation mode (not executable).
     */
    Reconfigure("reconfigure", "Reconfigure", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.allOf(EntityType.class), nonExecutableActionMode(), true),
    /**
     * Activate action automation mode.
     */
    Activate("activate", "Activate", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), actionExecutionMode(), true),
    /**
     * Storage Move action automation mode.
     */
    StorageMove("storageMove", "Storage Move", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), actionExecutionMode(), true),
    /**
     * CPU utilization threshold.
     */
    CpuUtilization("cpuUtilization", "CPU Utilization",
            Arrays.asList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE, EntityType.STORAGE_CONTROLLER),
            numeric(20f, 100f, 100f), true),
    /**
     * Memory utilization threshold.
     */
    MemoryUtilization("memoryUtilization", "Memory Utilization",
            Arrays.asList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(1f, 100f, 100f), true),
    /**
     * IO throughput utilization threshold.
     */
    IoThroughput("ioThroughput", "IO Throughput",
            Arrays.asList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(10f, 100f, 50f), true),
    /**
     * Network througput utilization threshold.
     */
    NetThroughput("netThroughput", "Net Throughput",
            Arrays.asList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE, EntityType.SWITCH),
            new NumericSettingDataType(10f, 100f, 50f,
                    Collections.singletonMap(EntityType.SWITCH, 70f)), true),
    /**
     * Swapping utilization threshold.
     */
    SwappingUtilization("swappingUtilization", "Swapping Utilization",
            Arrays.asList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(0f, 100f, 20f), true),
    /**
     * Ready queu utilization threshold.
     */
    ReadyQueueUtilization("readyQueueUtilization", "Ready Queue Utilization",
            Arrays.asList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(0f, 100f, 50f), true),
    /**
     * Storage utilization threshould.
     */
    StorageAmountUtilization("storageAmountUtilization", "Storage Amount Utilization",
            Arrays.asList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.STORAGE_CONTROLLER), numeric(0f, 100f, 90f),
            true),
    /**
     * IOPS utilization threshould.
     */
    IopsUtilization("iopsUtilization", "IOPS Utilization",
            Arrays.asList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE), numeric(0f, 100f, 100f), true),
    /**
     * Storage latency utilization threshold.
     */
    LatencyUtilization("latencyUtilization", "Latency Utilization",
            Arrays.asList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE), numeric(0f, 100f, 100f), true),
    /**
     * CPU overprovisioned in percents.
     */
    CpuOverprovisionedPercentage("cpuOverprovisionedPercentage", "CPU Overprovisioned Percentage",
            Arrays.asList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(0f, 1000000f, 1000f), true),
    /**
     * Memory overprovisioned in percents.
     */
    MemoryOverprovisionedPercentage("memoryOverprovisionedPercentage",
            "Memory Overprovisioned Percentage",
            Arrays.asList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(0f, 1000000f, 1000f), true),

    /**
     * Storage amount overprovisioned factor in percents.
     */
    StorageOverprovisionedPercentage("storageOverprovisionedPercentage",
            "Storage Overprovisioned Percentage",
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL),
            numeric(1f, 1000f, 200f), true),
    /**
     * Desired utilization target.
     */
    UtilTarget("utilTarget", "Center",
            //path is needed for the UI to display this setting in a separate category
            Arrays.asList("advanced", "utilTarget"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE),
            numeric(0.0f/*min*/, 100.0f/*max*/, 70.0f/*default*/), true),

    /**
     * Desired utilization range.
     */
    TargetBand("targetBand", "Diameter",
            //path is needed for the UI to display this setting in a separate category
            Arrays.asList("advanced", "utilTarget"),
            SettingTiebreaker.BIGGER, /*this is related to the center setting. biggger diameter is more conservative*/
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(0.0f/*min*/, 100.0f/*max*/, 10.0f/*default*/), true),
    /**
     * IOPS capacity to set on the entity.
     */
    IOPSCapacity("iopsCapacity", "IOPS Capacity",
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.DISK_ARRAY),
            new NumericSettingDataType(20f, 1000000, 5000,
                    Collections.singletonMap(EntityType.DISK_ARRAY, 10_000f)), true),
    /**
     * Storage latency capacity to set on the entity.
     */
    LatencyCapacity("latencyCapacity", "Storage latency capacity [ms]", Collections.emptyList(),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.STORAGE), numeric(1f, 2000f, 100f),
            true),
    /**
     * IOPS capacity to be set to disk arrays with SSD disks.
     */
    DiskCapacitySsd("diskCapacitySsd", "SSD Disk IOPS Capacity", Collections.emptyList(),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.DISK_ARRAY),
            numeric(20f, 1_000_000f, 5000f), true),
    /**
     * IOPS capacity to be set to disk arrays with 7.2 RPM disks.
     */
    DiskCapacity7200("diskCapacity7200", "7.2k RPM Disk IOPS Capacity", Collections.emptyList(),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.DISK_ARRAY),
            numeric(20f, 100_000f, 800f), true),
    /**
     * IOPS capacity to be set to disk arrays with 10k RPM disks.
     */
    DiskCapacity10k("diskCapacity10k", "10k RPM Disk IOPS Capacity", Collections.emptyList(),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.DISK_ARRAY),
            numeric(20f, 100_000f, 1200f), true),
    /**
     * IOPS capacity to be set to disk arrays with 15k RPM disks.
     */
    DiskCapacity15k("diskCapacity15k", "15k RPM Disk IOPS Capacity", Collections.emptyList(),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.DISK_ARRAY),
            numeric(20f, 100_000f, 1600f), true),
    /**
     * IOPS capacity to be set to VSeries LUN.
     */
    DiskCapacityVSeries("diskCapacityVSeries", "VSeries LUN IOPS Capacity", Collections.emptyList(),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.DISK_ARRAY),
            numeric(20f, 1_000_000f, 5000f), true),
    /**
     * Ignore High Availability(HA).
     */
    IgnoreHA("ignoreHa", "Ignore High Availability", Collections.emptyList(),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.PHYSICAL_MACHINE),
            new BooleanSettingDataType(false), true),

    /**
     * Virtual CPU Increment.
     */
    VcpuIncrement("usedIncrement_VCPU", "Increment constant for VCPU [MHz]",
            Arrays.asList("resizeRecommendationsConstants"),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(0.0f/*min*/, 1000000.0f/*max*/, 1800.0f/*default*/), true),

    /**
     * Virtual Memory Increment.
     */
    VmemIncrement("usedIncrement_VMEM", "Increment constant for VMem [MB]",
            Arrays.asList("resizeRecommendationsConstants"),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(0.0f/*min*/, 1000000.0f/*max*/, 1024.0f/*default*/), true),

    /**
     * Virtual Storage Increment.
     */
    VstorageIncrement("usedIncrement_VStorage", "Increment constant for VStorage [GB]",
            Arrays.asList("resizeRecommendationsConstants"),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(0.0f/*min*/, 999999.0f/*max*/, 999999.0f/*default*/), true);

    /**
     * Setting name to setting enumeration value map for fast access.
     */
    private static final Map<String, EntitySettingSpecs> SETTING_MAP;

    private final String name;
    private final String displayName;
    private final SettingTiebreaker tieBreaker;
    private final Set<EntityType> entityTypeScop;
    private final SettingDataStructure<?> dataStructure;
    private final List<String> categoryPath;
    private final boolean allowGlobalDefault;

    static {
        final EntitySettingSpecs[] settings = EntitySettingSpecs.values();
        final Map<String, EntitySettingSpecs> result = new HashMap<>(settings.length);
        for (EntitySettingSpecs setting : settings) {
            result.put(setting.getSettingName(), setting);
        }
        SETTING_MAP = Collections.unmodifiableMap(result);
    }

    EntitySettingSpecs(@Nonnull String name, @Nonnull String displayName,
            @Nonnull List<String> categoryPath, @Nonnull SettingTiebreaker tieBreaker,
            @Nonnull Set<EntityType> entityTypeScop, @Nonnull SettingDataStructure dataStructure,
            boolean allowGlobalDefault) {
        this.name = Objects.requireNonNull(name);
        this.displayName = Objects.requireNonNull(displayName);
        this.categoryPath = Objects.requireNonNull(categoryPath);
        this.tieBreaker = Objects.requireNonNull(tieBreaker);
        this.entityTypeScop = Objects.requireNonNull(entityTypeScop);
        this.dataStructure = Objects.requireNonNull(dataStructure);
        this.allowGlobalDefault = allowGlobalDefault;
    }

    /**
     * Returns setting name, identified by this enumeration value.
     *
     * @return setting name
     */
    @Nonnull
    public String getSettingName() {
        return name;
    }

    /**
     * Finds a setting (enumeration value) by setting name.
     *
     * @param settingName setting name
     * @return setting enumeration value or empty optional, if not setting found by the name
     * @throws NullPointerException if {@code settingName} is null
     */
    @Nonnull
    public static Optional<EntitySettingSpecs> getSettingByName(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return Optional.ofNullable(SETTING_MAP.get(settingName));
    }

    /**
     * Constructs Protobuf representation of setting specification.
     *
     * @return Protobuf representation
     */
    @Nonnull
    public SettingSpec createSettingSpec() {
        final EntitySettingScope.Builder scopeBuilder = EntitySettingScope.newBuilder();
        if (entityTypeScop.isEmpty()) {
            scopeBuilder.setAllEntityType(AllEntityType.getDefaultInstance());
        } else {
            scopeBuilder.setEntityTypeSet(EntityTypeSet.newBuilder()
                    .addAllEntityType(entityTypeScop.stream()
                            .map(EntityType::getNumber)
                            .collect(Collectors.toSet())));
        }
        final SettingSpec.Builder builder = SettingSpec.newBuilder()
                .setName(name)
                .setDisplayName(displayName)
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setTiebreaker(tieBreaker)
                        .setEntitySettingScope(scopeBuilder)
                .setAllowGlobalDefault(allowGlobalDefault));
        if (!categoryPath.isEmpty()) {
            builder.setPath(createCategoryPath());
        }
        dataStructure.build(builder);
        return builder.build();
    }

    /**
     * Method constructs setting category path object from the {@link #categoryPath} variable.
     *
     * @return {@link SettingCategoryPath} object.
     */
    @Nonnull
    private SettingCategoryPath createCategoryPath() {
        final ListIterator<String> categoryIterator =
                categoryPath.listIterator(categoryPath.size());
        SettingCategoryPathNode childNode = null;
        while (categoryIterator.hasPrevious()) {
            final SettingCategoryPathNode.Builder nodeBuilder =
                    SettingCategoryPathNode.newBuilder().setNodeName(categoryIterator.previous());
            if (childNode != null) {
                nodeBuilder.setChildNode(childNode);
            }
            childNode = nodeBuilder.build();
        }
        final SettingCategoryPath.Builder builder = SettingCategoryPath.newBuilder();
        if (childNode != null) {
            builder.setRootPathNode(childNode);
        }
        return builder.build();
    }

    @Nonnull
    private static SettingDataStructure<?> actionExecutionMode() {
        return new EnumSettingDataType<>(ActionMode.MANUAL);
    }

    @Nonnull
    private static SettingDataStructure<?> nonExecutableActionMode() {
        return new EnumSettingDataType<>(ActionMode.RECOMMEND, ActionMode.RECOMMEND);
    }

    @Nonnull
    private static SettingDataStructure<?> numeric(float min, float max, float defaultValue) {
        return new NumericSettingDataType(min, max, defaultValue);
    }

}
