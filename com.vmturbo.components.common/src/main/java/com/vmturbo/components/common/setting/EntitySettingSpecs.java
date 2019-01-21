package com.vmturbo.components.common.setting;

import static com.vmturbo.components.common.setting.SettingDTOUtil.createSettingCategoryPath;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.AllEntityType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
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
    Move("move", "Move / Compute Scale", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_MACHINE), actionExecutionMode(),
            true),
    /**
     * Resize action automation mode.
     */
    Resize("resize", "Resize", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_MACHINE), actionExecutionMode(),
            true),
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
            EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.DISK_ARRAY), actionExecutionMode(),
            true),
    /**
     * Reconfigure action automation mode (not executable).
     */
    Reconfigure("reconfigure", "Reconfigure", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), nonExecutableActionMode(), true),
    /**
     * Activate action automation mode.
     */
    Activate("activate", "Activate", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_MACHINE, EntityType.PHYSICAL_MACHINE), actionExecutionMode(),
            true),
    /**
     * Storage Move action automation mode.
     */
    StorageMove("storageMove", "Storage Move / Storage Scale", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), actionExecutionMode(), true),
    /**
     * CPU utilization threshold.
     */
    CpuUtilization("cpuUtilization", "CPU Utilization",
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE, EntityType.STORAGE_CONTROLLER),
            numeric(20f, 100f, 100f), true),
    /**
     * Memory utilization threshold.
     */
    MemoryUtilization("memoryUtilization", "Memory Utilization",
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(1f, 100f, 100f), true),
    /**
     * IO throughput utilization threshold.
     */
    IoThroughput("ioThroughput", "IO Throughput",
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(10f, 100f, 50f), true),
    /**
     * Network througput utilization threshold.
     */
    NetThroughput("netThroughput", "Net Throughput",
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE, EntityType.SWITCH),
            new NumericSettingDataType(10f, 100f, 50f,
                    Collections.singletonMap(EntityType.SWITCH, 70f)), true),
    /**
     * Swapping utilization threshold.
     */
    SwappingUtilization("swappingUtilization", "Swapping Utilization",
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(0f, 100f, 20f), true),
    /**
     * Ready queu utilization threshold.
     */
    ReadyQueueUtilization("readyQueueUtilization", "Ready Queue Utilization",
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(0f, 100f, 50f), true),
    /**
     * Storage utilization threshould.
     */
    StorageAmountUtilization("storageAmountUtilization", "Storage Amount Utilization",
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.DISK_ARRAY, EntityType.STORAGE_CONTROLLER),
            numeric(0f, 100f, 90f),
            true),
    /**
     * IOPS utilization threshould.
     */
    IopsUtilization("iopsUtilization", "IOPS Utilization",
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE), numeric(0f, 100f, 100f), true),
    /**
     * Storage latency utilization threshold.
     */
    LatencyUtilization("latencyUtilization", "Latency Utilization",
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE), numeric(0f, 100f, 100f), true),
    /**
     * CPU overprovisioned in percents.
     */
    CpuOverprovisionedPercentage("cpuOverprovisionedPercentage", "CPU Overprovisioned Percentage",
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(0f, 1000000f, 1000f), true),
    /**
     * Memory overprovisioned in percents.
     */
    MemoryOverprovisionedPercentage("memoryOverprovisionedPercentage",
            "Memory Overprovisioned Percentage",
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(0f, 1000000f, 1000f), true),

    /**
     * Storage amount overprovisioned factor in percents.
     */
    StorageOverprovisionedPercentage("storageOverprovisionedPercentage",
            "Storage Overprovisioned Percentage",
            Collections.emptyList(), SettingTiebreaker.SMALLER,
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
     * Resize target Utilization for VCPU.
     */
    ResizeTargetUtilizationVcpu("resizeTargetUtilizationVcpu", "Scaling Target VCPU Utilization",
            //path is needed for the UI to display this setting in a separate category
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.DATABASE, EntityType.DATABASE_SERVER, EntityType.CONTAINER),
            numeric(0.0f/*min*/, 100.0f/*max*/, 70.0f/*default*/), true),

    /**
     * Resize target Utilization for VMEM.
     */
    ResizeTargetUtilizationVmem("resizeTargetUtilizationVmem", "Scaling Target VMEM Utilization",
            //path is needed for the UI to display this setting in a separate category
            Collections.singletonList("utilizationThresholds"), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.DATABASE, EntityType.DATABASE_SERVER, EntityType.CONTAINER),
            numeric(0.0f/*min*/, 100.0f/*max*/, 70.0f/*default*/), true),

    /**
     * IOPS capacity to set on the entity.
     */
    IOPSCapacity("iopsCapacity", "IOPS Capacity",
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL,
                EntityType.STORAGE_CONTROLLER, EntityType.STORAGE),
            new NumericSettingDataType(20f, 1000000, 5000,
                    Collections.singletonMap(EntityType.DISK_ARRAY, 10_000f)), true),
    /**
     * Storage latency capacity to set on the entity.
     */
    LatencyCapacity("latencyCapacity", "Storage latency capacity [ms]", Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.STORAGE_CONTROLLER, EntityType.LOGICAL_POOL,
                EntityType.DISK_ARRAY),
            numeric(1f, 2000f, 100f), true),
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
            Collections.singletonList("resizeRecommendationsConstants"),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(0.0f/*min*/, 1000000.0f/*max*/, 1800.0f/*default*/), true),

    /**
     * Virtual Memory Increment.
     */
    VmemIncrement("usedIncrement_VMEM", "Increment constant for VMem [MB]",
            Collections.singletonList("resizeRecommendationsConstants"),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(0.0f/*min*/, 1000000.0f/*max*/, 1024.0f/*default*/), true),

    /**
     * Virtual Storage Increment.
     */
    VstorageIncrement("usedIncrement_VStorage", "Increment constant for VStorage [GB]",
            Collections.singletonList("resizeRecommendationsConstants"),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(0.0f/*min*/, 999999.0f/*max*/, 999999.0f/*default*/), true),

    /**
     * Storage Increment.
     */
    StorageIncrement("usedIncrement_StAmt", "Increment constant for Storage Amount [GB]",
            Collections.singletonList("resizeRecommendationsConstants"),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(0.0f/*min*/, 100000.0f/*max*/, 100.0f/*default*/), true),

    /**
     * Automation Policy for the Suspend Workflow. The value is the name of an
     * Orchestration workflow to invoke when a suspend action is generated and executed.
     */
    SuspendActionWorkflow("suspendActionWorkflow", "Suspend Workflow",
            Collections.singletonList("automation"),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE, EntityType.STORAGE),
            string(), true),

    /**
     * Automation Policy for the Provision Workflow. The value is the name of an
     * Orchestration workflow to invoke when a provision action is generated and executed.
     */
    ProvisionActionWorkflow("provisionActionWorkflow", "Provision Workflow",
            Collections.singletonList("automation"),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE, EntityType.STORAGE),
            string(), true),
    /**
     * Automation Policy for the Resize Workflow. The value is the name of an
     * Orchestration workflow to invoke when a resize action is generated and executed.
     */
    ResizeActionWorkflow("resizeActionWorkflow", "Resize Workflow",
            Collections.singletonList("automation"),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE),
            string(), true),

    /**
     * Response Time Capacity used by Application and Database.
     */
    ResponseTimeCapacity("responseTimeCapacity", "Response Time Capacity [ms]",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.APPLICATION, EntityType.DATABASE_SERVER),
            numeric(1.0f/*min*/, 31536000000000.0f/*max*/, 10000.0f/*default*/),
            true),

    /**
     * SLA Capacity used by Application and Database.
     */
    SLACapacity("slaCapacity", "SLA Capacity",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.APPLICATION, EntityType.DATABASE_SERVER),
            numeric(1.0f/*min*/, 31536000000000.0f/*max*/, 10000.0f/*default*/),
            true),

    /**
     * Transactions Capacity used by Application and Database.
     */
    TransactionsCapacity("transactionsCapacity", "Transactions Capacity",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.APPLICATION, EntityType.DATABASE_SERVER),
            numeric(1.0f/*min*/, 31536000000000.0f/*max*/, 10000.0f/*default*/),
            true),

    /**
     * Indicates whether to auto set the transaction capacity of an entity's commodity to the value
     * of the TransactionsCapacity setting or to calculate it as the max of the commodity's capacity,
     * used value, and the TransactionsCapacity setting.
     * Used by Application and Database.
     */
    AutoSetTransactionsCapacity("autoSetTransactionsCapacity", "Auto Set Transactions Capacity",
            Collections.emptyList(),
            SettingTiebreaker.BIGGER,
            EnumSet.of(EntityType.APPLICATION, EntityType.DATABASE_SERVER),
            new BooleanSettingDataType(false),
            true),

    IgnoreDirectories("ignoreDirectories", "Directories to ignore",
        Collections.emptyList(),
        SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.STORAGE),
        string("\\.dvsData.*|\\.snapshot.*|\\.vSphere-HA.*|\\.naa.*|\\.etc.*|lost\\+found.*|stCtlVM-.*"),
        true),

    IgnoreFiles("ignoreFiles", "Files to ignore",
        Collections.emptyList(),
        SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.STORAGE),
        string(),
        true),

    /**
     * This Action Script action is added as a temporary work-around for a bug in the UI.
     * The UI processes workflows as part of the 'actionScript' case - so at least one
     * 'actionScript' must be included.
     * TODO: remove this as part of fix OM-38669
     */
    ProvisionActionScript("provisionActionScript", "Provision",
            Collections.singletonList("actionScript"),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE, EntityType.STORAGE, EntityType.DISK_ARRAY),
            string(), true);

    private static final ImmutableSet<String> AUTOMATION_SETTINGS =
        ImmutableSet.of(
            EntitySettingSpecs.Activate.name,
            EntitySettingSpecs.Move.name,
            EntitySettingSpecs.Provision.name,
            EntitySettingSpecs.Reconfigure.name,
            EntitySettingSpecs.Resize.name,
            EntitySettingSpecs.StorageMove.name,
            EntitySettingSpecs.Suspend.name);

    /**
     * Default value for a String-type SettingDataStructure = empty String.
     */
    public static final String DEFAULT_STRING_VALUE = "";

    /**
     * Default regex for a String-type SettingDataStructure = matches anything.
     */
    public static final String MATCH_ANYTHING_REGEX = ".*";

    /**
     * Setting name to setting enumeration value map for fast access.
     */
    private static final Map<String, EntitySettingSpecs> SETTING_MAP;

    private final String name;
    private final String displayName;
    private final SettingTiebreaker tieBreaker;
    private final Set<EntityType> entityTypeScope;
    private final SettingDataStructure<?> dataStructure;
    private final List<String> categoryPath;
    private final boolean allowGlobalDefault;

    /**
     * The protobuf representation of this setting spec.
     */
    private final SettingSpec settingSpec;

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
            @Nonnull Set<EntityType> entityTypeScope, @Nonnull SettingDataStructure dataStructure,
            boolean allowGlobalDefault) {
        this.name = Objects.requireNonNull(name);
        this.displayName = Objects.requireNonNull(displayName);
        this.categoryPath = Objects.requireNonNull(categoryPath);
        this.tieBreaker = Objects.requireNonNull(tieBreaker);
        this.entityTypeScope = Objects.requireNonNull(entityTypeScope);
        this.dataStructure = Objects.requireNonNull(dataStructure);
        this.allowGlobalDefault = allowGlobalDefault;
        this.settingSpec = createSettingSpec();
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
     * Get the protobuf representation of this {@link EntitySettingSpecs}.
     *
     * @return A {@link SettingSpec} protobuf.
     */
    @Nonnull
    public SettingSpec getSettingSpec() {
        return settingSpec;
    }

    /**
     * Constructs Protobuf representation of setting specification.
     *
     * @return Protobuf representation
     */
    @Nonnull
    private SettingSpec createSettingSpec() {
        final EntitySettingScope.Builder scopeBuilder = EntitySettingScope.newBuilder();
        if (entityTypeScope.isEmpty()) {
            scopeBuilder.setAllEntityType(AllEntityType.getDefaultInstance());
        } else {
            scopeBuilder.setEntityTypeSet(EntityTypeSet.newBuilder()
                    .addAllEntityType(entityTypeScope.stream()
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
            builder.setPath(createSettingCategoryPath(categoryPath));
        }
        dataStructure.build(builder);
        return builder.build();
    }

    /**
     *  Check if the given setting spec name is an automation setting.
     *
     * @param specName Name of the setting spec.
     * @return Return true if the setting is an automation setting else return false.
     */
    public static boolean isAutomationSetting(@Nonnull String specName) {
        Objects.requireNonNull(specName);
        return AUTOMATION_SETTINGS.contains(specName);
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

    @Nonnull
    private static SettingDataStructure<?> string() {
        return new StringSettingDataType(DEFAULT_STRING_VALUE, MATCH_ANYTHING_REGEX);
    }

    @Nonnull
    private static SettingDataStructure<?> string(String defaultValue) {
        return new StringSettingDataType(defaultValue, MATCH_ANYTHING_REGEX);
    }

}
