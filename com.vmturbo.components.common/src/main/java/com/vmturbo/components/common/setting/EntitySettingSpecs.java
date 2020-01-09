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
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.AllEntityType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValueType.Type;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Enumeration for all the pre-built entity settings.
 */
public enum EntitySettingSpecs {

    /**
     * Move action automation mode.
     */
    Move("move", "Move / Compute Scale", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_MACHINE, EntityType.VIRTUAL_VOLUME,
                    EntityType.CONTAINER_POD, EntityType.CONTAINER,
                    EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL), actionExecutionModeSetToManual(), true),

    /**
     * Resize action automation mode.
     *
     * For VM, lets say we generate an action to resize some other commodity of  a VM. then it will use this setting.
     * Also, if we resize reservation attribute instead of capacity attribute, then it will use this.
     */
    Resize("resize", "Resize", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER,
                            EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL,
                            EntityType.APPLICATION_SERVER, EntityType.DATABASE_SERVER), actionExecutionModeSetToManual(), true),

    /**
     * Resize action automation mode for vcpu resize ups where the target capacity is between
     * {@link EntitySettingSpecs#ResizeVcpuMinThreshold} and {@link EntitySettingSpecs#ResizeVcpuMaxThreshold}.
     */
    ResizeVcpuUpInBetweenThresholds("resizeVcpuUpInBetweenThresholds", "VCPU Resize Up", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), actionExecutionModeSetToManual(), true),

    /**
     * Resize action automation mode for vcpu resize downs where the target capacity is between
     * {@link EntitySettingSpecs#ResizeVcpuMinThreshold} and {@link EntitySettingSpecs#ResizeVcpuMaxThreshold}.
     */
    ResizeVcpuDownInBetweenThresholds("resizeVcpuDownInBetweenThresholds", "VCPU Resize Down", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), actionExecutionModeSetToManual(), true),

    /**
     * Resize action automation mode for vcpu resizes where the target capacity is above the max threshold value {@link EntitySettingSpecs#ResizeVcpuMaxThreshold}.
     */
    ResizeVcpuAboveMaxThreshold("resizeVcpuAboveMaxThreshold", "VCPU Resize Above Max", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), actionExecutionModeSetToRecommend(), true),

    /**
     * Resize action automation mode for vcpu resizes where the target capacity is below the min value {@link EntitySettingSpecs#ResizeVcpuMinThreshold}.
     */
    ResizeVcpuBelowMinThreshold("resizeVcpuBelowMinThreshold", "VCPU Resize Below Min", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), actionExecutionModeSetToRecommend(), true),

    /**
     * The minimum number of vcpu cores which is the threshold to decide automation mode.
     *
     */
    ResizeVcpuMinThreshold("resizeVcpuMinThreshold", "VCPU Resize Min Threshold (in Cores)",
            Collections.emptyList(), SettingTiebreaker.BIGGER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), numeric(0, 1000, 1), true),

    /**
     * The maximum number of vcpu cores which is the threshold to decide automation mode.
     */
    ResizeVcpuMaxThreshold("resizeVcpuMaxThreshold", "VCPU Resize Max Threshold (in Cores)",
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), numeric(0, 1000, 64), true),

    /**
     * Resize action automation mode for vmem resize ups where the target capacity is between
     * {@link EntitySettingSpecs#ResizeVmemMinThreshold} and {@link EntitySettingSpecs#ResizeVmemMaxThreshold}.
     */
    ResizeVmemUpInBetweenThresholds("resizeVmemUpInBetweenThresholds", "VMem Resize Up", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), actionExecutionModeSetToManual(), true),

    /**
     * Resize action automation mode for vmem resize downs where the target capacity is between
     * {@link EntitySettingSpecs#ResizeVmemMinThreshold} and {@link EntitySettingSpecs#ResizeVmemMaxThreshold}.
     */
    ResizeVmemDownInBetweenThresholds("resizeVmemDownInBetweenThresholds", "VMem Resize Down", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), actionExecutionModeSetToManual(), true),

    /**
     * Resize action automation mode for vmem resizes where the target capacity is above the max threshold value {@link EntitySettingSpecs#ResizeVmemMaxThreshold}.
     */
    ResizeVmemAboveMaxThreshold("resizeVmemAboveMaxThreshold", "VMem Resize Above Max", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), actionExecutionModeSetToRecommend(), true),

    /**
     * Resize action automation mode for vmem resizes where the target capacity is below the min value {@link EntitySettingSpecs#ResizeVmemMinThreshold}.
     */
    ResizeVmemBelowMinThreshold("resizeVmemBelowMinThreshold", "VMem Resize Below Min", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), actionExecutionModeSetToRecommend(), true),

    /**
     * The minimum number of vmem cores which is the threshold to decide automation mode.
     *
     */
    ResizeVmemMinThreshold("resizeVmemMinThreshold", "VMEM Resize Min Threshold (in MB)",
            Collections.emptyList(), SettingTiebreaker.BIGGER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), numeric(0, 1000000, 512), true),

    /**
     * The maximum number of vmem cores which is the threshold to decide automation mode.
     */
    ResizeVmemMaxThreshold("resizeVmemMaxThreshold", "VMEM Resize Max Threshold (in MB)",
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), numeric(0, 1000000, 131072), true),

    /**
     * Suspend action automation mode.
     */
    Suspend("suspend", "Suspend", Collections.emptyList(), SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.VIRTUAL_MACHINE,
            EntityType.CONTAINER_POD, EntityType.CONTAINER,
            EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL), actionExecutionModeSetToManual(), true),

    /**
     * Delete action automation mode.
     */
    Delete("delete", "Delete", Collections.emptyList(), SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_VOLUME), actionExecutionModeSetToManual(), true),

    /**
     * Provision action automation mode.
     */
    Provision("provision", "Provision", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.DISK_ARRAY,
                    EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_POD, EntityType.CONTAINER,
                    EntityType.LOGICAL_POOL, EntityType.STORAGE_CONTROLLER), actionExecutionModeSetToManual(), true),

    /**
     * Reconfigure action automation mode (not executable).
     */
    Reconfigure("reconfigure", "Reconfigure", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_POD), nonExecutableActionMode(), true),

    /**
     * Activate action automation mode.
     */
    Activate("activate", "Start", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.VIRTUAL_MACHINE,
                    EntityType.CONTAINER_POD, EntityType.CONTAINER,
                    EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL), actionExecutionModeSetToManual(), true),

    /**
     * Storage Move action automation mode.
     */
    StorageMove("storageMove", "Storage Move / Storage Scale", Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), actionExecutionModeSetToRecommend(), true),

    /**
     * CPU utilization threshold.
     */
    CpuUtilization("cpuUtilization", "CPU Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE, EntityType.STORAGE_CONTROLLER),
            numeric(0f, 1000000f, 100f), true),

    /**
     * Memory utilization threshold.
     */
    MemoryUtilization("memoryUtilization", "Memory Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(0f, 100f, 100f), true),

    /**
     * IO throughput utilization threshold.
     */
    IoThroughput("ioThroughput", "IO Throughput",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(0f, 100f, 50f), true),

    /**
     * Network throughput utilization threshold.
     */
    NetThroughput("netThroughput", "Net Throughput",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE, EntityType.SWITCH),
            new NumericSettingDataType(0f, 100f, 50f,
                    Collections.singletonMap(EntityType.SWITCH, 70f)), true),

    /**
     * Swapping utilization threshold.
     */
    SwappingUtilization("swappingUtilization", "Swapping Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(0f, 100f, 20f), true),

    /**
     * Ready queue utilization threshold.
     */
    ReadyQueueUtilization("readyQueueUtilization", "Ready Queue Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(0f, 100f, 50f), true),

    /**
     * Storage utilization threshold.
     */
    StorageAmountUtilization("storageAmountUtilization", "Storage Amount Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.DISK_ARRAY, EntityType.STORAGE_CONTROLLER),
            numeric(0f, 100f, 90f),
            true),

    /**
     * VCPURequest utilization threshold.
     * Setting VCPURequest utilization threshold to 0.9999 to avoid rounding errors due to
     * conversion from kubernetes millicores to MHz.
     */
    VCPURequestUtilization("vcpuRequestUtilization", "VCPU Request Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_POD),
            numeric(0f, 100f, 99.99f), true),

    /**
     * IOPS utilization threshold.
     */
    IopsUtilization("iopsUtilization", "IOPS Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE), numeric(0f, 100f, 100f), true),

    /**
     * Storage latency utilization threshold.
     */
    LatencyUtilization("latencyUtilization", "Latency Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE), numeric(0f, 100f, 100f), true),

    /**
     * CPU overprovisioned in percents.
     */
    CpuOverprovisionedPercentage("cpuOverprovisionedPercentage", "CPU Overprovisioned Percentage",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(1f, 1000000f, 30000f), true),

    /**
     * Memory overprovisioned in percents.
     */
    MemoryOverprovisionedPercentage("memoryOverprovisionedPercentage",
            "Memory Overprovisioned Percentage",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(1f, 1000000f, 1000f), true),

    /**
     * Storage amount overprovisioned factor in percents.
     */
    StorageOverprovisionedPercentage("storageOverprovisionedPercentage",
            "Storage Overprovisioned Percentage",
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL),
            numeric(1f, 1000000f, 200f), true),

    /**
     * Desired utilization target.
     */
    UtilTarget("utilTarget", "Center",
            //path is needed for the UI to display this setting in a separate category
            Arrays.asList(CategoryPathConstants.ADVANCED, CategoryPathConstants.UTILTARGET), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE),
            numeric(0.0f/*min*/, 100.0f/*max*/, 70.0f/*default*/), true),

    /**
     * Desired utilization range.
     */
    TargetBand("targetBand", "Diameter",
            //path is needed for the UI to display this setting in a separate category
            Arrays.asList(CategoryPathConstants.ADVANCED, CategoryPathConstants.UTILTARGET),
            SettingTiebreaker.BIGGER, /*this is related to the center setting. bigger diameter is more conservative*/
            EnumSet.of(EntityType.PHYSICAL_MACHINE), numeric(0.0f/*min*/, 100.0f/*max*/, 10.0f/*default*/), true),

    /**
     * Aggressiveness for business user.
     */
    PercentileAggressivenessBusinessUser("percentileAggressivenessBusinessUser",
            SettingConstants.AGGRESSIVENESS,
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.BUSINESS_USER),
            numeric(90.0f, 100.0f, 95.0f), true),

    /**
     * Aggressiveness for virtual machine.
     */
    PercentileAggressivenessVirtualMachine("percentileAggressivenessVirtualMachine",
            SettingConstants.AGGRESSIVENESS,
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(90.0f, 100.0f, 95.0f), true),

    /**
     * Min observation period for business user.
     */
    MinObservationPeriodVirtualMachine("minObservationPeriodVirtualMachine",
            "Min Observation Period",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(0.0f, 7.0f, 0.0f), true),

    /**
     * Max observation period for business user.
     */
    MaxObservationPeriodBusinessUser("maxObservationPeriodBusinessUser",
            SettingConstants.MAX_OBSERVATION_PERIOD,
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.BUSINESS_USER),
            numeric(7.0f, 90.0f, 30.0f), true),

    /**
     * Max observation period for virtual machine.
     */
    MaxObservationPeriodVirtualMachine("maxObservationPeriodVirtualMachine",
            SettingConstants.MAX_OBSERVATION_PERIOD,
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(7.0f, 90.0f, 30.0f), true),

    /**
     * Resize target Utilization for Image CPU.
     */
    ResizeTargetUtilizationImageCPU("resizeTargetUtilizationImageCPU",
            "Image CPU Target Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.BUSINESS_USER),
            numeric(1.0f, 100.0f, 100.0f), true),

    /**
     * Resize target Utilization for Image Mem.
     */
    ResizeTargetUtilizationImageMem("resizeTargetUtilizationImageMem",
            "Image Mem Target Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.BUSINESS_USER),
            numeric(1.0f, 100.0f, 100.0f), true),

    /**
     * Resize target Utilization for Image Storage.
     */
    ResizeTargetUtilizationImageStorage("resizeTargetUtilizationImageStorage",
            "Image Storage Target Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.BUSINESS_USER),
            numeric(1.0f, 100.0f, 100.0f), true),

    /**
     * Resize target Utilization for Net Throughput.
     */
    ResizeTargetUtilizationNetThroughput("resizeTargetUtilizationNetThroughput",
            "Scaling Target Net Throughput Utilization", Collections.emptyList(),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(1.0f, 100.0f, 70.0f), true),

    /**
     * Resize target Utilization for IO Throughput.
     */
    ResizeTargetUtilizationIoThroughput("resizeTargetUtilizationIoThroughput",
            "Scaling Target IO Throughput Utilization", Collections.emptyList(),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(1.0f, 100.0f, 70.0f), true),

    /**
     * Resize target Utilization for VCPU.
     */
    ResizeTargetUtilizationVcpu("resizeTargetUtilizationVcpu", "Scaling Target VCPU Utilization",
            //path is needed for the UI to display this setting in a separate category
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER, EntityType.DATABASE,
                    EntityType.DATABASE_SERVER),
            numeric(1.0f/*min*/, 100.0f/*max*/, 70.0f/*default*/), true),

    /**
     * Resize target Utilization for VMEM.
     */
    ResizeTargetUtilizationVmem("resizeTargetUtilizationVmem", "Scaling Target VMEM Utilization",
            //path is needed for the UI to display this setting in a separate category
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER, EntityType.DATABASE,
                    EntityType.DATABASE_SERVER),
            numeric(1.0f/*min*/, 100.0f/*max*/, 90.0f/*default*/), true),

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
            EnumSet.of(EntityType.STORAGE, EntityType.STORAGE_CONTROLLER,
                    EntityType.LOGICAL_POOL, EntityType.DISK_ARRAY),
            numeric(1f, 2000f, 100f), true),

    /**
     * Virtual CPU Increment.
     */
    VcpuIncrement("usedIncrement_VCPU", "Increment constant for VCPU [MHz]",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER),
            numeric(0.0f/*min*/, 1000000.0f/*max*/, 1800.0f/*default*/), true),

    /**
     * Virtual Memory Increment.
     */
    VmemIncrement("usedIncrement_VMEM", "Increment constant for VMem [MB]",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER),
            numeric(0.0f/*min*/, 1000000.0f/*max*/, 1024.0f/*default*/), true),

    /**
     * Virtual Storage Increment.
     */
    VstorageIncrement("usedIncrement_VStorage", "Increment constant for VStorage [GB]",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(0.0f/*min*/, 999999.0f/*max*/, 999999.0f/*default*/), true),

    /**
     * Excluded Templates.
     */
    ExcludedTemplates("excludedTemplatesOids", "Excluded templates",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.UNION,
            EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.DATABASE, EntityType.DATABASE_SERVER),
            sortedSetOfOid(Type.ENTITY), true),

    /**
     * Storage Increment.
     */
    StorageIncrement("usedIncrement_StAmt", "Increment constant for Storage Amount [GB]",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.STORAGE),
            numeric(0.0f/*min*/, 100000.0f/*max*/, 100.0f/*default*/), true),

    /**
     * Ignore nvme pre-requisite. When this setting is enabled for a VM, we will not perform the
     * NVMe pre-requisite check.
     */
    IgnoreNvmePreRequisite("ignoreNvmePreRequisite", "Ignore NVMe Constraints",
        Collections.singletonList("resizeRecommendationsConstants"),
        SettingTiebreaker.BIGGER,
        EnumSet.of(EntityType.VIRTUAL_MACHINE),
        new BooleanSettingDataType(false), true),

    /**
     * Automation Policy for the Activate Workflow. The value is the name of an
     * Orchestration workflow to invoke when an activate action is generated and executed.
     */
    ActivateActionWorkflow("activateActionWorkflow", "Activate Workflow",
            Collections.singletonList(CategoryPathConstants.AUTOMATION),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.VIRTUAL_MACHINE,
                    EntityType.CONTAINER_POD, EntityType.CONTAINER,
                    EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL),
            string(), true),

    /**
     * Automation Policy for the Activate pre workflow. The value is the name of an
     * Orchestration workflow to invoke before an activate action is executed.
     *
     * NOTE: For action workflows, the first word MUST be the name of the action
     *       type affected by the workflow policy. The UI relies on this convention.
     *       So "Activate Pre Workflow" is okay, but "Pre Activate Workflow" is not.
     */
    PreActivateActionWorkflow("preActivateActionWorkflow", "Activate Pre Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.VIRTUAL_MACHINE,
                    EntityType.CONTAINER_POD, EntityType.CONTAINER,
                    EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL),
        string(), true),

    /**
     * Automation Policy for the Activate post workflow. The value is the name of an
     * Orchestration workflow to invoke after an activate action is executed (whether successful or not).
     */
    PostActivateActionWorkflow("postActivateActionWorkflow", "Activate Post Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.VIRTUAL_MACHINE,
                    EntityType.CONTAINER_POD, EntityType.CONTAINER,
                    EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL),
        string(), true),

    /**
     * Automation Policy for the Move Workflow. The value is the name of an
     * Orchestration workflow to invoke when a resize action is generated and executed.
     */
    MoveActionWorkflow("moveActionWorkflow", "Move Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_POD,
                EntityType.CONTAINER, EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL),
        string(), true),

    /**
     * Automation Policy for the Move Workflow pre workflow. The value is the name of an
     * Orchestration workflow to invoke before a resize action is executed.
     */
    PreMoveActionWorkflow("preMoveActionWorkflow", "Move Pre Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_POD,
                    EntityType.CONTAINER, EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL),
            string(), true),

    /**
     * Automation Policy for the Move Workflow post workflow. The value is the name of an
     * Orchestration workflow to invoke after a resize action is executed (whether successful or not).
     */
    PostMoveActionWorkflow("postMoveActionWorkflow", "Move Post Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_POD,
                    EntityType.CONTAINER, EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL),
        string(), true),

    /**
     * Automation Policy for the Provision Workflow. The value is the name of an
     * Orchestration workflow to invoke when a provision action is generated and executed.
     */
    ProvisionActionWorkflow("provisionActionWorkflow", "Provision Workflow",
            Collections.singletonList(CategoryPathConstants.AUTOMATION),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.DISK_ARRAY,
                    EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_POD, EntityType.CONTAINER,
                    EntityType.LOGICAL_POOL, EntityType.STORAGE_CONTROLLER),
            string(), true),

    /**
     * Automation Policy for the Provision pre workflow. The value is the name of an
     * Orchestration workflow to invoke before a provision action is executed.
     */
    PreProvisionActionWorkflow("preProvisionActionWorkflow", "Provision Pre Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.DISK_ARRAY,
                    EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_POD, EntityType.CONTAINER,
                    EntityType.LOGICAL_POOL, EntityType.STORAGE_CONTROLLER),
        string(), true),

    /**
     * Automation Policy for the Provision post workflow. The value is the name of an
     * Orchestration workflow to invoke after a provision action is executed (whether successful or not).
     */
    PostProvisionActionWorkflow("postProvisionActionWorkflow", "Provision Post Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.DISK_ARRAY,
                    EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_POD, EntityType.CONTAINER,
                    EntityType.LOGICAL_POOL, EntityType.STORAGE_CONTROLLER),
        string(), true),

    /**
     * Automation Policy for the Resize Workflow. The value is the name of an
     * Orchestration workflow to invoke when a resize action is generated and executed.
     */
    ResizeActionWorkflow("resizeActionWorkflow", "Resize Workflow",
            Collections.singletonList(CategoryPathConstants.AUTOMATION),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER,
                    EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL,
                    EntityType.APPLICATION_SERVER, EntityType.DATABASE_SERVER),
            string(), true),

    /**
     * Automation Policy for the Resize pre workflow. The value is the name of an
     * Orchestration workflow to invoke before a resize action is executed.
     */
    PreResizeActionWorkflow("preResizeActionWorkflow", "Resize Pre Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER,
                    EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL,
                    EntityType.APPLICATION_SERVER, EntityType.DATABASE_SERVER),
            string(), true),

    /**
     * Automation Policy for the Resize post workflow. The value is the name of an
     * Orchestration workflow to invoke after a resize action is executed (whether successful or not).
     */
    PostResizeActionWorkflow("postResizeActionWorkflow", "Resize Post Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER,
                    EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL,
                    EntityType.APPLICATION_SERVER, EntityType.DATABASE_SERVER),
        string(), true),

    /**
     * Automation Policy for the Suspend Workflow. The value is the name of an
     * Orchestration workflow to invoke when a suspend action is generated and executed.
     */
    SuspendActionWorkflow("suspendActionWorkflow", "Suspend Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.VIRTUAL_MACHINE,
                    EntityType.CONTAINER_POD, EntityType.CONTAINER,
                    EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL),
        string(), true),

    /**
     * Automation Policy for the Suspend pre workflow. The value is the name of an
     * Orchestration workflow to invoke before a suspend action is executed.
     */
    PreSuspendActionWorkflow("preSuspendActionWorkflow", "Suspend Pre Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.VIRTUAL_MACHINE,
                    EntityType.CONTAINER_POD, EntityType.CONTAINER,
                    EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL),
        string(), true),

    /**
     * Automation Policy for the Suspend post workflow. The value is the name of an
     * Orchestration workflow to invoke after a suspend action is executed (whether successful or not).
     */
    PostSuspendActionWorkflow("postSuspendActionWorkflow", "Suspend Post Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.PHYSICAL_MACHINE, EntityType.VIRTUAL_MACHINE,
                    EntityType.CONTAINER_POD, EntityType.CONTAINER,
                    EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL),
        string(), true),

    /**
     * Automation Policy for the Delete Workflow. The value is the name of an
     * Orchestration workflow to invoke when a delete action is generated and executed.
     */
    DeleteActionWorkflow("deleteActionWorkflow", "Delete Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_VOLUME),
        string(), true),

    /**
     * Automation Policy for the Delete pre workflow. The value is the name of an
     * Orchestration workflow to invoke before a delete action is executed.
     */
    PreDeleteActionWorkflow("preDeleteActionWorkflow", "Delete Pre Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_VOLUME),
        string(), true),

    /**
     * Automation Policy for the Delete post workflow. The value is the name of an
     * Orchestration workflow to invoke after a delete action is executed (whether successful or not).
     */
    PostDeleteActionWorkflow("postDeleteActionWorkflow", "Delete Post Workflow",
        Collections.singletonList(CategoryPathConstants.AUTOMATION),
        SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.STORAGE, EntityType.VIRTUAL_VOLUME),
        string(), true),

    /**
     * Response Time Capacity used by Application and Database.
     */
    ResponseTimeCapacity("responseTimeCapacity", "Response Time Capacity [ms]",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.APPLICATION, EntityType.VIRTUAL_APPLICATION,
                    EntityType.APPLICATION_SERVER, EntityType.BUSINESS_APPLICATION, EntityType.DATABASE_SERVER),
            numeric(1.0f/*min*/, 31536000000000.0f/*max*/, 10000.0f/*default*/),
            true),

    /**
     * SLA Capacity used by Application and Database.
     */
    SLACapacity("slaCapacity", "SLA Capacity",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.APPLICATION, EntityType.VIRTUAL_APPLICATION,
                    EntityType.APPLICATION_SERVER, EntityType.BUSINESS_APPLICATION, EntityType.DATABASE_SERVER),
            numeric(1.0f/*min*/, 31536000000000.0f/*max*/, 10000.0f/*default*/),
            true),

    /**
     * Transactions Capacity used by Application and Database.
     */
    TransactionsCapacity("transactionsCapacity", "Transactions Capacity",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.APPLICATION, EntityType.VIRTUAL_APPLICATION,
                    EntityType.APPLICATION_SERVER, EntityType.BUSINESS_APPLICATION, EntityType.DATABASE_SERVER),
            numeric(1.0f/*min*/, 31536000000000.0f/*max*/, 20.0f/*default*/),
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
            EnumSet.of(EntityType.APPLICATION, EntityType.VIRTUAL_APPLICATION,
                    EntityType.APPLICATION_SERVER, EntityType.BUSINESS_APPLICATION, EntityType.DATABASE_SERVER),
            new BooleanSettingDataType(false),
            true),

    /**
     * Heap utilization threshold.
     */
    HeapUtilization("heapUtilization", "Heap Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.APPLICATION, EntityType.APPLICATION_SERVER),
            numeric(20f, 100f, 80f), true),

    /**
     * Collection time utilization threshold.
     */
    CollectionTimeUtilization("collectionTimeUtilization", "Collection Time Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.APPLICATION, EntityType.APPLICATION_SERVER),
            numeric(1f, 100f, 10f), true),

    IgnoreDirectories("ignoreDirectories", "Directories to ignore",
        Collections.emptyList(),
        SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.STORAGE),
        string("\\.dvsData.*|\\.snapshot.*|\\.vSphere-HA.*|\\.naa.*|\\.etc.*|lost\\+found.*|stCtlVM-.*|\\.iSCSI-CONFIG.*|\\.vsan\\.stats.*|etc|targets"),
        true),

    IgnoreFiles("ignoreFiles", "Files to ignore",
        Collections.emptyList(),
        SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.STORAGE),
        string("config\\.db|stats\\.db.*"),
        true),

    EnforceNonDisruptive("enforceNonDisruptive", "Enforce Non Disruptive Mode",
                    Collections.emptyList(),
                    SettingTiebreaker.SMALLER,
                    EnumSet.of(EntityType.VIRTUAL_MACHINE),
                    new BooleanSettingDataType(false),
                    true),

    /**
     * Setting to represent Disk IOPS Capacity for SSD.
     * This is used to calculate IOPS capacity for Disk Array.
     */
    DiskCapacitySsd("diskCapacitySsd", "SSD Disk IOPS Capacity",
                    Collections.emptyList(), SettingTiebreaker.SMALLER,
                    EnumSet.of(EntityType.DISK_ARRAY),
                    new NumericSettingDataType(20f, 1000000, 50000), true),

    /**
     * Setting to represent Disk IOPS Capacity for 7.2k rpm disk drive.
     * This is used to calculate IOPS capacity for Disk Array.
     */
    DiskCapacity7200("diskCapacity7200", "7.2k Disk IOPS Capacity",
                    Collections.emptyList(), SettingTiebreaker.SMALLER,
                    EnumSet.of(EntityType.DISK_ARRAY),
                    new NumericSettingDataType(20f, 1000000, 800), true),

    /**
     * Setting to represent Disk IOPS Capacity for 10k rpm disk drive.
     * This is used to calculate IOPS capacity for Disk Array.
     */
    DiskCapacity10k("diskCapacity10k", "10k Disk IOPS Capacity",
                    Collections.emptyList(), SettingTiebreaker.SMALLER,
                    EnumSet.of(EntityType.DISK_ARRAY),
                    new NumericSettingDataType(20f, 1000000, 1200), true),

    /**
     * Setting to represent Disk IOPS Capacity for 15k rpm disk drive.
     * This is used to calculate IOPS capacity for Disk Array.
     */
    DiskCapacity15k("diskCapacity15k", "15k Disk IOPS Capacity",
                    Collections.emptyList(), SettingTiebreaker.SMALLER,
                    EnumSet.of(EntityType.DISK_ARRAY),
                    new NumericSettingDataType(20f, 1000000, 1600), true),

    /**
     * Setting to represent Disk IOPS Capacity for VSeries system.
     */
    DiskCapacityVSeries("diskCapacityVSeries", "VSeries LUN IOPS Capacity",
                    Collections.emptyList(), SettingTiebreaker.SMALLER,
                    EnumSet.of(EntityType.DISK_ARRAY),
                    new NumericSettingDataType(20f, 1000000, 5000), true),

    /**
     * This Action Script action is added as a temporary work-around for a bug in the UI.
     * The UI processes workflows as part of the 'actionScript' case - so at least one
     * 'actionScript' must be included, and it must include all EntityTypes that workflows
     * may apply to.
     * Note: ActionScripts are implemented as Workflows in XL, so this policy has nothing
     * to do with actual ActionScripts--those are covered in the workflow policies above!
     * TODO: remove this as part of fix OM-38669
     */
    ProvisionActionScript("provisionActionScript", "Provision",
            Collections.singletonList(CategoryPathConstants.ACTIONSCRIPT),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.DISK_ARRAY,
                EntityType.PHYSICAL_MACHINE,
                EntityType.STORAGE,
                EntityType.VIRTUAL_MACHINE),
            string(), true),

    /**
     * Indicates whether to enforce consistent resizing on a group.  Applies to: VM, Container
     */
    EnableConsistentResizing("consistentResizing", "Enable Consistent Resizing",
        Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
        SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER),
        new BooleanSettingDataType(false), false),

    /*
     Indicates the internal scaling group to which an entity belongs.
     */
    ScalingGroupMembership("scalingGroupMembership", "Scaling Group Membership",
        Collections.emptyList(), SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER),
        string(), false),

    /**
     * Enforce instance store aware scaling actions for {@link EntityType#VIRTUAL_MACHINE}s.
     */
    InstanceStoreAwareScaling("instanceStoreAwareScaling", "Instance Store Aware Scaling",
        Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
        SettingTiebreaker.BIGGER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
        new BooleanSettingDataType(false), true);

    private static final ImmutableSet<String> AUTOMATION_SETTINGS =
        ImmutableSet.of(
            EntitySettingSpecs.Activate.name,
            EntitySettingSpecs.Move.name,
            EntitySettingSpecs.Provision.name,
            EntitySettingSpecs.Reconfigure.name,
            EntitySettingSpecs.Resize.name,
            EntitySettingSpecs.StorageMove.name,
            EntitySettingSpecs.Suspend.name,
            EntitySettingSpecs.ResizeVcpuAboveMaxThreshold.name,
            EntitySettingSpecs.ResizeVcpuBelowMinThreshold.name,
            EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds.name,
            EntitySettingSpecs.ResizeVcpuDownInBetweenThresholds.name,
            EntitySettingSpecs.ResizeVmemAboveMaxThreshold.name,
            EntitySettingSpecs.ResizeVmemBelowMinThreshold.name,
            EntitySettingSpecs.ResizeVmemUpInBetweenThresholds.name,
            EntitySettingSpecs.ResizeVmemDownInBetweenThresholds.name,
            EntitySettingSpecs.EnforceNonDisruptive.name);

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

    /**
     * Create an EntitySettingsSpec, representing a setting attached to an entity or group.
     *
     * @param name the name (also called 'uuid') of this setting
     * @param displayName A human-readable display name for the setting.
     *                    NOTE: For action workflows, the first word MUST be the name of the action
     *                        type affected by the workflow policy. The UI relies on this convention.
     * @param categoryPath the category grouping in which to include this setting
     * @param tieBreaker used to break ties, choosing the bigger or smaller setting
     * @param entityTypeScope enumeration of entity types that this setting may apply to
     * @param dataStructure the type of data structure used to specify the values for this setting
     * @param allowGlobalDefault whether a global default can be set for this setting
     */
    EntitySettingSpecs(@Nonnull String name, @Nonnull String displayName,
            @Nonnull List<String> categoryPath, @Nonnull SettingTiebreaker tieBreaker,
            @Nonnull Set<EntityType> entityTypeScope, @Nonnull SettingDataStructure<?> dataStructure,
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
     * Returns scope of entity type applicable for this setting.
     * @return set of entities type in scope
     */
    public Set<EntityType> getEntityTypeScope() {
        return entityTypeScope;
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

    /**
     * Extract the value from a setting.
     *
     * @param <T> type of a setting value
     * @param setting setting
     * @param cls class of a setting value
     * @return value, null if not present
     */
    @Nullable
    public <T> T getValue(@Nonnull Setting setting, @Nonnull Class<T> cls) {
        Objects.requireNonNull(setting);
        Objects.requireNonNull(cls);
        Object value = dataStructure.getValue(setting);
        return cls.isInstance(value) ? cls.cast(value) : null;
    }

    @Nonnull
    private static SettingDataStructure<?> actionExecutionModeSetToManual() {
        return new EnumSettingDataType<>(ActionMode.MANUAL, ActionMode.class);
    }

    @Nonnull
    private static SettingDataStructure<?> actionExecutionModeSetToRecommend() {
        return new EnumSettingDataType<>(ActionMode.RECOMMEND, ActionMode.class);
    }

    @Nonnull
    private static SettingDataStructure<?> nonExecutableActionMode() {
        return new EnumSettingDataType<>(ActionMode.RECOMMEND, ActionMode.RECOMMEND, ActionMode.class);
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

    @Nonnull
    private static SettingDataStructure<?> sortedSetOfOid(@Nonnull final Type type) {
        return new SortedSetOfOidSettingDataType(type, Collections.emptySet());
    }

    @Nonnull
    private static SettingDataStructure<?> sortedSetOfOid(@Nonnull final Type type,
                                                          @Nonnull final Set<Long> defaultValue) {
        return new SortedSetOfOidSettingDataType(type, defaultValue);
    }

    /**
     * Class for storing setting constants.
     */
    private static class SettingConstants {
        private static final String AGGRESSIVENESS = "Aggressiveness";
        private static final String MAX_OBSERVATION_PERIOD = "Max Observation Period";
    }
}
