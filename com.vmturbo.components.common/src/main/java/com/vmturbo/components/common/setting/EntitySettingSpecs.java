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
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.EnumUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
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
     * Shop together setting for VMs.
     */
    ShopTogether("shopTogether", "Shared-Nothing Migration",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE),
            new BooleanSettingDataType(false),
            true),

    /**
     * Rate of resize.
     * Rate of resize setting UI style is defined in settingSpecStyle.json.
     */
    RateOfResize("RATE_OF_RESIZE", "Rate of Resize",
        Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
        SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_SPEC, EntityType.STORAGE),
        new NumericSettingDataType(1.0f, 3.0f, 2.0f,
                ImmutableMap.of(EntityType.CONTAINER_SPEC, 3.0f, EntityType.STORAGE, 3.0f)), true),

    /**
     * The minimum number of vcpu cores which is the threshold to decide automation mode.
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
     * The minimum number of vmem cores which is the threshold to decide automation mode.
     *
     */
    ResizeVmemMinThreshold("resizeVmemMinThreshold", "VMEM Resize Min Threshold (in MB)",
            Collections.emptyList(), SettingTiebreaker.BIGGER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), numeric(0, 100000000, 512), true),

    /**
     * The maximum number of vmem cores which is the threshold to decide automation mode.
     */
    ResizeVmemMaxThreshold("resizeVmemMaxThreshold", "VMEM Resize Max Threshold (in MB)",
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), numeric(0, 100000000, 131072), true),

    /**
     * The minimum number of VCPU request millicores which is the threshold to decide automation mode.
     */
    ResizeVcpuRequestMinThreshold("resizeVcpuRequestMinThreshold",
        String.format("VCPU Request Resize Min Threshold (in %s)", CommodityTypeMapping.CPU_MILLICORE),
        Collections.emptyList(), SettingTiebreaker.BIGGER,
        EnumSet.of(EntityType.CONTAINER_SPEC), numeric(0, 1000000, 10), true),
    /**
     * The minimum number of VCPU limit millicores which is the threshold to decide automation mode.
     */
    ResizeVcpuLimitMinThreshold("resizeVcpuLimitMinThreshold",
        String.format("VCPU Limit Resize Min Threshold (in %s)", CommodityTypeMapping.CPU_MILLICORE),
        Collections.emptyList(), SettingTiebreaker.BIGGER,
        EnumSet.of(EntityType.CONTAINER_SPEC), numeric(0, 1000000, 500), true),

    /**
     * The maximum number of VCPU limit millicores which is the threshold to decide automation mode.
     */
    ResizeVcpuLimitMaxThreshold("resizeVcpuLimitMaxThreshold",
        String.format("VCPU Limit Resize Max Threshold (in %s)", CommodityTypeMapping.CPU_MILLICORE),
        Collections.emptyList(), SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.CONTAINER_SPEC), numeric(0, 1000000, 64000), true),

    /**
     * The minimum number of VMem request which is the threshold to decide automation mode.
     *
     */
    ResizeVmemRequestMinThreshold("resizeVmemRequestMinThreshold", "VMEM Request Resize Min Threshold (in MB)",
        Collections.emptyList(), SettingTiebreaker.BIGGER,
        EnumSet.of(EntityType.CONTAINER_SPEC), numeric(0, 1048576, 10), true),

    /**
     * The minimum number of VMem limit which is the threshold to decide automation mode.
     *
     */
    ResizeVmemLimitMinThreshold("resizeVmemLimitMinThreshold", "VMEM Limit Resize Min Threshold (in MB)",
        Collections.emptyList(), SettingTiebreaker.BIGGER,
        EnumSet.of(EntityType.CONTAINER_SPEC), numeric(0, 1048576, 10), true),

    /**
     * The maximum number of VMem limit which is the threshold to decide automation mode.
     */
    ResizeVmemLimitMaxThreshold("resizeVmemLimitMaxThreshold", "VMEM Limit Resize Max Threshold (in MB)",
        Collections.emptyList(), SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.CONTAINER_SPEC), numeric(0, 1048576, 1048576), true),

    /**
     * Scaling Policy.
     */
    ScalingPolicy("scalingPolicy", "Scaling Policy",
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.APPLICATION_COMPONENT), scalingPolicy(), true),

    /**
     * Whether allow resizing VMEM commodity when it is collected from hypervisors only (not from ACM, APM, etc)
     * If this setting is false, VMEMs collected from only hypervisors will not have RESIZE action.
     */
    UseHypervisorMetricsForResizing("useHypervisorMetricsForResizing", "Use hypervisor VMEM for resize",
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE), new BooleanSettingDataType(true), true),

    /**
     * Enable Scale actions (currently it is used for Volumes, Cloud DB Servers and Virtual Machine Specs only).
     */
    EnableScaleActions("enableScaleActions", "Enable Scale Actions", Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_VOLUME, EntityType.DATABASE_SERVER, EntityType.VIRTUAL_MACHINE_SPEC),
            new BooleanSettingDataType(true), true),

    /**
     * Enable Delete actions (currently it is used for Volumes and Virtual Machine Specs only).
     */
    EnableDeleteActions("enableDeleteActions", "Enable Delete Actions", Collections.emptyList(),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_VOLUME, EntityType.VIRTUAL_MACHINE_SPEC),
            new BooleanSettingDataType(true), true),

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
     * Storage amount utilization threshold.
     */
    StorageAmountUtilization("storageAmountUtilization", "Storage Amount Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE, EntityType.DISK_ARRAY, EntityType.STORAGE_CONTROLLER,
                    EntityType.DATABASE),
            numeric(0f, 100f, 90f),
            true),



    /**
     * Storage provisioned utilization threshold.
     */
    StorageProvisionedUtilization("storageProvisionedUtilization", "Storage Provisioned Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE), numeric(0f, 100f, 100f), true),

    /**
     * VCPURequest utilization threshold.
     * Setting VCPURequest utilization threshold to 0.9999 to avoid rounding errors due to
     * conversion from kubernetes millicores to MHz.
     * This is an internal setting, which should not be modified by user. Therefore, it is
     * hidden from API. The list of category path below is empty as there is no need to specify
     * category grouping for this setting.
     */
    VCPURequestUtilization("vcpuRequestUtilization", "VCPU Request Utilization",
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.CONTAINER_POD),
            numeric(0f, 100f, 99.99f), true, true),
    /**
     * DTU utilization threshold.
     */
    DTUUtilization("dtuUtilization", "Scaling Target DTU Utilization",
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.DATABASE),
            numeric(0f, 100f, 70.0f), true),

    /**
     * Storage amount utilization scaling constraints. Used for cloud entities only.
     */
    ResizeTargetUtilizationStorageAmount("resizeTargetUtilizationStorageAmount", "Scaling Target Storage Amount Utilization",
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.DATABASE_SERVER, EntityType.VIRTUAL_MACHINE_SPEC),
            numeric(0f, 100f, 90f),
            true),

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
            EnumSet.of(EntityType.PHYSICAL_MACHINE, EntityType.VIRTUAL_VOLUME), numeric(0.0f/*min*/,
        100.0f/*max*/, 10.0f/*default*/), true),

    /**
     * Aggressiveness for business user.
     */
    PercentileAggressivenessBusinessUser("percentileAggressivenessBusinessUser",
            SettingConstants.AGGRESSIVENESS,
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.BUSINESS_USER),
            numeric(90.0f, 100.0f, 95.0f), true),

    /**
     * Aggressiveness for container spec.
     */
    PercentileAggressivenessContainerSpec("percentileAggressivenessContainerSpec",
        SettingConstants.AGGRESSIVENESS,
        Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
        SettingTiebreaker.BIGGER, EnumSet.of(EntityType.CONTAINER_SPEC),
        numeric(90.0f, 100.0f, 99.0f), true),

    /**
     * Aggressiveness for virtual machine.
     */
    PercentileAggressivenessVirtualMachine("percentileAggressivenessVirtualMachine",
            SettingConstants.AGGRESSIVENESS,
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(90.0f, 100.0f, 95.0f), true),

    /**
     * Aggressiveness for virtual machine spec.
     */
    PercentileAggressivenessVirtualMachineSpec("percentileAggressivenessVirtualMachineSpec",
            SettingConstants.AGGRESSIVENESS,
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.VIRTUAL_MACHINE_SPEC),
            numeric(90.0f, 100.0f, 95.0f), true),

    /**
     * Aggressiveness for virtual volume.
     */
    PercentileAggressivenessVirtualVolume("percentileAggressivenessVirtualVolume",
            SettingConstants.AGGRESSIVENESS,
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.VIRTUAL_VOLUME),
            numeric(90.0f, 100.0f, 95.0f), true),

    /**
     * Min observation period for container spec.
     */
    MinObservationPeriodContainerSpec("minObservationPeriodContainerSpec",
        "Min Observation Period",
        Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
        SettingTiebreaker.BIGGER, EnumSet.of(EntityType.CONTAINER_SPEC),
        numeric(0.0f, 90.0f, 1.0f), true),

    /**
     * Aggressiveness for Databases.
     */
    PercentileAggressivenessDatabase("percentileAggressivenessDatabase",
            SettingConstants.AGGRESSIVENESS,
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.DATABASE),
            numeric(90.0f, 100.0f, 95.0f), true),

    /**
     * Aggressiveness for Database Servers.
     */
    PercentileAggressivenessDatabaseServer("percentileAggressivenessDatabaseServer",
        SettingConstants.AGGRESSIVENESS,
        Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
        SettingTiebreaker.BIGGER, EnumSet.of(EntityType.DATABASE_SERVER),
        numeric(90.0f, 99.0f, 95.0f), true),

    /**
     * Min observation period for virtual machine.
     */
    MinObservationPeriodVirtualMachine("minObservationPeriodVirtualMachine",
            "Min Observation Period",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(0.0f, 90.0f, 0.0f), true),

    /**
     * Min observation period for virtual machine spec.
     */
    MinObservationPeriodVirtualMachineSpec("minObservationPeriodVirtualMachineSpec",
            "Min Observation Period",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.VIRTUAL_MACHINE_SPEC),
            numeric(0.0f, 7.0f, 0.0f), true),

    /**
     * Min observation period for virtual volume.
     */
    MinObservationPeriodVirtualVolume("minObservationPeriodVirtualVolume",
            "Min Observation Period",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.VIRTUAL_VOLUME),
            numeric(0.0f, 7.0f, 0.0f), true),


    /**
     * Min observation period for DB.
     */
    MinObservationPeriodDatabase("minObservationPeriodDatabase",
            "Min Observation Period",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.DATABASE),
            numeric(0.0f, 7.0f, 0.0f), true),

    /**
     * Min observation period for DB Server.
     */
    MinObservationPeriodDatabaseServer("minObservationPeriodDatabaseServer",
            "Min Observation Period",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.DATABASE_SERVER),
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
     * Max observation period for container spec.
     */
    MaxObservationPeriodContainerSpec("maxObservationPeriodContainerSpec",
        SettingConstants.MAX_OBSERVATION_PERIOD,
        Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
        SettingTiebreaker.BIGGER, EnumSet.of(EntityType.CONTAINER_SPEC),
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
     * Max observation period for virtual machine spec.
     */
    MaxObservationPeriodVirtualMachineSpec("maxObservationPeriodVirtualMachineSpec",
            SettingConstants.MAX_OBSERVATION_PERIOD,
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.VIRTUAL_MACHINE_SPEC),
            numeric(3.0f, 30.0f, 14.0f), true),

    /**
     * Max observation period for virtual volume.
     */
    MaxObservationPeriodVirtualVolume("maxObservationPeriodVirtualVolume",
            SettingConstants.MAX_OBSERVATION_PERIOD,
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.VIRTUAL_VOLUME),
            numeric(7.0f, 90.0f, 30.0f), true),

    /**
     * Max observation period for desktop pool. Used for timeslot feature.
     */
    MaxObservationPeriodDesktopPool("maxObservationPeriodDesktopPool",
                                     SettingConstants.MAX_OBSERVATION_PERIOD,
                                     Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
                                     SettingTiebreaker.BIGGER, EnumSet.of(EntityType.DESKTOP_POOL),
                                     numeric(3, 30, 7), true),

    /**
     *  Max observation period for Databases.
     */
    MaxObservationPeriodDatabase("maxObservationPeriodDatabase",
            SettingConstants.MAX_OBSERVATION_PERIOD,
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.DATABASE),
            numeric(3.0f, 30.0f, 14.0f), true),

    /**
     *  Max observation period for Database Servers.
     */
    MaxObservationPeriodDatabaseServer("maxObservationPeriodDatabaseServer",
        SettingConstants.MAX_OBSERVATION_PERIOD,
        Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
        SettingTiebreaker.BIGGER, EnumSet.of(EntityType.DATABASE_SERVER),
        numeric(3.0f, 30.0f, 14.0f), true),

    /**
     * Resize target Utilization for Image CPU.
     */
    ResizeTargetUtilizationImageCPU("resizeTargetUtilizationImageCPU",
            "Image CPU Target Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.BUSINESS_USER),
            numeric(1.0f, 100.0f, 70.0F), true),

    /**
     * Resize target Utilization for Image Mem.
     */
    ResizeTargetUtilizationImageMem("resizeTargetUtilizationImageMem",
            "Image Mem Target Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.BUSINESS_USER),
            numeric(1.0f, 100.0f, 70.0F), true),

    /**
     * Resize target Utilization for Image Storage.
     */
    ResizeTargetUtilizationImageStorage("resizeTargetUtilizationImageStorage",
            "Image Storage Target Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.BUSINESS_USER),
            numeric(1.0f, 100.0f, 70.0F), true),

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
            EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.VIRTUAL_MACHINE_SPEC, EntityType.DATABASE, EntityType.DATABASE_SERVER),
            numeric(1.0f/*min*/, 100.0f/*max*/, 70.0f/*default*/), true),

    /**
     * Resize target Utilization for VMEM.
     */
    ResizeTargetUtilizationVmem("resizeTargetUtilizationVmem", "Scaling Target VMEM Utilization",
            //path is needed for the UI to display this setting in a separate category
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.VIRTUAL_MACHINE_SPEC, EntityType.DATABASE, EntityType.DATABASE_SERVER),
            numeric(1.0f/*min*/, 100.0f/*max*/, 90.0f/*default*/), true),

    /**
     * Resize target Utilization for IOPS and Throughput. We use this unified setting for both IOPS
     * and Throughput because IOPS and Throughput utilization values are dependent on each other.
     */
    ResizeTargetUtilizationIopsAndThroughput("resizeTargetUtilizationIopsAndThroughput",
            "Scaling Target IOPS/Throughput Utilization",
            //path is needed for the UI to display this setting in a separate category
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_VOLUME, EntityType.DATABASE),
            numeric(1.0f/*min*/, 100.0f/*max*/, 70.0f/*default*/), true),

    /**
     * IOPS Capacity set for on-prem to cloud migration actions
     */
    OnPremIopsCapacity("onPremIopsCapacity",
            "On-Premise IOPS Capacity",
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_VOLUME),
            numeric(1000.0f/*min*/, 100000.0f/*max*/, 50000.0f/*default*/), true),

    /**
     * Resize target Utilization for IOPs.
     */
    ResizeTargetUtilizationIops("resizeTargetUtilizationIops", "Scaling Target IOPS Utilization",
        //path is needed for the UI to display this setting in a separate category
        Collections.emptyList(), SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.DATABASE_SERVER),
        numeric(0.0f/*min*/, 100.0f/*max*/, 70.0f/*default*/), true),

    /**
     * IOPS capacity to set on the entity.
     */
    IOPSCapacity("iopsCapacity", "IOPS Capacity",
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.DISK_ARRAY, EntityType.LOGICAL_POOL,
                EntityType.STORAGE_CONTROLLER, EntityType.STORAGE),
            new NumericSettingDataType(20f, 1000000, 50000,
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
     * Virtual CPU Increment for virtual machines.
     */
    VmVcpuIncrement("usedIncrement_VCPU", "Increment constant for VCPU [MHz]",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(0.0f/*min*/, 1000000.0f/*max*/, 1800.0f/*default*/), true),

    /**
     * Core Socket Ratio Policy for virtual machines.
     */
    VcpuScalingUnits("vcpuScalingUnits", "VCPU Scaling Units",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            new EnumSettingDataType<>(VCPUScalingUnitsEnum.SOCKETS, VCPUScalingUnitsEnum.class), true),

    /**
     * Cores Per Socket mode chosen when scale in sockets.
     * The cores per socket can be either discovered by probe or specified by user in
     * Sockets_CoresPerSocketValue.
     */
    VcpuScaling_Sockets_CoresPerSocketMode("vcpuScaling_sockets_coresPerSocketMode", "Cores per socket mode",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            new EnumSettingDataType<>(VcpuScalingSocketsCoresPerSocketModeEnum.PRESERVE_CORES_PER_SOCKET, VcpuScalingSocketsCoresPerSocketModeEnum.class), true),

    /**
     * Cores Per Socket value specified by user when CoresPerSocket mode is user_specified.
     */
    VcpuScaling_Sockets_CoresPerSocketValue("vcpuScaling_sockets_coresPerSocketValue", "Cores per socket value",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(1.0f, 1000.0f, 1.0f), true),

    /**
     * The increment value when scale in sockets.
     */
    VcpuScaling_Sockets_SocketIncrementValue("vcpuScaling_sockets_socketIncrementValue", "Increment in sockets",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(1.0f, 1000.0f, 1.0f), true),

    /**
     * Socket scaling mode chosen when scale in cores per socket.
     * The socket can be discovered by probe, match the host, or specified by user in
     * CoresPerSocket_SocketValue.
     */
    VcpuScaling_CoresPerSocket_SocketMode("vcpuScaling_coresPerSocket_SocketMode", "Socket mode",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            new EnumSettingDataType<>(VcpuScalingCoresPerSocketSocketModeEnum.PRESERVE_SOCKETS, VcpuScalingCoresPerSocketSocketModeEnum.class), true),

    /**
     * Cores Per Socket value specified by user when CoresPerSocket mode is user_specified.
     */
    VcpuScaling_CoresPerSocket_SocketValue("vcpuScaling_coresPerSocket_SocketValue", "Socket value",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(1.0f, 1000.f, 1.0f), true),

    /**
     * VCPUs value specified by user when VCPUs mode is default increment_size.
     */
    VcpuScaling_Vcpus_VcpusIncrementValue("vcpuScaling_Vcpus_VcpusIncrementValue", "VCPUs value",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(1.0f, 1000.f, 1.0f), true),

    /**
     * Virtual Memory Increment for virtual machines.
     */
    VmVmemIncrement("usedIncrement_VMEM", "Increment constant for VMem [MB]",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(0.0f/*min*/, 1000000.0f/*max*/, 1024.0f/*default*/), true),

    /**
     * Processing Units Increment for IBM virtual machines.
     */
    VmProcessingUnitsIncrement("vmProcessingUnitsIncrement", "Increment constant for ProcessingUnits [PU]",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(0.0f/*min*/, 32.0f/*max*/, 0.5f/*default*/), true, false),

    /**
     * Virtual CPU Increment for containers.
     */
    ContainerSpecVcpuIncrement("usedIncrement_Container_VCPU",
        String.format("Increment constant for VCPU Limit and VCPU Request [%s]",
            CommodityTypeMapping.CPU_MILLICORE),
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.CONTAINER_SPEC),
            numeric(0.0f, 1000000.0f, 100.0f), true),

    /**
     * Virtual Memory Increment for containers.
     */
    ContainerSpecVmemIncrement("usedIncrement_Container_VMEM", "Increment constant for VMem Limit and VMem Request [MB]",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.CONTAINER_SPEC),
            numeric(0.0f, 1000000.0f, 128.0f), true),

    /**
     * Virtual Storage Increment.
     */
    VstorageIncrement("usedIncrement_VStorage", "Increment constant for VStorage [GB]",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
            numeric(0.0f/*min*/, 999999.0f/*max*/, 1024.0f/*default*/), true),

    /**
     * Switch to enable/disable VStorage resizes.
     */
    ResizeVStorage("resizeVStorage", "Resize VStorage",
        Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
        SettingTiebreaker.BIGGER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
        new BooleanSettingDataType(false), true),

    /**
     * Excluded Templates.  This refers to templates that can be excluded in scaling constraints.
     * For example, for virtual machine the templates are the full list of cloud instance
     * ComputeTiers, from which the user can select items for exclusion.
     */
    ExcludedTemplates("excludedTemplatesOids", "Excluded templates",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.UNION,
            EnumSet.of(EntityType.VIRTUAL_MACHINE, EntityType.VIRTUAL_MACHINE_SPEC,
                    EntityType.DATABASE, EntityType.DATABASE_SERVER, EntityType.VIRTUAL_VOLUME),
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
     * Response Time SLO used by Application and Database.
     * @deprecated since ResponseTimeSLO was added.
     * This setting shouldn't be removed in case an old topology is loaded.
     */
    @Deprecated
    ResponseTimeCapacity("responseTimeCapacity", "Response Time SLO [ms]",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.SERVICE, EntityType.BUSINESS_APPLICATION,
                    EntityType.DATABASE_SERVER,  EntityType.APPLICATION_COMPONENT,
                    EntityType.BUSINESS_TRANSACTION),
            numeric(1.0f/*min*/, 31536000000000.0f/*max*/, 2000.0f/*default*/),
            true),

    /**
     * SLA Capacity used by Application and Database.
     */
    @Deprecated
    SLACapacity("slaCapacity", "SLA Capacity",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.APPLICATION_COMPONENT,
                    EntityType.BUSINESS_APPLICATION, EntityType.DATABASE_SERVER),
            numeric(1.0f/*min*/, 31536000000000.0f/*max*/, 10000.0f/*default*/),
            true),

    /**
     * Indicates whether to auto set the response time SLO of an entity's commodity to the value
     * of the ResponseTimeCapacity setting or to calculate it as the max of the commodity's capacity,
     * used value, and the ResponseTimeCapacity setting.
     * Used by Application and Database.
     * @deprecated since ResponseTimeSLOEnabled was added.
     * This setting shouldn't be removed in case an old topology is loaded.
     */
    @Deprecated
    AutoSetResponseTimeCapacity("autoSetResponseTimeCapacity", "Disable Response Time SLO",
            Collections.emptyList(),
            SettingTiebreaker.BIGGER,
            EnumSet.of(EntityType.BUSINESS_APPLICATION, EntityType.APPLICATION_COMPONENT,
                    EntityType.BUSINESS_TRANSACTION, EntityType.SERVICE,
                    EntityType.DATABASE_SERVER),
            new BooleanSettingDataType(false),
            true),

    /**
     * Transaction SLO used by Application and Database.
     * @deprecated since TransactionSLO was added.
     * This setting shouldn't be removed in case an old topology is loaded.
     */
    @Deprecated
    TransactionsCapacity("transactionsCapacity", "Transaction SLO",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.SERVICE, EntityType.BUSINESS_APPLICATION,
                    EntityType.DATABASE_SERVER,
                    EntityType.APPLICATION_COMPONENT, EntityType.BUSINESS_TRANSACTION),
            numeric(1.0f/*min*/, 31536000000000.0f/*max*/, 10.0f/*default*/),
            true),

    /**
     * Indicates whether to auto set the transaction capacity of an entity's commodity to the value
     * of the TransactionsCapacity setting or to calculate it as the max of the commodity's capacity,
     * used value, and the TransactionsCapacity setting.
     * Used by Application and Database.
     * @deprecated since TransactionSLOEnabled was added.
     * This setting shouldn't be removed in case an old topology is loaded.
     */
    @Deprecated
    AutoSetTransactionsCapacity("autoSetTransactionsCapacity", "Disable Transaction SLO",
            Collections.emptyList(),
            SettingTiebreaker.BIGGER,
            EnumSet.of(EntityType.SERVICE, EntityType.BUSINESS_APPLICATION,
                    EntityType.BUSINESS_TRANSACTION, EntityType.DATABASE_SERVER,
                    EntityType.APPLICATION_COMPONENT),
            new BooleanSettingDataType(false),
            true),

    /**
     * Response Time SLO for Business Applications, Business Transactions, Services, Application
     * Components and Database Servers.
     * This value will be set only if the ResponseTimeSLOEnabled setting is "true".
     * The default value is 2 seconds, and the user can set it to anything between 1 millisecond
     * and a 1000 years.
     */
    ResponseTimeSLO("responseTimeSLO", "Response Time SLO [ms]",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            SettingDTOUtil.entityTypesWithSLOSettings,
            numeric(1.0f, 31536000000000.0f, 2000.0f),
            true),

    /**
     * Indicates whether to use the ResponseTimeSLO setting value as the Response Time capacity for
     * the entity.
     * This setting is used for Business Applications, Business Transactions, Services, Application
     * Components and Database Servers.
     */
    ResponseTimeSLOEnabled("responseTimeSLOEnabled", "Enable Response Time SLO",
            Collections.emptyList(),
            SettingTiebreaker.BIGGER,
            SettingDTOUtil.entityTypesWithSLOSettings,
            new BooleanSettingDataType(false),
            true),

    /**
     * Transaction SLO for Business Applications, Business Transactions, Services, Application
     * Components and Database Servers.
     * This value will be set only if the TransactionSLOEnabled setting is "true".
     * The default value is 10 (transactions per minute), and the user can set it to anything between
     * 1 transaction and 31536000000000 transactions.
     */
    TransactionSLO("transactionSLO", "Transaction SLO",
            Collections.emptyList(),
            SettingTiebreaker.BIGGER,
            SettingDTOUtil.entityTypesWithSLOSettings,
            numeric(1.0f, 31536000000000.0f, 10.0f),
            true),

    /**
     * Indicates whether to use the TransactionSLO setting value as the Transaction capacity for
     * the entity.
     * This setting is used for Business Applications, Business Transactions, Services, Application
     * Components and Database Servers.
     */
    TransactionSLOEnabled("transactionSLOEnabled", "Enable Transaction SLO",
            Collections.emptyList(),
            SettingTiebreaker.BIGGER,
            SettingDTOUtil.entityTypesWithSLOSettings,
            new BooleanSettingDataType(false),
            true),

    /**
     * Heap utilization threshold.
     */
    HeapUtilization("heapUtilization", "Heap Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.APPLICATION_COMPONENT),
            numeric(20f, 100f, 80f), true),

    /**
     * Minimum replicas.
     */
    MinReplicas("minReplicas", "Minimum Replicas",
            Collections.emptyList(),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.SERVICE),
            numeric(1, 10000, 1), true),

    /**
     * Maximum replicas.
     */
    MaxReplicas("maxReplicas", "Maximum Replicas",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.SERVICE),
            numeric(1, 10000, 10000), true),

    /**
     * RemainingGcCapacity utilization threshold.
     * This is not exposed to the user and has a fixed value. If we ever want to expose
     * it then add it to settingManagers.json.
     */
    RemainingGcCapacityUtilization("remainingGcCapacityUtilization", "Remaining GC Capacity Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.APPLICATION_COMPONENT),
            numeric(90f, 99f, 97f), true, true),

    /**
     * DbCacheHitRate utilization threshold.
     * This is not exposed to the user and has a fixed value. If we ever want to expose
     * it then add it to settingManagers.json.
     */
    DbCacheHitRateUtilization("dbCacheHitRateUtilization", "DB Cache Hit Rate Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.DATABASE_SERVER),
            numeric(80f, 99f, 90f), true, true),

    /**
     * DBMem utilization threshold.
     */
    DBMemUtilization("dbmemUtilization", "DBMem Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.DATABASE_SERVER),
            numeric(20f, 100f, 100f), true),

    IgnoreDirectories("ignoreDirectories", "Directories to ignore",
        Collections.emptyList(),
        SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.STORAGE),
        string("\\.dvsData.*|\\.snapshot.*|\\.vSphere-HA.*|\\.naa.*|\\.etc.*|lost\\+found.*|stCtlVM-.*|\\.iSCSI-CONFIG.*|\\.vsan\\.stats.*|etc|targets|^(\\[[^\\[]*\\] )?kubevols"),
        true),

    IgnoreFiles("ignoreFiles", "Files to ignore",
        Collections.emptyList(),
        SettingTiebreaker.SMALLER,
        EnumSet.of(EntityType.STORAGE),
        string("config\\.db|stats\\.db.*"),
        true),

    MinWastedFilesSize("minWastedFilesSize", "Minimum Wasted Files Size [KB]",
            Collections.emptyList(), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE),
            numeric(0f, 1000000000f, 1000f), true),

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
                EntityType.VIRTUAL_MACHINE,
                EntityType.APPLICATION_COMPONENT),
            string(), true),

    /**
     * Yet another hack that described in javadoc of {@link #ProvisionActionScript}.
     * TODO: remove this as part of fix OM-38669
     */
    MoveActionScript("moveActionScript", "Move",
                     Collections.singletonList(CategoryPathConstants.ACTIONSCRIPT),
                     SettingTiebreaker.SMALLER,
                     EnumSet.of(EntityType.BUSINESS_USER),
                     string("false"), true),

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
        string(), false, true),

    /**
     * Enforce instance store aware scaling actions for {@link EntityType#VIRTUAL_MACHINE}s.
     */
    InstanceStoreAwareScaling("instanceStoreAwareScaling", "Instance Store Aware Scaling",
        Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
        SettingTiebreaker.BIGGER, EnumSet.of(EntityType.VIRTUAL_MACHINE),
        new BooleanSettingDataType(false), true),

    /**
     * Pool CPU utilization threshold.
     */
    PoolCpuUtilizationThreshold("poolCpuUtilizationThreshold", "Pool CPU Utilization",
                 Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
                 EnumSet.of(EntityType.DESKTOP_POOL), numeric(0f, 100f, 95.0f), true),

    /**
     * Pool memory utilization threshold.
     */
    PoolMemoryUtilizationThreshold("poolMemoryUtilizationThreshold", "Pool Memory Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.DESKTOP_POOL), numeric(0f, 100f, 95.0f), true),

    /**
     * Pool storage utilization threshold.
     */
    PoolStorageUtilizationThreshold("poolStorageUtilizationThreshold", "Pool Storage Utilization",
            Collections.singletonList(CategoryPathConstants.UTILIZATION_THRESHOLDS), SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.DESKTOP_POOL), numeric(0f, 100f, 95.0f), true),

    /**
     * Active session capacity.
     */
    ViewPodActiveSessionsCapacity("viewPodActiveSessionCapacity", "Active Sessions Capacity",
                          Collections.emptyList(), SettingTiebreaker.SMALLER,
                          EnumSet.of(EntityType.VIEW_POD),
                          numeric(0f, 10000f, 8000f), true),

    /**
     * Count of observation window per day for desktop pools. Used for "timeslot" feature.
     */
    DailyObservationWindowDesktopPool("dailyObservationWindowDesktopPool",
                                      "Daily Observation Windows",
                                      Collections.emptyList(),
                                      SettingTiebreaker.SMALLER,
                                      EnumSet.of(EntityType.DESKTOP_POOL),
                                      new EnumSettingDataType<>(DailyObservationWindowsCount.THREE,
                                                                DailyObservationWindowsCount.class),
                                      true),

    /**
     * Heap scaling increment for applications.
     */
    ApplicationHeapScalingIncrement("appHeapScalingIncrement", "Heap Scaling Increment [MB]",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.SMALLER, EnumSet.of(EntityType.APPLICATION_COMPONENT),
            numeric(0.0f, 1000000.0f, 128.0f), true),

    /**
     * DBMem Scaling increment.
     */
    DBMemScalingIncrement("dbMemScalingIncrement", "DBMem Scaling Increment [MB]",
            Collections.singletonList(CategoryPathConstants.RESIZE_RECOMMENDATIONS_CONSTANTS),
            SettingTiebreaker.BIGGER, EnumSet.of(EntityType.DATABASE_SERVER),
            numeric(0.0f/*min*/, 1000000.0f/*max*/, 128.0f/*default*/), true),
    /**
     * Hyperconverged Infrastructure setting. Give the value of the uncompressed
     * amount divided by the compressed amount. A setting of 1 means no
     * compression, and a setting of 2 means compression of 50% – compressing 2
     * MB to 1 MB is a ratio of 2:1, which equals 2.
     */
    HciCompressionRatio(
            "hciCompressionRatio",
            "Compression Ratio",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE),
            numeric(0, 1000, 1),
            true),
    /**
     * Hyperconverged Infrastructure setting. Turn this on if you want
     * Turbonomic to consider the compression ratio when calculating storage
     * utilization and capacity. Whether this is on or off, Turbonomic always
     * considers compression when calculating utilization of StorageProvisioned.
     */
    HciUseCompression(
            "hciUseCompression",
            "Usable Space Includes Compression",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE),
            new BooleanSettingDataType(false),
            true),
    /**
     * Hyperconverged Infrastructure setting. The percentage of vSAN capacity
     * that you want to reserve for overhead.
     */
    HciSlackSpacePercentage(
            "hciSlackSpacePercentage",
            "Slack Space Percentage",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE),
            numeric(0, 100, 25),
            true),
    /**
     * Hyperconverged Infrastructure setting. The amount of space to reserve to
     * the array can support hosts going offline. For example, a value of 2
     * reserves enough space to support two hosts going offline at the same
     * time. With that setting you could put two hosts in maintenance mode
     * without impacting the vSAN array.
     *
     * <p>This is not the same as redundancy – It does not specify how the array
     * distributes data to maintain integrity.
     */
    HciHostCapacityReservation(
            "hciHostCapacityReservation",
            "Host Capacity Reservation",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE),
            numeric(0, 1000, 1),
            true),
    /**
     * Hyperconverged Infrastructure setting. The effective IOPS for an
     * individual host in a vSAN cluster. Note that Turbonomic calculates the
     * effective IOPS for the entire vSAN entity as the sum of the IOPS for each
     * host in the cluster.
     */
    HciHostIopsCapacity(
            "hciHostIopsCapacity",
            "Host IOPS Capacity",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.STORAGE),
            numeric(0, Float.POSITIVE_INFINITY, 50000),
            true),

    /**
     * Instructs Market analysis to prefer savings over reversibility when generating Volume Scale
     * actions.
     */
    PreferSavingsOverReversibility(
            "preferSavingsOverReversibility",
            "Prefer Savings Over Reversibility",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.VIRTUAL_VOLUME),
            new BooleanSettingDataType(true),
            true),

    /**
     * After the hosts in fully automated clusters leave maintenance, keep them not controllable for so long.
     */
    DrsMaintenanceProtectionWindow(
            "drsMaintenanceProtectionWindow",
            "Maintenance Automation Avoidance (minutes)",
            Collections.emptyList(),
            SettingTiebreaker.SMALLER,
            EnumSet.of(EntityType.PHYSICAL_MACHINE),
            numeric(0, 240, 30),
            true);

    private static final Logger logger = LogManager.getLogger();

    private static final ImmutableSet<String> AUTOMATION_SETTINGS = ImmutableSet.<String>builder()
        .add(EntitySettingSpecs.EnforceNonDisruptive.name)
        .add(EntitySettingSpecs.ScalingPolicy.name)
        .add(EntitySettingSpecs.UseHypervisorMetricsForResizing.name)
        .add(EntitySettingSpecs.ShopTogether.name)
        .add(EntitySettingSpecs.DrsMaintenanceProtectionWindow.name)
        .addAll(Arrays.stream(ConfigurableActionSettings.values())
            .map(ConfigurableActionSettings::getSettingName)
            .collect(Collectors.toList()))
        .build();

    /**
     * A set containing the deprecated settings names. We keep deprecated settings in the codebase
     * to support loading old topologies. Deprecated settings are not visible to the user, and are
     * not used anywhere else in the code.
     */
    @VisibleForTesting
    protected static final Set<String> DEPRECATED_SETTINGS = Arrays.stream(values())
            .filter(setting -> {
                    try {
                        return null != EntitySettingSpecs.class.getField(setting.name())
                                .getAnnotation(Deprecated.class);
                    } catch (NoSuchFieldException e) {
                        return false;
                    }
                })
                .map(EntitySettingSpecs::getSettingName)
                .collect(Collectors.toSet());

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
     * A setting that is used internally by the system. Internal settings should not be visible to
     * the user.
     */
    private final boolean isInternal;

    /**
     * Create an EntitySettingsSpec, representing a setting attached to an entity or group. Creates
     * an external setting by default. to create an internal setting should use another constructor.
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
        this(name, displayName, categoryPath, tieBreaker, entityTypeScope,
                dataStructure, allowGlobalDefault, false);
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
     * @param isInternal whether a setting is only used internally by the system
     */
    EntitySettingSpecs(@Nonnull String name, @Nonnull String displayName,
            @Nonnull List<String> categoryPath, @Nonnull SettingTiebreaker tieBreaker,
            @Nonnull Set<EntityType> entityTypeScope, @Nonnull SettingDataStructure<?> dataStructure,
            boolean allowGlobalDefault, boolean isInternal) {
        this.name = Objects.requireNonNull(name);
        this.displayName = Objects.requireNonNull(displayName);
        this.categoryPath = Objects.requireNonNull(categoryPath);
        this.tieBreaker = Objects.requireNonNull(tieBreaker);
        this.entityTypeScope = Objects.requireNonNull(entityTypeScope);
        this.dataStructure = Objects.requireNonNull(dataStructure);
        this.allowGlobalDefault = allowGlobalDefault;
        this.settingSpec = createSettingSpec();
        this.isInternal = isInternal;
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
     * Returns setting name, identified by this enumeration value.
     *
     * @return setting name
     */
    @Nonnull
    public String getDisplayName() {
        return displayName;
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
     * A shortcut to get the numeric default value.
     *
     * @return numeric default value
     */
    public double getNumericDefault() {
        return settingSpec.getNumericSettingValueType().getDefault();
    }

    /**
     * A shortcut to get the numeric minimum value.
     *
     * @return numeric default value
     */
    public double getNumericMin() {
        return settingSpec.getNumericSettingValueType().getMin();
    }

    /**
     * A shortcut to get the numeric maximum value.
     *
     * @return numeric default value
     */
    public double getNumericMax() {
        return settingSpec.getNumericSettingValueType().getMax();
    }


    /**
     * A shortcut to get the boolean default value.
     *
     * @return boolean default value
     */
    public boolean getBooleanDefault() {
        return settingSpec.getBooleanSettingValueType().getDefault();
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

    @VisibleForTesting
    protected static boolean isDeprecatedSetting(@Nonnull String specName) {
        return DEPRECATED_SETTINGS.contains(specName);
    }

    @VisibleForTesting
    protected static boolean isInternalSetting(@Nonnull String specName) {
        boolean isInternal = false;
        EntitySettingSpecs spec = EnumUtils.getEnumIgnoreCase(EntitySettingSpecs.class, specName);
        if (null == spec) {
            logger.error("{} is an invalid EntitySettingSpec name", specName);
            return isInternal;
        }
        isInternal = spec.isInternal;
        return isInternal;
    }

    /**
     * Check if the given setting spec name is a hidden setting. What makes a setting hidden is that
     * it is not listed in settingManagers.json.
     *
     * @param specName Name of the setting spec.
     * @return whether the setting is hidden.
     */
    public static boolean isHiddenSetting(@Nonnull String specName) {
        return isInternalSetting(specName) || isDeprecatedSetting(specName);
    }

    /**
     * Check if the given setting spec name is a visible setting.
     *
     * @param specName Name of the setting spec.
     * @return true if the setting is a visible setting else return false.
     */
    public static boolean isVisibleSetting(@Nonnull String specName) {
        return !isHiddenSetting(specName);
    }

    /**
     * Return {@link EntitySettingSpecs}s with given {@link SettingTiebreaker}.
     *
     * @param tieBreaker a {@link SettingTiebreaker}
     * @return {@link EntitySettingSpecs}s with given {@link SettingTiebreaker}
     */
    public static Stream<EntitySettingSpecs> getEntitySettingSpecByTierBreaker(final SettingTiebreaker tieBreaker) {
        return Stream.of(EntitySettingSpecs.values())
            .filter(settingSpecs -> settingSpecs.getSettingSpec().getEntitySettingSpec().getTiebreaker() == tieBreaker);
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
    public SettingDataStructure<?> getDataStructure() {
        return dataStructure;
    }

    @Nonnull
    private static SettingDataStructure<?> numeric(float min, float max, float defaultValue) {
        return new NumericSettingDataType(min, max, defaultValue);
    }

    @Nonnull
    private static SettingDataStructure<?> string() {
        return new StringSettingDataType(null, MATCH_ANYTHING_REGEX);
    }

    @Nonnull
    private static SettingDataStructure<?> string(String defaultValue) {
        return new StringSettingDataType(defaultValue, MATCH_ANYTHING_REGEX);
    }

    @Nonnull
    private static SettingDataStructure<?> sortedSetOfOid(@Nonnull final Type type) {
        return new SortedSetOfOidSettingDataType(type, null);
    }

    public boolean isAllowGlobalDefault() {
        return allowGlobalDefault;
    }

    @Nonnull
    private static SettingDataStructure<?> scalingPolicy() {
        return new EnumSettingDataType<>(ScalingPolicyEnum.RESIZE, ScalingPolicyEnum.class);
    }

    /**
     * Class for storing setting constants.
     */
    private static class SettingConstants {
        private static final String AGGRESSIVENESS = "Aggressiveness";
        private static final String MAX_OBSERVATION_PERIOD = "Max Observation Period";
    }
}
