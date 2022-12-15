package com.vmturbo.stitching;

import java.time.Clock;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;

import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;
import com.vmturbo.stitching.poststitching.CloudNativeAppCPUFrequencyPostStitchingOperation;
import com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig;
import com.vmturbo.stitching.poststitching.ComputedNumVCoreUsedValuePostStitchingOperation;
import com.vmturbo.stitching.poststitching.ComputedQxVcpuUsedValuePostStitchingOperation;
import com.vmturbo.stitching.poststitching.ComputedUsedValuePostStitchingOperation;
import com.vmturbo.stitching.poststitching.CpuCapacityPostStitchingOperation;
import com.vmturbo.stitching.poststitching.CpuConsistentScalingFactorPostStitchingOperation.CloudNativeEntityConsistentScalingFactorPostStitchingOperation;
import com.vmturbo.stitching.poststitching.CpuConsistentScalingFactorPostStitchingOperation.VirtualMachineConsistentScalingFactorPostStitchingOperation;
import com.vmturbo.stitching.poststitching.CpuScalingFactorPostStitchingOperation.CloudNativeVMCpuScalingFactorPostStitchingOperation;
import com.vmturbo.stitching.poststitching.CpuScalingFactorPostStitchingOperation.HostCpuScalingFactorPostStitchingOperation;
import com.vmturbo.stitching.poststitching.DisableActionForHxControllerVmAndDsOperation;
import com.vmturbo.stitching.poststitching.DiskCapacityCalculator;
import com.vmturbo.stitching.poststitching.GuestLoadAppPostStitchingOperation;
import com.vmturbo.stitching.poststitching.OverprovisionCapacityPostStitchingOperation.CpuProvisionedPostStitchingOperation;
import com.vmturbo.stitching.poststitching.OverprovisionCapacityPostStitchingOperation.MemoryProvisionedPostStitchingOperation;
import com.vmturbo.stitching.poststitching.OverprovisionCapacityPostStitchingOperation.PmCpuAllocationPostStitchingOperation;
import com.vmturbo.stitching.poststitching.OverprovisionCapacityPostStitchingOperation.PmMemoryAllocationPostStitchingOperation;
import com.vmturbo.stitching.poststitching.OverprovisionCapacityPostStitchingOperation.VmmPmMemoryAllocationPostStitchingOperation;
import com.vmturbo.stitching.poststitching.PmStateChangePostStitchingOperation;
import com.vmturbo.stitching.poststitching.PropagatePowerStatePostStitchingOperation;
import com.vmturbo.stitching.poststitching.PropagateStorageAccessAndLatencyPostStitchingOperation;
import com.vmturbo.stitching.poststitching.PropagatedUpUsedPostStitchingOperation;
import com.vmturbo.stitching.poststitching.ProtectSharedStorageWastedFilesPostStitchingOperation;
import com.vmturbo.stitching.poststitching.ServiceResponseTimePostStitchingOperation;
import com.vmturbo.stitching.poststitching.SetAutoSetCommodityCapacityPostStitchingOperation;
import com.vmturbo.stitching.poststitching.SetAutoSetCommodityCapacityPostStitchingOperation.MaxCapacityCache;
import com.vmturbo.stitching.poststitching.SetCommBoughtUsedFromCommSoldMaxPostStitchingOperation;
import com.vmturbo.stitching.poststitching.SetCommodityCapacityFromSettingPostStitchingOperation;
import com.vmturbo.stitching.poststitching.SetCommodityMaxQuantityPostStitchingOperation;
import com.vmturbo.stitching.poststitching.SetDefaultCommodityCapacityPostStitchingOperation;
import com.vmturbo.stitching.poststitching.SetMovableFalseForHyperVAndVMMNotClusteredVmsOperation;
import com.vmturbo.stitching.poststitching.SetResizeDownAnalysisSettingPostStitchingOperation;
import com.vmturbo.stitching.poststitching.SetTransactionsCapacityPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageAccessCapacityPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageEntityIopsOrLatencyCapacityPostStitchingOp;
import com.vmturbo.stitching.poststitching.StorageLatencyPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageProvisionedPostStitchingOperation.DiskArrayStorageProvisionedPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageProvisionedPostStitchingOperation.LogicalPoolStorageProvisionedPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageProvisionedPostStitchingOperation.StorageEntityStorageProvisionedPostStitchingOperation;
import com.vmturbo.stitching.poststitching.UseHypervisorVmemForResizingPostStitchingOperation;
import com.vmturbo.stitching.poststitching.VMemUsedVMPostStitchingOperation;
import com.vmturbo.stitching.poststitching.VirtualDatacenterCpuAllocationPostStitchingOperation;
import com.vmturbo.stitching.poststitching.VolumeEntityAccessCapacityPostStitchingOperation;
import com.vmturbo.stitching.poststitching.WastedFilesPostStitchingOperation;

/**
 * A library of {@link PostStitchingOperation}s. Maintains the known topology preStitching operations
 * so that they can be run at the appropriate phases of the stitching lifecycle.
 *
 * {@link PostStitchingOperation}s are maintained in the order that they are run.
 */
@Immutable
public class PostStitchingOperationLibrary {
    final ImmutableList<PostStitchingOperation> postStitchingOperations;

    /**
     * Create a new calculation library.
     * Note: these operations are executed in order.
     * todo (2018-2-20): consider removing the need for execution in order.
     *
     * Operations which depend on order:
     *  - CpuCapacityPostStitchingOperation must be executed before
     *    CpuProvisionedPostStitchingOperation and PmCpuAllocationPostStitchingOperation.
     *  - DiskArrayStorageAccessPostStitchingOperation must be executed before
     *    LogicalPoolStorageAccessPostStitchingOperation.
     *  - VmmPmMemoryAllocationPostStitchingOperation must be executed before
     *    PmMemoryAllocationPostStitchingOperation.
     *  - ProtectSharedStorageWastedFilesPostStitchingOperation must be executed before
     *    WastedFilesPostStitchingOperation
     *
     * @param commodityPostStitchingOperationConfig Configuration parameters for SetCommodityMaxQuantityPostStitchingOperation
     * @param cpuCapacityStore For cpu capacities.
     * @param clock The {@link Clock}.
     * @param diskCapacityCalculator The {@link DiskCapacityCalculator}.
     * @param maxCapacityCache The {@link MaxCapacityCache}.
     * @param resizeDownWarmUpIntervalHours See: {@link SetResizeDownAnalysisSettingPostStitchingOperation}.
     */
    public PostStitchingOperationLibrary(
            @Nonnull CommodityPostStitchingOperationConfig commodityPostStitchingOperationConfig,
            @Nonnull final DiskCapacityCalculator diskCapacityCalculator,
            @Nonnull final CpuCapacityStore cpuCapacityStore,
            @Nonnull final Clock clock,
            final double resizeDownWarmUpIntervalHours,
            @Nonnull final MaxCapacityCache maxCapacityCache) {
        postStitchingOperations = ImmutableList.of(
            new PropagateStorageAccessAndLatencyPostStitchingOperation(),
            new MemoryProvisionedPostStitchingOperation(),
            new ComputedUsedValuePostStitchingOperation(
                EntityType.PHYSICAL_MACHINE, CommodityType.MEM_PROVISIONED),
            new CpuCapacityPostStitchingOperation(),
            new CpuProvisionedPostStitchingOperation(),
            new ComputedUsedValuePostStitchingOperation(
                EntityType.PHYSICAL_MACHINE, CommodityType.CPU_PROVISIONED),
            new PmCpuAllocationPostStitchingOperation(),
            new VirtualDatacenterCpuAllocationPostStitchingOperation(),
            new StorageLatencyPostStitchingOperation(),
            new DiskArrayStorageProvisionedPostStitchingOperation(),
            new StorageEntityStorageProvisionedPostStitchingOperation(),
            new LogicalPoolStorageProvisionedPostStitchingOperation(),
            new VmmPmMemoryAllocationPostStitchingOperation(),
            new PmMemoryAllocationPostStitchingOperation(),
            new StorageAccessCapacityPostStitchingOperation(EntityType.DISK_ARRAY, diskCapacityCalculator),
            new StorageAccessCapacityPostStitchingOperation(EntityType.LOGICAL_POOL, diskCapacityCalculator),
            new StorageAccessCapacityPostStitchingOperation(EntityType.STORAGE_CONTROLLER, diskCapacityCalculator),
            new StorageEntityIopsOrLatencyCapacityPostStitchingOp(CommodityType.STORAGE_ACCESS, EntitySettingSpecs.IOPSCapacity),
            new VolumeEntityAccessCapacityPostStitchingOperation(),
            new StorageEntityIopsOrLatencyCapacityPostStitchingOp(CommodityType.STORAGE_LATENCY, EntitySettingSpecs.LatencyCapacity),
            new PropagatedUpUsedPostStitchingOperation(EntityType.STORAGE, CommodityType.STORAGE_LATENCY),
            new PropagatedUpUsedPostStitchingOperation(EntityType.STORAGE, CommodityType.STORAGE_ACCESS),
            new SetResizeDownAnalysisSettingPostStitchingOperation(resizeDownWarmUpIntervalHours, clock),
            new SetCommodityMaxQuantityPostStitchingOperation(commodityPostStitchingOperationConfig),
            new SetCommBoughtUsedFromCommSoldMaxPostStitchingOperation(),
            new SetMovableFalseForHyperVAndVMMNotClusteredVmsOperation(),
            new DisableActionForHxControllerVmAndDsOperation(),
            new UseHypervisorVmemForResizingPostStitchingOperation(),
            new ComputedQxVcpuUsedValuePostStitchingOperation(),
            new ComputedNumVCoreUsedValuePostStitchingOperation(),
            new HostCpuScalingFactorPostStitchingOperation(cpuCapacityStore),
            new CloudNativeVMCpuScalingFactorPostStitchingOperation(),
            new VirtualMachineConsistentScalingFactorPostStitchingOperation(),
            new CloudNativeEntityConsistentScalingFactorPostStitchingOperation(),
            new CloudNativeAppCPUFrequencyPostStitchingOperation(),
            new ServiceResponseTimePostStitchingOperation(),
            // Set capacity from settings for entities coming from public cloud
            new SetCommodityCapacityFromSettingPostStitchingOperation(EntityType.SERVICE,
                ProbeCategory.CLOUD_MANAGEMENT,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName()),
            // APPLICATION_COMPONENT_SPEC does have a default setting yet, using default value set
            // for ResponseTimeSLO setting instead.
            new SetDefaultCommodityCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT_SPEC,
                    ProbeCategory.CLOUD_MANAGEMENT,
                    CommodityType.RESPONSE_TIME,
                    (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT_SPEC)),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.SERVICE,
                ProbeCategory.CLOUD_MANAGEMENT,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.SERVICE),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.DATABASE_SERVER,
                ProbeCategory.CLOUD_MANAGEMENT,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.DATABASE_SERVER),
                EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.DATABASE_SERVER,
                ProbeCategory.CLOUD_MANAGEMENT,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.DATABASE_SERVER),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT,
                ProbeCategory.CLOUD_MANAGEMENT,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT),
                EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT,
                ProbeCategory.CLOUD_MANAGEMENT,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),

            // Set capacity from settings for entities coming from PaaS, ie CloudFoundry
            new SetCommodityCapacityFromSettingPostStitchingOperation(EntityType.SERVICE,
                ProbeCategory.PAAS,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName()),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.SERVICE,
                ProbeCategory.PAAS,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.SERVICE),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT,
                ProbeCategory.PAAS,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT),
                EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT,
                ProbeCategory.PAAS,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.DATABASE_SERVER),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),

            // Set capacity from settings for entities coming from Cloud Native, ie Kubernetes
            new SetCommodityCapacityFromSettingPostStitchingOperation(EntityType.SERVICE,
                ProbeCategory.CLOUD_NATIVE,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName()),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.SERVICE,
                ProbeCategory.CLOUD_NATIVE,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.SERVICE),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT,
                ProbeCategory.CLOUD_NATIVE,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT),
                EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT,
                ProbeCategory.CLOUD_NATIVE,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),

            // Set capacity from settings for entities coming from LoadBalancer, ie NetScaler
            new SetCommodityCapacityFromSettingPostStitchingOperation(EntityType.SERVICE,
                ProbeCategory.LOAD_BALANCER,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName()),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.SERVICE,
                ProbeCategory.LOAD_BALANCER,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.SERVICE),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT,
                ProbeCategory.LOAD_BALANCER,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT),
                EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT,
                ProbeCategory.LOAD_BALANCER,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),

            // Set capacity from settings for entities coming from ACM
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT,
                ProbeCategory.APPLICATION_SERVER,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT),
                EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT,
                ProbeCategory.APPLICATION_SERVER,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.DATABASE_SERVER,
                ProbeCategory.DATABASE_SERVER,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.DATABASE_SERVER),
                EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.DATABASE_SERVER,
                ProbeCategory.DATABASE_SERVER,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.DATABASE_SERVER),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            // Set capacity from settings for entities coming from Turbo APM or 3rd party APM
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.BUSINESS_APPLICATION,
                ProbeCategory.GUEST_OS_PROCESSES,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.BUSINESS_APPLICATION),
                EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.BUSINESS_APPLICATION,
                ProbeCategory.GUEST_OS_PROCESSES,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.BUSINESS_APPLICATION),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.BUSINESS_TRANSACTION,
                ProbeCategory.GUEST_OS_PROCESSES,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.BUSINESS_TRANSACTION),
                EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.BUSINESS_TRANSACTION,
                ProbeCategory.GUEST_OS_PROCESSES,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.BUSINESS_TRANSACTION),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.DATABASE_SERVER,
                ProbeCategory.GUEST_OS_PROCESSES,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.DATABASE_SERVER),
                EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.DATABASE_SERVER,
                ProbeCategory.GUEST_OS_PROCESSES,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.DATABASE_SERVER),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT,
                ProbeCategory.GUEST_OS_PROCESSES,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT),
                EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT,
                ProbeCategory.GUEST_OS_PROCESSES,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.SERVICE,
                ProbeCategory.GUEST_OS_PROCESSES,
                CommodityType.RESPONSE_TIME,
                EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.SERVICE),
                EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.SERVICE,
                ProbeCategory.GUEST_OS_PROCESSES,
                EntitySettingSpecs.TransactionSLO.getSettingName(),
                (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.SERVICE),
                EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),

            // Set capacity from settings for entities coming from Custom probe, such as DIF
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.BUSINESS_APPLICATION,
                    ProbeCategory.CUSTOM,
                    CommodityType.RESPONSE_TIME,
                    EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                    (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.BUSINESS_APPLICATION),
                    EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.BUSINESS_APPLICATION,
                    ProbeCategory.CUSTOM,
                    EntitySettingSpecs.TransactionSLO.getSettingName(),
                    (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.BUSINESS_APPLICATION),
                    EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.BUSINESS_TRANSACTION,
                    ProbeCategory.CUSTOM,
                    CommodityType.RESPONSE_TIME,
                    EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                    (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.BUSINESS_TRANSACTION),
                    EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.BUSINESS_TRANSACTION,
                    ProbeCategory.CUSTOM,
                    EntitySettingSpecs.TransactionSLO.getSettingName(),
                    (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.BUSINESS_TRANSACTION),
                    EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.DATABASE_SERVER,
                    ProbeCategory.CUSTOM,
                    CommodityType.RESPONSE_TIME,
                    EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                    (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.DATABASE_SERVER),
                    EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.DATABASE_SERVER,
                    ProbeCategory.CUSTOM,
                    EntitySettingSpecs.TransactionSLO.getSettingName(),
                    (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.DATABASE_SERVER),
                    EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT,
                    ProbeCategory.CUSTOM,
                    CommodityType.RESPONSE_TIME,
                    EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                    (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT),
                    EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT,
                    ProbeCategory.CUSTOM,
                    EntitySettingSpecs.TransactionSLO.getSettingName(),
                    (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT),
                    EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new SetAutoSetCommodityCapacityPostStitchingOperation(EntityType.SERVICE,
                    ProbeCategory.CUSTOM,
                    CommodityType.RESPONSE_TIME,
                    EntitySettingSpecs.ResponseTimeSLO.getSettingName(),
                    (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.SERVICE),
                    EntitySettingSpecs.ResponseTimeSLOEnabled.getSettingName(), maxCapacityCache),
            new SetTransactionsCapacityPostStitchingOperation(EntityType.SERVICE,
                    ProbeCategory.CUSTOM,
                    EntitySettingSpecs.TransactionSLO.getSettingName(),
                    (Float)EntitySettingSpecs.TransactionSLO.getDataStructure().getDefault(EntityType.SERVICE),
                    EntitySettingSpecs.TransactionSLOEnabled.getSettingName(), maxCapacityCache),
            new ProtectSharedStorageWastedFilesPostStitchingOperation(),
            new WastedFilesPostStitchingOperation(),
            new PropagatePowerStatePostStitchingOperation(),
            new GuestLoadAppPostStitchingOperation(),
            new PmStateChangePostStitchingOperation(),
            new VMemUsedVMPostStitchingOperation()
        );
    }

    /**
     * Get the list of {@link PostStitchingOperation}s to run after the main {@link StitchingOperation}s
     * during post-stitching.
     *
     * @return the list of {@link PostStitchingOperation}s to after the main {@link StitchingOperation}s.
     */
    public List<PostStitchingOperation> getPostStitchingOperations() {
        return postStitchingOperations;
    }
}
