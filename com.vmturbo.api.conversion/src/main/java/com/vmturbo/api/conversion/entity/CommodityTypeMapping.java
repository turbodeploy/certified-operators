package com.vmturbo.api.conversion.entity;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * This class provides mapping between SDK commodity types and API commodity types.
 */
public class CommodityTypeMapping {
    private static final Logger logger = LogManager.getLogger();


    /**
     * Set of CPU commodity types to use "millicore" as unit.
     */
    private static final Set<Integer> CPU_COMMODITY_TYPES = ImmutableSet.of(CommodityType.VCPU_VALUE,
        CommodityType.VCPU_REQUEST_VALUE);
    // Millicore unit for container CPU commodity types.
    private static final String MILLICORE_UNIT = "millicore";

    // Unit type for vCPU on VMs.
    private static final String VCPU_UNIT = "vCPU";


    /**
     * The map from SDK commodity type to API Commodity type.
     */
    public static final Map<CommodityType, CommodityInfo> COMMODITY_TYPE_TO_API_STRING =
        ImmutableMap.<CommodityType, CommodityInfo>builder()
            .put(CommodityType.ACTION_PERMIT, CommodityInfo.of("ActionPermit", "ActionPermit", ""))
            .put(CommodityType.ACTIVE_SESSIONS, CommodityInfo.of("ActiveSessions", "ActiveSessions", ""))
            .put(CommodityType.APPLICATION, CommodityInfo.of("ApplicationCommodity", "ApplicationCommodity", ""))
            .put(CommodityType.BALLOONING, CommodityInfo.of("Ballooning", "Ballooning", "KB"))
            .put(CommodityType.BUFFER_COMMODITY, CommodityInfo.of("BufferCommodity", "BufferCommodity", ""))
            .put(CommodityType.CLUSTER, CommodityInfo.of("ClusterCommodity", "ClusterCommodity", ""))
            .put(CommodityType.COLLECTION_TIME, CommodityInfo.of("CollectionTime", "CollectionTime", "%"))
            .put(CommodityType.REMAINING_GC_CAPACITY, CommodityInfo.of("RemainingGcCapacity", "RemainingGcCapacity", "%"))
            .put(CommodityType.COOLING, CommodityInfo.of("Cooling", "Cooling", "C"))
            .put(CommodityType.CONNECTION, CommodityInfo.of("Connection", "Connection", "Connections"))
            .put(CommodityType.COUPON, CommodityInfo.of("Coupon", "Coupon", ""))
            .put(CommodityType.CPU, CommodityInfo.of("CPU", "CPU", "MHz"))
            .put(CommodityType.CPU_ALLOCATION, CommodityInfo.of("CPUAllocation", "CPUAllocation", "MHz"))
            .put(CommodityType.CPU_PROVISIONED, CommodityInfo.of("CPUProvisioned", "CPUProvisioned", "MHz"))
            .put(CommodityType.CPU_REQUEST_ALLOCATION, CommodityInfo.of("CPURequestAllocation", "CPURequestAllocation", "MHz"))
            .put(CommodityType.CROSS_CLOUD_MOVE_SVC, CommodityInfo.of("CrossCloudMoveSvc", "CrossCloudMoveSVC", ""))
            .put(CommodityType.CROSS_CLUSTER_MOVE_SVC, CommodityInfo.of("CrossClusterMoveSvc", "CrossClusterMoveSVC", ""))
            .put(CommodityType.DATACENTER, CommodityInfo.of("DataCenterCommodity", "DataCenterCommodity", ""))
            .put(CommodityType.DATASTORE, CommodityInfo.of("DatastoreCommodity", "DatastoreCommodity", ""))
            .put(CommodityType.DB_CACHE_HIT_RATE, CommodityInfo.of("DBCacheHitRate", "DBCacheHitRate", "%"))
            .put(CommodityType.DB_MEM, CommodityInfo.of("DBMem", "DBMem", "KB"))
            .put(CommodityType.DISK_ARRAY_ACCESS, CommodityInfo.of("DISK_ARRAY_ACCESS", "DiskArrayAccess", ""))
            .put(CommodityType.DRS_SEGMENTATION, CommodityInfo.of("DrsSegmentationCommodity", "DrsSegmentationCommodity", ""))
            .put(CommodityType.DSPM_ACCESS, CommodityInfo.of("DSPMAccessCommodity", "DSPMAccessCommodity", ""))
            .put(CommodityType.EXTENT, CommodityInfo.of("Extent", "Extent", ""))
            .put(CommodityType.FLOW, CommodityInfo.of("Flow", "Flow", "KByte/sec"))
            .put(CommodityType.FLOW_ALLOCATION, CommodityInfo.of("FlowAllocation", "FlowAllocation", "Bytes"))
            .put(CommodityType.HEAP, CommodityInfo.of("Heap", "Heap", "KB"))
            .put(CommodityType.HOST_LUN_ACCESS, CommodityInfo.of("HOST_LUN_ACCESS", "HostLunAccess", ""))
            .put(CommodityType.IMAGE_CPU, CommodityInfo.of("ImageCPU", "ImageCPU", "MHz"))
            .put(CommodityType.IMAGE_MEM, CommodityInfo.of("ImageMem", "ImageMem", "KB"))
            .put(CommodityType.IMAGE_STORAGE, CommodityInfo.of("ImageStorage", "ImageStorage", "MB"))
            .put(CommodityType.INSTANCE_DISK_SIZE, CommodityInfo.of("InstanceDiskSize", "InstanceDiskSize", "MB"))
            .put(CommodityType.INSTANCE_DISK_TYPE, CommodityInfo.of("InstanceDiskType", "InstanceDiskType", ""))
            .put(CommodityType.INSTANCE_DISK_COUNT, CommodityInfo.of("InstanceDiskCount", "InstanceDiskCount", ""))
            .put(CommodityType.IO_THROUGHPUT, CommodityInfo.of("IOThroughput", "IOThroughput", "KByte/sec"))
            .put(CommodityType.LICENSE_ACCESS, CommodityInfo.of("LICENSE_ACCESS", "LicenseAccess", ""))
            .put(CommodityType.MEM, CommodityInfo.of("Mem", "Mem", "KB"))
            .put(CommodityType.MEM_ALLOCATION, CommodityInfo.of("MemAllocation", "MemAllocation", "KB"))
            .put(CommodityType.MEM_REQUEST_ALLOCATION, CommodityInfo.of("MemRequestAllocation", "MemRequestAllocation", "KB"))
            .put(CommodityType.MEM_PROVISIONED, CommodityInfo.of("MemProvisioned", "MemProvisioned", "KB"))
            .put(CommodityType.NET_THROUGHPUT, CommodityInfo.of("NetThroughput", "NetThroughput", "KByte/sec"))
            .put(CommodityType.NETWORK, CommodityInfo.of("NetworkCommodity", "NetworkCommodity", ""))
            .put(CommodityType.NUM_DISK, CommodityInfo.of("NumDisk", "NumDisk", ""))
            .put(CommodityType.NUMBER_CONSUMERS, CommodityInfo.of("NumberConsumers", "NumberConsumers", ""))
            .put(CommodityType.NUM_VCORE, CommodityInfo.of("NumVCore", "NumVCore", ""))
            .put(CommodityType.POOL_CPU, CommodityInfo.of("PoolCPU", "PoolCPU", "MHz"))
            .put(CommodityType.POOL_MEM, CommodityInfo.of("PoolMem", "PoolMem", "KB"))
            .put(CommodityType.POOL_STORAGE, CommodityInfo.of("PoolStorage", "PoolStorage", "MB"))
            .put(CommodityType.PORT_CHANEL, CommodityInfo.of("PORT_CHANNEL", "PortChannel", "KByte/sec"))
            .put(CommodityType.POWER, CommodityInfo.of("Power", "Power", "W"))
            .put(CommodityType.Q16_VCPU, CommodityInfo.of("Q16VCPU", "Q16VCPU", "msec"))
            .put(CommodityType.Q1_VCPU, CommodityInfo.of("Q1VCPU", "Q1VCPU", "msec"))
            .put(CommodityType.Q2_VCPU, CommodityInfo.of("Q2VCPU", "Q2VCPU", "msec"))
            .put(CommodityType.Q32_VCPU, CommodityInfo.of("Q32VCPU", "Q32VCPU", "msec"))
            .put(CommodityType.Q4_VCPU, CommodityInfo.of("Q4VCPU", "Q4VCPU", "msec"))
            .put(CommodityType.Q64_VCPU, CommodityInfo.of("Q64VCPU", "Q64VCPU", "msec"))
            .put(CommodityType.Q8_VCPU, CommodityInfo.of("Q8VCPU", "Q8VCPU", "msec"))
            .put(CommodityType.RESPONSE_TIME, CommodityInfo.of("ResponseTime", "ResponseTime", "msec"))
            .put(CommodityType.SAME_CLUSTER_MOVE_SVC, CommodityInfo.of("SameClusterMoveSvc", "SameClusterMoveSVC", ""))
            .put(CommodityType.SEGMENTATION, CommodityInfo.of("SegmentationCommodity", "SegmentationCommodity", ""))
            .put(CommodityType.SLA_COMMODITY, CommodityInfo.of("SLACommodity", "SLACommodity", ""))
            .put(CommodityType.SOFTWARE_LICENSE_COMMODITY, CommodityInfo.of("SoftwareLicenseCommodity", "SoftwareLicenseCommodity", ""))
            .put(CommodityType.SPACE, CommodityInfo.of("Space", "Space", ""))
            .put(CommodityType.STORAGE_ACCESS, CommodityInfo.of("StorageAccess", "StorageAccess", "IOPS"))
            .put(CommodityType.STORAGE_ALLOCATION, CommodityInfo.of("StorageAllocation", "StorageAllocation", "MB"))
            .put(CommodityType.STORAGE_AMOUNT, CommodityInfo.of("StorageAmount", "StorageAmount", "MB"))
            .put(CommodityType.STORAGE_CLUSTER, CommodityInfo.of("StorageClusterCommodity", "StorageClusterCommodity", ""))
            .put(CommodityType.STORAGE_LATENCY, CommodityInfo.of("StorageLatency", "StorageLatency", "msec"))
            .put(CommodityType.STORAGE_PROVISIONED, CommodityInfo.of("StorageProvisioned", "StorageProvisioned", "MB"))
            .put(CommodityType.SWAPPING, CommodityInfo.of("Swapping", "Swapping", "Byte/sec"))
            .put(CommodityType.THREADS, CommodityInfo.of("Threads", "Threads", "Threads"))
            .put(CommodityType.TRANSACTION, CommodityInfo.of("Transaction", "Transaction", "TPS"))
            .put(CommodityType.TRANSACTION_LOG, CommodityInfo.of("TransactionLog", "TransactionLog", "MB"))
            .put(CommodityType.VCPU, CommodityInfo.of("VCPU", "VCPU", "MHz"))
            .put(CommodityType.VCPU_LIMIT_QUOTA, CommodityInfo.of("VCPULimitQuota", "VCPULimitQuota", "MHz"))
            .put(CommodityType.VCPU_REQUEST, CommodityInfo.of("VCPURequest", "VCPURequest", "MHz"))
            .put(CommodityType.VCPU_REQUEST_QUOTA, CommodityInfo.of("VCPURequestQuota", "VCPURequestQuota", "MHz"))
            .put(CommodityType.VDC, CommodityInfo.of("VDCCommodity", "VDCCommodity", ""))
            .put(CommodityType.VMEM, CommodityInfo.of("VMem", "VMem", "KB"))
            .put(CommodityType.VMEM_LIMIT_QUOTA, CommodityInfo.of("VMemLimitQuota", "VMemLimitQuota", "KB"))
            .put(CommodityType.VMEM_REQUEST, CommodityInfo.of("VMemRequest", "VMemRequest", "KB"))
            .put(CommodityType.VMEM_REQUEST_QUOTA, CommodityInfo.of("VMemRequestQuota", "VMemRequestQuota", "KB"))
            .put(CommodityType.VMPM_ACCESS, CommodityInfo.of("VMPMAccessCommodity", "VMPMAccessCommodity", ""))
            .put(CommodityType.VSTORAGE, CommodityInfo.of("VStorage", "VStorage", "MB"))
            .put(CommodityType.ZONE, CommodityInfo.of("Zone", "Zone", ""))
            .put(CommodityType.LICENSE_COMMODITY, CommodityInfo.of("LicenseCommodity", "LicenseCommodity", ""))
            .put(CommodityType.Q3_VCPU, CommodityInfo.of("Q3VCPU", "Q3VCPU", "msec"))
            .put(CommodityType.NUMBER_CONSUMERS_PM, CommodityInfo.of("NumberConsumersPM", "NumberConsumersPM", ""))
            .put(CommodityType.Q6_VCPU, CommodityInfo.of("Q6VCPU", "Q6VCPU", "msec"))
            .put(CommodityType.Q7_VCPU, CommodityInfo.of("Q7VCPU", "Q7VCPU", "msec"))
            .put(CommodityType.QN_VCPU, CommodityInfo.of("QNVCPU", "QNVCPU", "msec"))
            .put(CommodityType.RIGHT_SIZE_SVC, CommodityInfo.of("RightSizeSVC", "RightsizeSVC", ""))
            .put(CommodityType.RIGHT_SIZE_DOWN, CommodityInfo.of("RightSizeDown", "RightsizeDown", ""))
            .put(CommodityType.MOVE, CommodityInfo.of("Move", "Move", ""))
            .put(CommodityType.Q5_VCPU, CommodityInfo.of("Q5VCPU", "Q5VCPU", "msec"))
            .put(CommodityType.STORAGE, CommodityInfo.of("Storage", "Storage", "MB"))
            .put(CommodityType.NUMBER_CONSUMERS_STORAGE, CommodityInfo.of("NumberConsumersStorage", "NumberConsumersStorage", ""))
            .put(CommodityType.ACCESS, CommodityInfo.of("Access", "Access", ""))
            .put(CommodityType.RIGHT_SIZE_UP, CommodityInfo.of("RightSizeUp", "RightsizeUp", ""))
            .put(CommodityType.VAPP_ACCESS, CommodityInfo.of("VAppAccess", "VAppAccessCommodity", ""))
            .put(CommodityType.HOT_STORAGE, CommodityInfo.of("HotStorage", "HotStorage", ""))
            .put(CommodityType.HA_COMMODITY, CommodityInfo.of("HACommodity", "HACommodity", ""))
            .put(CommodityType.NETWORK_POLICY, CommodityInfo.of("NetworkPolicy", "NetworkPolicy", ""))
            .put(CommodityType.SERVICE_LEVEL_CLUSTER, CommodityInfo.of("ServiceLevelCluster", "ServiceLevelCluster", ""))
            .put(CommodityType.PROCESSING_UNITS, CommodityInfo.of("ProcessingUnits", "ProcessingUnits", ""))
            .put(CommodityType.TENANCY_ACCESS, CommodityInfo.of("TenancyAccess", "TenancyAccess", ""))
            .put(CommodityType.TEMPLATE_ACCESS, CommodityInfo.of("TemplateAccess", "TemplateAccess", ""))
            .put(CommodityType.BURST_BALANCE, CommodityInfo.of("BurstBalance", "BurstBalance", ""))
            .put(CommodityType.DESIRED_COUPON, CommodityInfo.of("DesiredCoupon", "DesiredCoupon", ""))
            .put(CommodityType.NETWORK_INTERFACE_COUNT, CommodityInfo.of("NetworkInterfaceCount", "NetworkInterfaceCount", ""))
            .put(CommodityType.BICLIQUE, CommodityInfo.of("Biclique", "Biclique", ""))
            .put(CommodityType.KPI, CommodityInfo.of("KPI", "KPI", ""))
            .put(CommodityType.TOTAL_SESSIONS, CommodityInfo.of("TotalSessions", "TotalSessions", ""))
            .put(CommodityType.DTU, CommodityInfo.of("DTU", "DTU", ""))
            .put(CommodityType.CONCURRENT_SESSION, CommodityInfo.of("ConcurrentSession", "ConcurrentSession", ""))
            .put(CommodityType.CONCURRENT_WORKER, CommodityInfo.of("ConcurrentWorker", "ConcurrentWorker", ""))
            .put(CommodityType.NUMBER_REPLICAS, CommodityInfo.of("NumberReplicas", "NumberReplicas", ""))
            .put(CommodityType.VCPU_THROTTLING, CommodityInfo.of("VCPUThrottling", "VCPUThrottling", ""))
            .put(CommodityType.UNKNOWN, CommodityInfo.of("Unknown", "Unknown", ""))
            .build();

    private static final Map<String, CommodityType> MIXED_NAME_TO_COMMODITY_TYPE =
        Collections.unmodifiableMap(
            COMMODITY_TYPE_TO_API_STRING.entrySet()
            .stream()
            .collect(Collectors.toMap(e -> e.getValue().getMixedCase(), Map.Entry::getKey)));

    private CommodityTypeMapping() {
    }

    /**
     * Gets the API commodity type based on sdk commodity type.
     *
     * @param commodityType the sdk commodity type.
     * @return the api commodity type.
     */
    @Nonnull
    public static String getApiCommodityType(int commodityType) {
        CommodityType type = CommodityType.forNumber(commodityType);
        if (type != null) {
            return getApiCommodityType(type);
        } else {
            return "Unknown";
        }
    }

    /**
     * Gets the API commodity type based on sdk commodity type.
     *
     * @param commodityType the sdk commodity type.
     * @return the api commodity type.
     */
    @Nonnull
    public static String getApiCommodityType(@Nonnull CommodityType commodityType) {
        final CommodityInfo commodityInfo =
            COMMODITY_TYPE_TO_API_STRING.get(commodityType);
        if (commodityInfo != null) {
            return commodityInfo.getApiString();
        } else {
            return "Unknown";
        }
    }

    /**
     * Gets the Unit based on Sdk commodity type.
     *
     * @param commodityType the sdk commodity type.
     * @return the commodity unit.
     */
    @Nullable
    public static String getUnitForCommodityType(int commodityType) {
        CommodityType type = CommodityType.forNumber(commodityType);
        if (type != null) {
            return getUnitForCommodityType(type);
        } else {
            return null;
        }
    }

    /**
     * Gets the Unit based on Sdk commodity type.
     *
     * @param commodityType the sdk commodity type.
     * @return the commodity unit.
     */
    @Nullable
    public static String getUnitForCommodityType(CommodityType commodityType) {
        final CommodityInfo commodityInfo =
            COMMODITY_TYPE_TO_API_STRING.get(commodityType);
        if (commodityInfo != null) {
            return commodityInfo.getUnits();
        } else {
            return null;
        }
    }

    /**
     * Gets the mix-case value of commodity.
     *
     * @param commodityType the commodity for which mixed case is being retrieved.
     * @return the mixed case value.
     */
    @Nonnull
    public static String getMixedCaseFromCommodityType(@Nonnull CommodityType commodityType) {
        final CommodityInfo commodityInfo =
            COMMODITY_TYPE_TO_API_STRING.get(commodityType);
        if (commodityInfo != null) {
            return commodityInfo.getMixedCase();
        } else {
            return "Unknown";
        }
    }

    /**
     * Gets the mix-case value of commodity.
     *
     * @param commodityType the number of commodity for which mixed case is being retrieved.
     * @return the mixed case value.
     */
    @Nonnull
    public static String getMixedCaseFromCommodityType(int commodityType) {
        CommodityType type = CommodityType.forNumber(commodityType);
        if (type != null) {
            return getMixedCaseFromCommodityType(type);
        } else {
            return "Unknown";
        }
    }

    /**
     * Gets the commodity type based on the mixed case.
     *
     * @param mixedCase the mix-case value of the commodity.
     * @return the commodity.
     */
    public static CommodityType getCommodityTypeFromMixedCase(String mixedCase) {
        return MIXED_NAME_TO_COMMODITY_TYPE.get(mixedCase);
    }

    /**
     * Get units for the given commodity type for an action description.
     *
     * @param commodityTypeInt proto integer type of commodity
     * @param atomicResizeTargetEntityTypeInt type of the target entity in atomic resize action
     * @return optional of units, or empty if no units
     */
    public static Optional<String> getCommodityUnitsForActions(int commodityTypeInt,
                                               @Nullable Integer atomicResizeTargetEntityTypeInt) {
        final CommodityType commodityType = CommodityType.forNumber(commodityTypeInt);
        try {
            String units = getUnitForCommodityType(commodityType);
            // Action translation converts vCPU resizes from "Mhz", which is how we represent
            // vCPU stats, to vCPUs/millicores. We need to account for that here.
            if (CPU_COMMODITY_TYPES.contains(commodityTypeInt)) {
                // If resize info is container CPU commodity, set unit as "millicore".
                if (atomicResizeTargetEntityTypeInt != null
                    && atomicResizeTargetEntityTypeInt == CommonDTO.EntityDTO.EntityType.CONTAINER_SPEC_VALUE) {
                    units = MILLICORE_UNIT;
                } else {
                    units = VCPU_UNIT;
                }
            }
            return StringUtils.isEmpty(units) ? Optional.empty() : Optional.of(units);
        } catch (IllegalArgumentException e) {
            // the Enum is missing, it may be expected if there is no units associated with the
            // commodity, or unexpected if someone forgot to define units for the commodity
            logger.warn("No units for commodity {}", commodityType);
            return Optional.empty();
        }
    }

    /**
     * This class keeps information such as string used in API level, the mixed case literal,
     * and units for a commodity.
     */
    public static class CommodityInfo {
        private final String apiString;
        private final String mixedCase;
        private final String units;

        private CommodityInfo(String apiString, String mixedCase, String units) {
            this.apiString = apiString;
            this.mixedCase = mixedCase;
            this.units = units;
        }

        static CommodityInfo of(String apiString, String mixedName, String unit) {
            return new CommodityInfo(apiString, mixedName, unit);
        }

        public String getApiString() {
            return apiString;
        }

        public String getMixedCase() {
            return mixedCase;
        }

        public String getUnits() {
            return units;
        }
    }
}
