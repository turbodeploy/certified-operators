package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.VirtualMachineProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Architecture;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo.DriverInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualizationType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.mediation.hybrid.cloud.common.OsDetailParser;
import com.vmturbo.mediation.hybrid.cloud.common.OsDetailParser.OsDetails;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.VStoragePartitionData;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 * Populate the {@link TypeSpecificInfo} unique to a VirtualMachine - i.e. {@link VirtualMachineInfo}
 **/
public class VirtualMachineInfoMapper extends TypeSpecificInfoMapper {

    private static final Logger logger = LogManager.getLogger();

    private static final Map<String, OS> OS_BY_NAME = Collections.synchronizedMap(new HashMap<>());

    private static final String HAS_DEDICATED_PU = "hasDedicatedProcessors";
    private static final String PROCESSOR_COMPAT_MODE = "processorCompatMode";
    private static final String SHARING_MODE = "sharingMode";

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(EntityDTOOrBuilder sdkEntity,
            Map<String, String> entityPropertyMap) {
        return mapEntityDtoToTypeSpecificInfo(null, sdkEntity, entityPropertyMap);
    }

    @Override
    @Nonnull
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(@Nullable TopologyStitchingEntity entity,
            @Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasVirtualMachineData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final VirtualMachineData vmData = sdkEntity.getVirtualMachineData();
        VirtualMachineInfo.Builder vmInfo = VirtualMachineInfo.newBuilder()
            .setGuestOsInfo(parseGuestName(vmData.getGuestName()))
            .setLicenseModel(vmData.getLicenseModel())
            .setBillingType(vmData.getBillingType())
            .addAllIpAddresses(parseIpAddressInfo(vmData));
        // "numCpus" is supposed to be set in VirtualMachineData, but currently most probes
        // set it in "entityProperties" (like vc, aws...), we should also try to find from the map
        if (vmData.hasNumCpus()) {
            vmInfo.setNumCpus(vmData.getNumCpus());
        } else {
            final String numCpus = entityPropertyMap.get(SupplyChainConstants.NUM_CPUS);
            if (numCpus != null) {
                try {
                    vmInfo.setNumCpus(Integer.valueOf(numCpus));
                } catch (NumberFormatException e) {
                    logger.error("Illegal numCpus in VM entity properties: {}", numCpus, e);
                }
            }
        }
        if (vmData.hasCoresPerSocketRatio()) {
            vmInfo.setCoresPerSocketRatio(vmData.getCoresPerSocketRatio());
        }
        if (vmData.hasCoresPerSocketChangeable()) {
            vmInfo.setCoresPerSocketChangeable(vmData.getCoresPerSocketChangeable());
        }
        if (!vmData.getConnectedNetworkList().isEmpty()) {
            vmInfo.addAllConnectedNetworks(vmData.getConnectedNetworkList());
        }
        if (vmData.hasNumEphemeralStorages()) {
            vmInfo.setNumEphemeralStorages(vmData.getNumEphemeralStorages());
        }
        CommonDTO.EntityDTO.VirtualMachineRelatedData vmRelatedData =
                sdkEntity.getVirtualMachineRelatedData();
        if (vmRelatedData.hasMemory()) {
            vmInfo.setDynamicMemory(vmRelatedData.getMemory().getDynamic());
        }
        final DriverInfo.Builder driverInfo = DriverInfo.newBuilder();
        final String hasEnaDriver =
                entityPropertyMap.get(VirtualMachineProtoUtil.PROPERTY_IS_ENA_SUPPORTED);
        if (StringUtils.isNotEmpty(hasEnaDriver)) {
            driverInfo.setHasEnaDriver(Boolean.parseBoolean(hasEnaDriver));
        }
        final String hasNvmeDriver =
                entityPropertyMap.get(VirtualMachineProtoUtil.PROPERTY_SUPPORTS_NVME);
        if (StringUtils.isNotEmpty(hasNvmeDriver)) {
            driverInfo.setHasNvmeDriver(Boolean.parseBoolean(hasNvmeDriver));
        }
        vmInfo.setDriverInfo(driverInfo);
        final String architectureStr =
                entityPropertyMap.get(VirtualMachineProtoUtil.PROPERTY_ARCHITECTURE);
        if (StringUtils.isNotEmpty(architectureStr)) {
            final Architecture architecture =
                    VirtualMachineProtoUtil.ARCHITECTURE.get(architectureStr);
            if (architecture != null) {
                vmInfo.setArchitecture(architecture);
            } else {
                logger.error(
                        "Unknown architecture - {}. Could not add architecture in VirtualMachineSpecificInfo for {}",
                        architectureStr, sdkEntity.getDisplayName());
            }
        }
        final String virtualizationTypeStr =
                entityPropertyMap.get(VirtualMachineProtoUtil.PROPERTY_VM_VIRTUALIZATION_TYPE);
        if (StringUtils.isNotEmpty(virtualizationTypeStr)) {
            final VirtualizationType virtualizationType =
                    VirtualMachineProtoUtil.VIRTUALIZATION_TYPE.get(virtualizationTypeStr);
            if (virtualizationType != null) {
                vmInfo.setVirtualizationType(virtualizationType);
            } else {
                logger.error(
                        "Unknown virtualization type - {}. Could not add virtualization in VirtualMachineSpecificInfo for {}",
                        virtualizationTypeStr, sdkEntity.getDisplayName());
            }
        }
        final String tenancyStr =
            entityPropertyMap.get(VirtualMachineProtoUtil.PROPERTY_VM_TENANCY);
        if (StringUtils.isNotEmpty(tenancyStr)) {
            final Tenancy tenancy =
                VirtualMachineProtoUtil.TENANCY.get(tenancyStr.toLowerCase());
            if (tenancy != null) {
                vmInfo.setTenancy(tenancy);
            } else {
                logger.error(
                    "Unknown tenancy - {}. Could not add tenancy in VirtualMachineSpecificInfo for {}",
                    tenancyStr, sdkEntity.getDisplayName());
            }
        }
        // If there are any (Azure) VM read-only lock prerequisites preventing action execution.
        final String locks = entityPropertyMap.get(VirtualMachineProtoUtil.PROPERTY_LOCKS);
        if (StringUtils.isNotBlank(locks)) {
            vmInfo.setLocks(locks);
        }

        vmInfo.putAllPartitions(parsePartitions(entity));
        if (vmData.getAgentSoftwareProperties().hasVersion()) {
            vmInfo.setVendorToolsVersion(vmData.getAgentSoftwareProperties().getVersion());
        }
        // set VM is a part of an Azure VDI instance or not.
        final String isVdi = entityPropertyMap.get(SearchableProperties.IS_VDI);
        if (StringUtils.isNotEmpty(isVdi)) {
            vmInfo.setIsVdi(Boolean.parseBoolean(isVdi));
        }
        if (entityPropertyMap.containsKey(HAS_DEDICATED_PU)) {
            vmInfo.setHasDedicatedProcessors(
                    Boolean.parseBoolean(entityPropertyMap.get(HAS_DEDICATED_PU)));
        }
        if (entityPropertyMap.containsKey(PROCESSOR_COMPAT_MODE)) {
            vmInfo.setProcessorCompatibilityMode(entityPropertyMap.get(PROCESSOR_COMPAT_MODE));
        }
        if (entityPropertyMap.containsKey(SHARING_MODE)) {
            vmInfo.setSharingMode(entityPropertyMap.get(SHARING_MODE));
        }

        return TypeSpecificInfo.newBuilder().setVirtualMachine(vmInfo.build()).build();
    }

    @Nonnull
    private static Map<String, String> parsePartitions(@Nullable TopologyStitchingEntity entity) {
        Map<String, String> result = new HashMap<>();

        if (entity == null) {
            return result;
        }

        entity.getCommoditiesSold().filter(c -> c.getCommodityType() == CommodityType.VSTORAGE)
                .forEach(comm -> {
                    if (comm.hasVstoragePartitionData()) {
                        VStoragePartitionData data = comm.getVstoragePartitionData();
                        result.put(comm.getKey(), data.getPartition());
                    }
                });

        return result;
    }

    @Nonnull
    private static List<IpAddress> parseIpAddressInfo(VirtualMachineData vmData) {
        int numberElasticIps = vmData.getNumElasticIps();
        List<IpAddress> returnValue = Lists.newArrayList();
        for (String ipAddr : vmData.getIpAddressList()) {
            returnValue.add(IpAddress.newBuilder()
                .setIpAddress(ipAddr)
                .setIsElastic(numberElasticIps-- > 0)
                .build());
        }
        return returnValue;
    }

    @Nonnull
    protected static OS parseGuestName(@Nonnull final String guestName) {
        return OS_BY_NAME.computeIfAbsent(guestName, k -> {
            final OsDetails osDetails = OsDetailParser.parseOsDetails(guestName);
            final String osType = osDetails.getOsType().toString();
            final String osName = osDetails.getOsName();
            final OS.Builder os = OS.newBuilder();
            try {
                return os.setGuestOsType(OSType.valueOf(osType))
                        .setGuestOsName(osName)
                        .build();
            } catch (IllegalArgumentException e) {
                // for On-prem OS type, we pass the OS name with UNKNOWN Cloud type
                return os.setGuestOsType(OSType.UNKNOWN_OS)
                    // it's possible that probe doesn't return guestName which is empty string by
                    // default, we should set it to Unknown so it's searchable from UI
                    .setGuestOsName(guestName.isEmpty() ? StringConstants.UNKNOWN : guestName)
                    .build();
            }
        });
    }
}
