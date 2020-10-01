package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.VirtualMachineProtoUtil;
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
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;

/**
 * Populate the {@link TypeSpecificInfo} unique to a VirtualMachine - i.e. {@link VirtualMachineInfo}
 **/
public class VirtualMachineInfoMapper extends TypeSpecificInfoMapper {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(@Nonnull final EntityDTOOrBuilder sdkEntity,
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
        if (!vmData.getConnectedNetworkList().isEmpty()) {
            vmInfo.addAllConnectedNetworks(vmData.getConnectedNetworkList());
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
        return TypeSpecificInfo.newBuilder().setVirtualMachine(vmInfo.build()).build();
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
    protected static OS.Builder parseGuestName(@Nonnull final String guestName) {
        final OsDetails osDetails = OsDetailParser.parseOsDetails(guestName);
        final String osType = osDetails.getOsType().toString();
        final String osName = osDetails.getOsName();
        final OS.Builder os = OS.newBuilder();
        try {
            return os.setGuestOsType(OSType.valueOf(osType)).setGuestOsName(osName);
        } catch (IllegalArgumentException e) {
            // for On-prem OS type, we pass the OS name with UNKNOWN Cloud type
            return os.setGuestOsType(OSType.UNKNOWN_OS).setGuestOsName(
                    // it's possible that probe doesn't return guestName which is empty string by
                    // default, we should set it to Unknown so it's searchable from UI
                    guestName.isEmpty() ? StringConstants.UNKNOWN : guestName);
        }
    }
}
