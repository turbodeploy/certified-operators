package com.vmturbo.topology.processor.conversions.typespecific;

import static com.vmturbo.components.common.utils.StringConstants.ARM_64;
import static com.vmturbo.components.common.utils.StringConstants.BIT_32;
import static com.vmturbo.components.common.utils.StringConstants.BIT_64;
import static com.vmturbo.components.common.utils.StringConstants.HVM;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_ARCHITECTURE;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_IS_ENA_SUPPORTED;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_SUPPORTS_NVME;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_VM_VIRTUALIZATION_TYPE;
import static com.vmturbo.components.common.utils.StringConstants.PVM;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Architecture;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo.DriverInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualizationType;
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

    Map<String, Architecture> architectureMap = ImmutableMap.of(
        BIT_64, Architecture.BIT_64,
        BIT_32, Architecture.BIT_32,
        ARM_64, Architecture.ARM_64);
    Map<String, VirtualizationType> virtualizationTypeMap = ImmutableMap.of(
        HVM, VirtualizationType.HVM,
        PVM, VirtualizationType.PVM);

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(@Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasVirtualMachineData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final VirtualMachineData vmData = sdkEntity.getVirtualMachineData();
        VirtualMachineInfo.Builder vmInfo = VirtualMachineInfo.newBuilder()
            // We're not currently sending tenancy via the SDK
            .setTenancy(Tenancy.DEFAULT)
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
        DriverInfo.Builder driverInfo = DriverInfo.newBuilder();
        String hasEnaDriver = entityPropertyMap.get(PROPERTY_IS_ENA_SUPPORTED);
        if (hasEnaDriver != null && !hasEnaDriver.isEmpty()) {
            driverInfo.setHasEnaDriver(Boolean.parseBoolean(hasEnaDriver));
        }
        String hasNvmeDriver = entityPropertyMap.get(PROPERTY_SUPPORTS_NVME);
        if (hasNvmeDriver != null && !hasNvmeDriver.isEmpty()) {
            driverInfo.setHasNvmeDriver(Boolean.parseBoolean(hasNvmeDriver));
        }
        vmInfo.setDriverInfo(driverInfo);
        parseArchitecture(sdkEntity, entityPropertyMap.get(PROPERTY_ARCHITECTURE))
            .ifPresent(arch -> vmInfo.setArchitecture(arch));
        parseVirtualizationType(sdkEntity, entityPropertyMap.get(PROPERTY_VM_VIRTUALIZATION_TYPE))
            .ifPresent(virtType -> vmInfo.setVirtualizationType(virtType));
        return TypeSpecificInfo.newBuilder().setVirtualMachine(vmInfo.build()).build();
    }

    @Nonnull
    private Optional<Architecture> parseArchitecture(@Nonnull final EntityDTOOrBuilder sdkEntity,
                                                     @Nullable String architectureStr) {
        Optional<Architecture> architectureOpt = Optional.empty();
        if (architectureStr != null && !architectureStr.isEmpty()) {
            architectureOpt = Optional.ofNullable(architectureMap.get(architectureStr));
            if (!architectureOpt.isPresent()) {
                logger.error("Unknown architecture - {}. Could not add architecture in VirtualMachineSpecificInfo for {}",
                    architectureStr, sdkEntity.getDisplayName());
            }
        }
        return architectureOpt;
    }

    @Nonnull
    private Optional<VirtualizationType> parseVirtualizationType(@Nonnull final EntityDTOOrBuilder sdkEntity,
                                                                 @Nullable String virtualizationTypeStr) {
        Optional<VirtualizationType> virtualizationTypeOpt = Optional.empty();
        if (virtualizationTypeStr != null && !virtualizationTypeStr.isEmpty()) {
            virtualizationTypeOpt = Optional.ofNullable(virtualizationTypeMap.get(virtualizationTypeStr));
            if (!virtualizationTypeOpt.isPresent()) {
                logger.error("Unknown virtualization type - {}. Could not add architecture in VirtualMachineSpecificInfo for {}",
                    virtualizationTypeStr, sdkEntity.getDisplayName());
            }
        }
        return virtualizationTypeOpt;
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
    private static OS.Builder parseGuestName(@Nonnull final String guestName) {
        final OS.Builder os = OS.newBuilder();
        final String upperCaseOsName = guestName.toUpperCase();
        try {
            return os.setGuestOsType(OSType.valueOf(upperCaseOsName)).setGuestOsName(guestName);
        } catch (IllegalArgumentException e) {
            // for On-prem OS type, we pass the OS name with UNKNOWN Cloud type
            return os.setGuestOsType(OSType.UNKNOWN_OS).setGuestOsName(guestName);
        }
    }
}
