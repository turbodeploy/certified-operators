package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
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
                // We're not currently sending tenancy via the SDK
                .setTenancy(Tenancy.DEFAULT)
                .setGuestOsInfo(parseGuestName(vmData.getGuestName()))
                .setBillingType(vmData.getBillingType())
                .addAllIpAddresses(parseIpAddressInfo(vmData));
        // "numCpus" is supposed to be set in VirtualMachineData, but currently most probes
        // set it in "entityProperties" (like vc, aws...), we should also try to find from the map
        if (vmData.hasNumCpus()) {
            vmInfo.setNumCpus(vmData.getNumCpus());
        } else {
            String numCpus = entityPropertyMap.get(SupplyChainConstants.NUM_CPUS);
            if (numCpus != null) {
                try {
                    vmInfo.setNumCpus(Integer.valueOf(numCpus));
                } catch (NumberFormatException e) {
                    logger.error("Illegal numCpus in VM entity properties: {}", numCpus, e);
                }
            }
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
