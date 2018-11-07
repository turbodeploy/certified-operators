package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Populate the {@link TypeSpecificInfo} unique to a VirtualMachine - i.e. {@link VirtualMachineInfo}
 **/
public class VirtualMachineInfoMapper extends TypeSpecificInfoMapper {

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(final EntityDTOOrBuilder sdkEntity) {
        if (!sdkEntity.hasVirtualMachineData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final VirtualMachineData vmData = sdkEntity.getVirtualMachineData();
        return TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder()
                        // We're not currently sending tenancy via the SDK
                        .setTenancy(Tenancy.DEFAULT)
                        .setGuestOsType(parseOsType(vmData.getGuestName()))
                        .addAllIpAddresses(parseIpAddressInfo(vmData))
                        .build())
                .build();
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
    private static OSType parseOsType(@Nonnull final String guestName) {
        // These should come from the OSType enum in com.vmturbo.mediation.hybrid.cloud.utils.
        // Really, the SDK should be setting the num.
        // This is actually a problem for non cloud targets as the guestName coming in will
        // not match  OSType and hence all guestNames will match OSType.OTHER.
        // TODO Add smarter logic here to convert the guestName properly.   See OM-39287
        final String upperCaseOsName = guestName.toUpperCase();
        try {
            return OSType.valueOf(upperCaseOsName);
        } catch (IllegalArgumentException e) {
            return OSType.UNKNOWN_OS;
        }
    }

}
