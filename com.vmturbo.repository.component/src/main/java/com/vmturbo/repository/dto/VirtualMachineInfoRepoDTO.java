package com.vmturbo.repository.dto;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Class that encapsulates the virtual machine data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class VirtualMachineInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private String guestOsType;

    private String tenancy;

    private List<IpAddressRepoDTO> ipAddressInfoList;

    public VirtualMachineInfoRepoDTO(@Nullable final String guestOsType,
                                     final String tenancy,
                                     @Nullable final List<IpAddressRepoDTO> ipAddressInfoList) {
        this.guestOsType = guestOsType;
        this.tenancy = tenancy;
        this.ipAddressInfoList = ipAddressInfoList;
    }

    public VirtualMachineInfoRepoDTO() {
        guestOsType = null;
        tenancy = null;
        ipAddressInfoList = Lists.newArrayList();
    }

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo,
                                         @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {
        if (!typeSpecificInfo.hasVirtualMachine()) {
            return;
        }
        VirtualMachineInfo vmInfo = typeSpecificInfo.getVirtualMachine();

        setGuestOsType(vmInfo.hasGuestOsType() ? vmInfo.getGuestOsType().toString() : null);
        setTenancy(vmInfo.hasTenancy() ? vmInfo.getTenancy().toString() : null);
        setIpAddressInfoList(vmInfo.getIpAddressesList().stream()
                .map(ipAddrInfo -> new IpAddressRepoDTO(ipAddrInfo.getIpAddress(),
                        ipAddrInfo.getIsElastic()))
                .collect(Collectors.toList()));

        serviceEntityRepoDTO.setVirtualMachineInfoRepoDTO(this);
    }

    public @Nonnull TypeSpecificInfo createTypeSpecificInfo() {

        final VirtualMachineInfo.Builder vmBuilder = VirtualMachineInfo.newBuilder();

        if (getIpAddressInfoList() != null) {
            getIpAddressInfoList().stream()
                    .filter(ipAddressRepoDTO ->
                            ipAddressRepoDTO.getIpAddress() != null)
                    .map(ipAddressDTO -> IpAddress.newBuilder()
                            .setIpAddress(ipAddressDTO.getIpAddress())
                            .setIsElastic(ipAddressDTO.getElastic())
                            .build())
                    .forEach(vmBuilder::addIpAddresses);
        }
        if (getGuestOsType() != null) {
            vmBuilder.setGuestOsType(OSType.valueOf(
                    getGuestOsType()));
        }
        if (getTenancy() != null) {
            vmBuilder.setTenancy(Tenancy.valueOf(
                    getTenancy()));
        }
        return TypeSpecificInfo.newBuilder()
                .setVirtualMachine(vmBuilder)
                .build();
    }


    public String getGuestOsType() {
        return guestOsType;
    }

    public String getTenancy() {
        return tenancy;
    }

    public List<IpAddressRepoDTO> getIpAddressInfoList() {
        return ipAddressInfoList;
    }

    public void setGuestOsType(String guestOsType) {
        this.guestOsType = guestOsType;
    }

    public void setTenancy(String tenancy) {
        this.tenancy = tenancy;
    }

    public void setIpAddressInfoList(List<IpAddressRepoDTO> ipAddressInfoList) {
        this.ipAddressInfoList = ipAddressInfoList;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).omitNullValues()
                .add("guestOsType", guestOsType)
                .add("tenancy", tenancy)
                .add("ipAddressInfo", ipAddressInfoList)
                .toString();
    }
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final VirtualMachineInfoRepoDTO that = (VirtualMachineInfoRepoDTO) o;

        if (!Objects.equals(guestOsType, that.guestOsType)) return false;
        if (!Objects.equals(tenancy, that.tenancy)) return false;
        return Objects.equals(ipAddressInfoList, that.ipAddressInfoList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(guestOsType, tenancy, ipAddressInfoList);
    }
}
