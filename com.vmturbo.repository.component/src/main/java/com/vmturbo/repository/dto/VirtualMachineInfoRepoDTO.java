package com.vmturbo.repository.dto;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Class that encapsulates the virtual machine data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class VirtualMachineInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private GuestOSRepoDTO guestOsInfo;

    private String tenancy;

    private List<IpAddressRepoDTO> ipAddressInfoList;

    private Integer numCpus;

    public VirtualMachineInfoRepoDTO() {
        guestOsInfo = null;
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

        setGuestOsInfo(vmInfo.hasGuestOsInfo()
            ? new GuestOSRepoDTO(vmInfo.getGuestOsInfo().getGuestOsType(),
                vmInfo.getGuestOsInfo().getGuestOsName())
            : null);
        setTenancy(vmInfo.hasTenancy() ? vmInfo.getTenancy().toString() : null);
        setIpAddressInfoList(vmInfo.getIpAddressesList().stream()
                .map(ipAddrInfo -> new IpAddressRepoDTO(ipAddrInfo.getIpAddress(),
                        ipAddrInfo.getIsElastic()))
                .collect(Collectors.toList()));
        setNumCpus(vmInfo.hasNumCpus() ? vmInfo.getNumCpus() : null);
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
        if (getGuestOsInfo() != null) {
            final GuestOSRepoDTO guestOsRepoDTO = getGuestOsInfo();
            vmBuilder.setGuestOsInfo(OS.newBuilder()
                .setGuestOsType(guestOsRepoDTO.getGuestOsType())
                .setGuestOsName(guestOsRepoDTO.getGuestOsName()).build());
        }
        if (getTenancy() != null) {
            vmBuilder.setTenancy(Tenancy.valueOf(
                    getTenancy()));
        }
        if (getNumCpus() != null) {
            vmBuilder.setNumCpus(getNumCpus());
        }
        return TypeSpecificInfo.newBuilder()
                .setVirtualMachine(vmBuilder)
                .build();
    }


    public GuestOSRepoDTO getGuestOsInfo() {
        return guestOsInfo;
    }

    public String getTenancy() {
        return tenancy;
    }

    public List<IpAddressRepoDTO> getIpAddressInfoList() {
        return ipAddressInfoList;
    }

    public Integer getNumCpus() {
        return numCpus;
    }

    public void setGuestOsInfo(GuestOSRepoDTO guestOsInfo) {
        this.guestOsInfo = guestOsInfo;
    }

    public void setTenancy(String tenancy) {
        this.tenancy = tenancy;
    }

    public void setIpAddressInfoList(List<IpAddressRepoDTO> ipAddressInfoList) {
        this.ipAddressInfoList = ipAddressInfoList;
    }

    public void setNumCpus(Integer numCpus) {
        this.numCpus = numCpus;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).omitNullValues()
                .add("guestOsInfo", guestOsInfo)
                .add("tenancy", tenancy)
                .add("ipAddressInfo", ipAddressInfoList)
                .add("numCpus", numCpus)
                .toString();
    }
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final VirtualMachineInfoRepoDTO that = (VirtualMachineInfoRepoDTO) o;

        return Objects.equals(guestOsInfo, that.guestOsInfo) &&
                Objects.equals(ipAddressInfoList, that.ipAddressInfoList) &&
                Objects.equals(tenancy, that.tenancy) &&
                Objects.equals(numCpus, that.numCpus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(guestOsInfo, tenancy, ipAddressInfoList, numCpus);
    }
}
