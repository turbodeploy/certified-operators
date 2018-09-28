package com.vmturbo.repository.dto;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

/**
 * Class that encapsulates the virtual machine data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class VirtualMachineInfoRepoDTO {

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
