package com.vmturbo.repository.dto;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.MoreObjects;

/**
 * Class to hold contents of ipAddress from TopologyEntityDTO for a VM.  Holds ipAddress
 * and a flag indicating whether the IP Address is elastic.
 */
@JsonInclude(Include.NON_EMPTY)
public class IpAddressRepoDTO {

    private String ipAddress;

    private boolean elastic;

    public IpAddressRepoDTO() {
        ipAddress = null;
        elastic = false;
    }

    public IpAddressRepoDTO(String ipAddress, boolean elastic) {
        this.ipAddress = ipAddress;
        this.elastic = elastic;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public boolean getElastic() {
        return elastic;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public void setElastic(boolean elastic) {
        this.elastic = elastic;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("IP Addresses", ipAddress)
                .add("elastic", elastic)
                .toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final IpAddressRepoDTO that = (IpAddressRepoDTO) o;

        if (!(Objects.equals(ipAddress, that.ipAddress) && elastic == that.getElastic())) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ipAddress, elastic);
    }
}
