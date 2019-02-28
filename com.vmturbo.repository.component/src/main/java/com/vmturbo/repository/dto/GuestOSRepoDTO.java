package com.vmturbo.repository.dto;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.MoreObjects;

import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * Class to hold contents of guest OS info from TopologyEntityDTO for a VM. It includes the
 * {@link OSType} and original OS name. The {@link OSType} is only for cloud cost calculation and
 * will be set to UNKNOWN type for on-prem entities that have no such OS type. The guest OS name
 * will be displayed on UI.
 */
@JsonInclude(Include.NON_EMPTY)
public class GuestOSRepoDTO {

    private OSType guestOsType;

    private String guestOsName;


    public GuestOSRepoDTO() {
        guestOsType = OSType.UNKNOWN_OS;
        guestOsName = OSType.UNKNOWN_OS.name();
    }

    public GuestOSRepoDTO(OSType guestOsType, String guestOsName) {
        this.guestOsType = guestOsType;
        this.guestOsName = guestOsName;
    }

    public OSType getGuestOsType() {
        return guestOsType;
    }

    public void setGuestOsType(final OSType guestOsType) {
        this.guestOsType = guestOsType;
    }

    public String getGuestOsName() {
        return guestOsName;
    }

    public void setGuestOsName(final String guestOsName) {
        this.guestOsName = guestOsName;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("Guest OS type", guestOsType.name())
                .add("Guest OS name", guestOsName)
                .toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final GuestOSRepoDTO that = (GuestOSRepoDTO) o;

        return Objects.equals(guestOsType, that.guestOsType)
            && Objects.equals(guestOsName, that.guestOsName);

    }

    @Override
    public int hashCode() {
        return Objects.hash(guestOsType, guestOsName);
    }

}
