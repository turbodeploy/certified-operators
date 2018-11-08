package com.vmturbo.repository.dto;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationInfo;

/**
 * Class that encapsulates the Application data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class ApplicationInfoRepoDTO implements TypeSpecificInfoRepoDTO {
    private IpAddressRepoDTO ipAddress;

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo,
                                         @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {
        if (!typeSpecificInfo.hasApplication()) {
            return;
        }
        if (typeSpecificInfo.getApplication().hasIpAddress()) {
            final IpAddress topologyIpAddress = typeSpecificInfo.getApplication().getIpAddress();

            final IpAddressRepoDTO repoIpAddress = new IpAddressRepoDTO();
            repoIpAddress.setIpAddress(topologyIpAddress.getIpAddress());
            repoIpAddress.setElastic(topologyIpAddress.getIsElastic());

            setIpAddress(repoIpAddress);
        }

        serviceEntityRepoDTO.setApplicationInfoRepoDTO(this);
    }

    public @Nonnull TypeSpecificInfo createTypeSpecificInfo() {
        final ApplicationInfo.Builder applicationInfoBuilder = ApplicationInfo.newBuilder();
        if (getIpAddress() != null) {
            applicationInfoBuilder.setIpAddress(IpAddress.newBuilder()
                    .setIpAddress(getIpAddress().getIpAddress())
                    .setIsElastic(getIpAddress().getElastic()));
        }
        return TypeSpecificInfo.newBuilder()
                .setApplication(applicationInfoBuilder)
                .build();
    }

    public IpAddressRepoDTO getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(final IpAddressRepoDTO ipAddress) {
        this.ipAddress = ipAddress;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("ipAddress", ipAddress)
                .toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof ApplicationInfoRepoDTO)) return false;

        final ApplicationInfoRepoDTO that = (ApplicationInfoRepoDTO) o;

        return ipAddress.equals(that.ipAddress);
    }

    @Override
    public int hashCode() {
        return ipAddress.hashCode();
    }
}
