package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress.Builder;
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

    @Override
    @Nonnull
    public TypeSpecificInfo createTypeSpecificInfo() {
        final ApplicationInfo.Builder applicationInfoBuilder = ApplicationInfo.newBuilder();
        final IpAddressRepoDTO repoIpAddress = getIpAddress();
        if (repoIpAddress != null) {
            final Builder ipAddressBuilder = IpAddress.newBuilder();
            if (repoIpAddress.getIpAddress() != null) {
                    ipAddressBuilder.setIpAddress(repoIpAddress.getIpAddress());
            }
            ipAddressBuilder.setIsElastic(repoIpAddress.getElastic());
            applicationInfoBuilder.setIpAddress(ipAddressBuilder);
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
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ApplicationInfoRepoDTO that = (ApplicationInfoRepoDTO) o;
        return Objects.equals(ipAddress, that.ipAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ipAddress);
    }

    @Override
    public String toString() {
        return "ApplicationInfoRepoDTO{" +
                "ipAddress=" + ipAddress +
                '}';
    }
}
