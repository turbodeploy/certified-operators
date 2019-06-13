package com.vmturbo.repository.dto;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessUserInfo;

/**
 * Class that encapsulates the business user data from TopologyEntityDTO.TypeSpecificInfo.
 */
@JsonInclude(Include.NON_EMPTY)
public class BusinessUserInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private Map<Long, Long> vmOidToSessionDuration;

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull TypeSpecificInfo typeSpecificInfo,
            @Nonnull ServiceEntityRepoDTO serviceEntityRepoDTO) {
        if (!typeSpecificInfo.hasBusinessUser()) {
            return;
        }
        final BusinessUserInfo businessUserInfo = typeSpecificInfo.getBusinessUser();
        this.vmOidToSessionDuration = businessUserInfo.getVmOidToSessionDurationMap();
        serviceEntityRepoDTO.setBusinessUserInfoRepoDTO(this);
    }

    @Nonnull
    @Override
    public TypeSpecificInfo createTypeSpecificInfo() {
        final BusinessUserInfo.Builder builder = BusinessUserInfo.newBuilder();
        if (getVmOidToSessionDuration() != null) {
            builder.putAllVmOidToSessionDuration(getVmOidToSessionDuration());
        }
        return TypeSpecificInfo.newBuilder().setBusinessUser(builder).build();
    }

    public Map<Long, Long> getVmOidToSessionDuration() {
        return vmOidToSessionDuration;
    }

    public void setVmOidToSessionDuration(Map<Long, Long> vmOidToSessionDuration) {
        this.vmOidToSessionDuration = vmOidToSessionDuration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BusinessUserInfoRepoDTO that = (BusinessUserInfoRepoDTO)o;
        return Objects.equals(vmOidToSessionDuration, that.vmOidToSessionDuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vmOidToSessionDuration);
    }

    @Override
    public String toString() {
        return BusinessUserInfoRepoDTO.class.getSimpleName() + '{' + "vmOidToSessionDuration=" +
                vmOidToSessionDuration + '}';
    }
}
