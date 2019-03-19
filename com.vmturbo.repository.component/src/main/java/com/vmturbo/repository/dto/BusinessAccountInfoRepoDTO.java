package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;

/**
 * Class that encapsulates the business account data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class BusinessAccountInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private Boolean hasAssociatedTarget;

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo,
                                         @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {
        if (!typeSpecificInfo.hasBusinessAccount()) {
            return;
        }
        BusinessAccountInfo businessAccountInfo = typeSpecificInfo.getBusinessAccount();
        setHasAssociatedTarget(businessAccountInfo.getHasAssociatedTarget());
        serviceEntityRepoDTO.setBusinessAccountInfoRepoDTO(this);
    }

    @Override
    @Nonnull
    public TypeSpecificInfo createTypeSpecificInfo() {
        final BusinessAccountInfo.Builder businessAccountInfo = BusinessAccountInfo.newBuilder();
        if (hasAssociatedTarget != null) {
            businessAccountInfo.setHasAssociatedTarget(hasAssociatedTarget);
        }
        return TypeSpecificInfo.newBuilder()
                .setBusinessAccount(businessAccountInfo)
                .build();
    }

    public void setHasAssociatedTarget(final Boolean hasAssociatedTarget) {
        this.hasAssociatedTarget = hasAssociatedTarget;
    }

    public Boolean getHasAssociatedTarget() {
        return hasAssociatedTarget;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final BusinessAccountInfoRepoDTO that = (BusinessAccountInfoRepoDTO) o;

        return Objects.equals(hasAssociatedTarget, that.getHasAssociatedTarget());
    }

    @Override
    public int hashCode() {
        return Objects.hash(hasAssociatedTarget);
    }

    @Override
    public String toString() {
        return "BusinessAccountInfoRepoDTO{" +
                "hasAssociatedTarget=" + hasAssociatedTarget +
                '}';
    }
}
