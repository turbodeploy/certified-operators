package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.MoreObjects;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;

/**
 * Class that encapsulates the compute tier data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class ComputeTierInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private String family;

    private int numCoupons;

    public ComputeTierInfoRepoDTO(@Nullable final String family,
                                  int numCoupons) {
        this.family = family;
        this.numCoupons = numCoupons;
    }

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo,
                                         @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {
        if (!typeSpecificInfo.hasComputeTier()) {
            return;
        }
        ComputeTierInfo computeTierInfo = typeSpecificInfo.getComputeTier();

        setFamily(computeTierInfo.hasFamily() ? computeTierInfo.getFamily() : null);
        setNumCoupons(computeTierInfo.hasNumCoupons() ? computeTierInfo.getNumCoupons() : 0);

        serviceEntityRepoDTO.setComputeTierInfoRepoDTO(this);
    }

    public @Nonnull TypeSpecificInfo createTypeSpecificInfo() {
        final ComputeTierInfo.Builder computeTierBuilder = ComputeTierInfo.newBuilder();

        if (getFamily() != null) {
            computeTierBuilder.setFamily(getFamily());
        }
        computeTierBuilder.setNumCoupons(getNumCoupons());

        return TypeSpecificInfo.newBuilder()
                .setComputeTier(computeTierBuilder)
                .build();
    }

    public ComputeTierInfoRepoDTO() {
        this.family = null;
        this.numCoupons = 0;
    }

    public String getFamily() {
        return family;
    }

    public int getNumCoupons() {
        return numCoupons;
    }

    public void setFamily(final String family) {
        this.family = family;
    }

    public void setNumCoupons(final int numCoupons) {
        this.numCoupons = numCoupons;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).omitNullValues()
                .add("family", family)
                .add("numCoupons", numCoupons)
                .toString();
    }
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final ComputeTierInfoRepoDTO that = (ComputeTierInfoRepoDTO) o;

        return (Objects.equals(family, that.getFamily())
                && Objects.equals(numCoupons, that.getNumCoupons()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(family, numCoupons);
    }
}
