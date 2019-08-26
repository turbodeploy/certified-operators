package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;

/**
 * Class that encapsulates the compute tier data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class ComputeTierInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private String family;

    private int numCoupons;

    private int numCores;

    public ComputeTierInfoRepoDTO(@Nullable final String family,
                                  int numCoupons,
                                  int numCores) {
        this.family = family;
        this.numCoupons = numCoupons;
        this.numCores = numCores;
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
        setNumCores(computeTierInfo.hasNumCores() ? computeTierInfo.getNumCores() : 0);
        serviceEntityRepoDTO.setComputeTierInfoRepoDTO(this);
    }

    @Override
    @Nonnull
    public TypeSpecificInfo createTypeSpecificInfo() {
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
        this.numCores = 0;
    }

    public String getFamily() {
        return family;
    }

    public int getNumCoupons() {
        return numCoupons;
    }

    /**
     * Get the number of cores
     * @return the number of cores
     */
    public int getNumCores() {
        return numCores;
    }

    public void setFamily(final String family) {
        this.family = family;
    }

    public void setNumCoupons(final int numCoupons) {
        this.numCoupons = numCoupons;
    }

    /**
     * Set the number of cores
     * @param numCores number of cores to set on the compute tier
     */
    public void setNumCores(final int numCores) {
        this.numCores = numCores;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final ComputeTierInfoRepoDTO that = (ComputeTierInfoRepoDTO) o;

        return (Objects.equals(family, that.getFamily())
                && Objects.equals(numCoupons, that.getNumCoupons())
                && Objects.equals(numCores, that.getNumCores()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(family, numCoupons, numCores);
    }

    @Override
    public String toString() {
        return "ComputeTierInfoRepoDTO{" +
                "family='" + family + '\'' +
                ", numCoupons=" + numCoupons +
                ", numCores=" + numCores +
                '}';
    }
}
