package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

/**
 * Class that encapsulates the compute tier data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class ComputeTierInfoRepoDTO {

    private String family;

    private int numCoupons;

    public ComputeTierInfoRepoDTO(@Nullable final String family,
                                  int numCoupons) {
        this.family = family;
        this.numCoupons = numCoupons;
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
