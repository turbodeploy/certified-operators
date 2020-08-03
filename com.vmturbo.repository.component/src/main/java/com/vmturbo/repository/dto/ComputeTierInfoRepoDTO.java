package com.vmturbo.repository.dto;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Architecture;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo.SupportedCustomerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualizationType;

/**
 * Class that encapsulates the compute tier data from TopologyEntityDTO.TypeSpecificInfo.
 * Make sure that all the fields which need to be saved in DTO have getters to ensure serialization/deserialization.
 */
@JsonInclude(Include.NON_EMPTY)
public class ComputeTierInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private String family;

    private int numCoupons;

    private int numCores;

    Set<Architecture> supportedArchitectures;
    Set<VirtualizationType> supportedVirtualizationTypes;
    Boolean supportsOnlyEnaVms;
    Boolean supportsOnlyNVMeVMs;

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

        if (computeTierInfo.hasSupportedCustomerInfo()) {
            SupportedCustomerInfo supportedCustomerInfo = computeTierInfo.getSupportedCustomerInfo();
            setSupportsOnlyNVMeVMs(supportedCustomerInfo.hasSupportsOnlyNVMeVms() ?
                supportedCustomerInfo.getSupportsOnlyNVMeVms() : null);
            setSupportsOnlyEnaVms(supportedCustomerInfo.hasSupportsOnlyEnaVms() ?
                supportedCustomerInfo.getSupportsOnlyEnaVms() : null);
            setSupportedArchitectures(!supportedCustomerInfo.getSupportedArchitecturesList().isEmpty() ?
                Sets.newHashSet(supportedCustomerInfo.getSupportedArchitecturesList()) : null);
            setSupportedVirtualizationTypes(!supportedCustomerInfo.getSupportedVirtualizationTypesList().isEmpty() ?
                Sets.newHashSet(supportedCustomerInfo.getSupportedVirtualizationTypesList()) : null);
        }
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

        if (getSupportsOnlyNVMeVMs() != null || getSupportsOnlyEnaVms() != null ||
            getSupportedArchitectures() != null || getSupportedVirtualizationTypes() != null) {
            SupportedCustomerInfo.Builder supportedCustomerInfo = SupportedCustomerInfo.newBuilder();
            if (getSupportsOnlyNVMeVMs() != null) {
                supportedCustomerInfo.setSupportsOnlyNVMeVms(getSupportsOnlyNVMeVMs());
            }
            if (getSupportsOnlyEnaVms() != null) {
                supportedCustomerInfo.setSupportsOnlyEnaVms(getSupportsOnlyEnaVms());
            }
            if (getSupportedArchitectures() != null) {
                supportedCustomerInfo.addAllSupportedArchitectures(getSupportedArchitectures());
            }
            if (getSupportedVirtualizationTypes() != null) {
                supportedCustomerInfo.addAllSupportedVirtualizationTypes(getSupportedVirtualizationTypes());
            }
            computeTierBuilder.setSupportedCustomerInfo(supportedCustomerInfo);
        }

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

    public Set<Architecture> getSupportedArchitectures() {
        return supportedArchitectures;
    }

    public void setSupportedArchitectures(final Set<Architecture> supportedArchitectures) {
        this.supportedArchitectures = supportedArchitectures;
    }

    public Set<VirtualizationType> getSupportedVirtualizationTypes() {
        return supportedVirtualizationTypes;
    }

    public void setSupportedVirtualizationTypes(final Set<VirtualizationType> supportedVirtualizationTypes) {
        this.supportedVirtualizationTypes = supportedVirtualizationTypes;
    }

    public Boolean getSupportsOnlyEnaVms() {
        return supportsOnlyEnaVms;
    }

    public void setSupportsOnlyEnaVms(final Boolean supportsOnlyEnaVms) {
        this.supportsOnlyEnaVms = supportsOnlyEnaVms;
    }

    public Boolean getSupportsOnlyNVMeVMs() {
        return supportsOnlyNVMeVMs;
    }

    public void setSupportsOnlyNVMeVMs(final Boolean supportsOnlyNVMeVMs) {
        this.supportsOnlyNVMeVMs = supportsOnlyNVMeVMs;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final ComputeTierInfoRepoDTO that = (ComputeTierInfoRepoDTO) o;

        return (Objects.equals(family, that.getFamily())
                && Objects.equals(numCoupons, that.getNumCoupons())
                && Objects.equals(numCores, that.getNumCores())
                && Objects.equals(supportsOnlyEnaVms, that.supportsOnlyEnaVms)
                && Objects.equals(supportsOnlyNVMeVMs, that.supportsOnlyNVMeVMs)
                && Objects.equals(supportedArchitectures, that.supportedArchitectures)
                && Objects.equals(supportedVirtualizationTypes, that.supportedVirtualizationTypes));
    }

    @Override
    public int hashCode() {
        return Objects.hash(family, numCoupons, numCores, supportsOnlyEnaVms, supportsOnlyNVMeVMs,
            supportedArchitectures, supportedVirtualizationTypes);
    }

    private <T> String stringifySet(Set<T> set) {
        if (set == null) {
            return null;
        }
        return set.stream().map(x -> x.toString()).collect(Collectors.joining(","));
    }

    @Override
    public String toString() {
        return "ComputeTierInfoRepoDTO{" +
                "family='" + family + '\'' +
                ", numCoupons=" + numCoupons +
                ", numCores=" + numCores +
                ", supportsOnlyEnaVms=" + supportsOnlyEnaVms +
                ", supportsOnlyNVMeVMs=" + supportsOnlyNVMeVMs +
                ", supportedArchitectures=" + stringifySet(supportedArchitectures) +
                ", supportedVirtualizationTypes=" + stringifySet(supportedVirtualizationTypes) +
                '}';
    }
}
