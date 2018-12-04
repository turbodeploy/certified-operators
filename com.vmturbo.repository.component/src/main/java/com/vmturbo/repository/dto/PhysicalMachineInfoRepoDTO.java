package com.vmturbo.repository.dto;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;

/**
 * Class that encapsulates the PhysicalMachine data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class PhysicalMachineInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    // The cpu_model for this machine, if known, used to scale the CPU performance
    private String cpuModel;
    // The vendor (or manufacturer) of this Physical Machine (Host)
    private String vendor;
    // The model identifier of this Physical Machine (Host)
    private String model;

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo,
                                         @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {

        if (!typeSpecificInfo.hasPhysicalMachine()) {
            return;
        }
        final PhysicalMachineInfo physicalMachineInfo = typeSpecificInfo.getPhysicalMachine();
        setCpuModel(physicalMachineInfo.getCpuModel());
        setVendor(physicalMachineInfo.getVendor());
        setModel(physicalMachineInfo.getModel());

        serviceEntityRepoDTO.setPhysicalMachineInfoRepoDTO(this);

    }

    /**
     * Create a {@link TypeSpecificInfo} instance with {@link PhysicalMachineInfo} populated
     * from the fields of {@code this}. We must handle missing fields, which will be represented by 'null'
     * in the RepoDTO bean.
     *
     * @return a new {@link TypeSpecificInfo} instance with a populated {@link PhysicalMachineInfo}
     */
    public @Nonnull TypeSpecificInfo createTypeSpecificInfo() {
        final PhysicalMachineInfo.Builder physicalMachineInfoBuilder = PhysicalMachineInfo.newBuilder();
        if (getCpuModel() != null) {
            physicalMachineInfoBuilder.setCpuModel(getCpuModel());
        }
        if (getVendor() != null) {
            physicalMachineInfoBuilder.setVendor(getVendor());
        }
        if (getModel() != null) {
            physicalMachineInfoBuilder.setModel(getModel());
        }
        return TypeSpecificInfo.newBuilder()
                .setPhysicalMachine(physicalMachineInfoBuilder)
                .build();
    }

    public String getCpuModel() {
        return cpuModel;
    }

    public void setCpuModel(final String cpuModel) {
        this.cpuModel = cpuModel;
    }

    public String getVendor() {
        return vendor;
    }

    public void setVendor(final String vendor) {
        this.vendor = vendor;
    }

    public String getModel() {
        return model;
    }

    public void setModel(final String model) {
        this.model = model;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof PhysicalMachineInfoRepoDTO)) return false;
        final PhysicalMachineInfoRepoDTO that = (PhysicalMachineInfoRepoDTO) o;
        return Objects.equal(cpuModel, that.cpuModel) &&
            Objects.equal(vendor, that.vendor) &&
            Objects.equal(model, that.model);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(cpuModel, vendor, model);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("cpuModel", cpuModel)
                .append("vendor", vendor)
                .append("model", model)
                .toString();
    }
}
