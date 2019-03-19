package com.vmturbo.repository.dto;

import javax.annotation.Nonnull;

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
    // The total number of CPU cores on the host
    private Integer numCpus;
    // The timezone of this host
    private String timezone;

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

        if (physicalMachineInfo.hasNumCpus()) {
            setNumCpus(physicalMachineInfo.getNumCpus());
        }

        if (physicalMachineInfo.hasTimezone()) {
            setTimezone(physicalMachineInfo.getTimezone());
        }

        serviceEntityRepoDTO.setPhysicalMachineInfoRepoDTO(this);

    }

    @Override
    @Nonnull
    public TypeSpecificInfo createTypeSpecificInfo() {
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
        if (getNumCpus() != null) {
            physicalMachineInfoBuilder.setNumCpus(getNumCpus());
        }
        if (getTimezone() != null) {
            physicalMachineInfoBuilder.setTimezone(getTimezone());
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

    public Integer getNumCpus() {
        return numCpus;
    }

    public void setNumCpus(Integer numCpus) {
        this.numCpus = numCpus;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof PhysicalMachineInfoRepoDTO)) return false;
        final PhysicalMachineInfoRepoDTO that = (PhysicalMachineInfoRepoDTO) o;
        return Objects.equal(cpuModel, that.cpuModel) &&
                Objects.equal(vendor, that.vendor) &&
                Objects.equal(model, that.model) &&
                Objects.equal(numCpus, that.numCpus) &&
                Objects.equal(timezone, that.timezone);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(cpuModel, vendor, model, numCpus, timezone);
    }

    @Override
    public String toString() {
        return "PhysicalMachineInfoRepoDTO{" +
                "cpuModel='" + cpuModel + '\'' +
                ", vendor='" + vendor + '\'' +
                ", model='" + model + '\'' +
                ", numCpus=" + numCpus +
                ", timezone='" + timezone + '\'' +
                '}';
    }
}
