package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

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
    // The total number of CPU sockets on the host
    private Integer numCpuSockets;
    // The timezone of this host
    private String timezone;
    // The core speed in MHz
    private Integer cpuCoreMHz;

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

        if (physicalMachineInfo.hasNumCpuSockets()) {
            setNumCpuSockets(physicalMachineInfo.getNumCpuSockets());
        }

        if (physicalMachineInfo.hasTimezone()) {
            setTimezone(physicalMachineInfo.getTimezone());
        }

        if (physicalMachineInfo.hasCpuCoreMhz()) {
            setCpuCoreMHz(physicalMachineInfo.getCpuCoreMhz());
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
        if (getNumCpuSockets() != null) {
            physicalMachineInfoBuilder.setNumCpuSockets(getNumCpuSockets());
        }
        if (getNumCpus() != null) {
            physicalMachineInfoBuilder.setNumCpus(getNumCpus());
        }
        if (getTimezone() != null) {
            physicalMachineInfoBuilder.setTimezone(getTimezone());
        }
        if (getCpuCoreMHz() != null) {
            physicalMachineInfoBuilder.setCpuCoreMhz(getCpuCoreMHz());
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

    public Integer getNumCpuSockets() { return numCpuSockets; }

    public void setNumCpuSockets(final Integer numCpuSockets) {
        this.numCpuSockets = numCpuSockets;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public Integer getCpuCoreMHz() {
        return cpuCoreMHz;
    }

    public void setCpuCoreMHz(final Integer cpuCoreMHz) {
        this.cpuCoreMHz = cpuCoreMHz;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof PhysicalMachineInfoRepoDTO)) return false;
        final PhysicalMachineInfoRepoDTO that = (PhysicalMachineInfoRepoDTO) o;
        return Objects.equals(cpuModel, that.cpuModel) &&
            Objects.equals(vendor, that.vendor) &&
            Objects.equals(model, that.model) &&
            Objects.equals(numCpus, that.numCpus) &&
            Objects.equals(numCpuSockets, that.numCpuSockets) &&
            Objects.equals(timezone, that.timezone) &&
            Objects.equals(cpuCoreMHz, that.cpuCoreMHz);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cpuModel, vendor, model, numCpus, numCpuSockets, timezone, cpuCoreMHz);
    }

    @Override
    public String toString() {
        return "PhysicalMachineInfoRepoDTO{" +
            "cpuModel='" + cpuModel + '\'' +
            ", vendor='" + vendor + '\'' +
            ", model='" + model + '\'' +
            ", numCpus=" + numCpus +
            ", numCpuSockets=" + numCpuSockets +
            ", timezone='" + timezone + '\'' +
            ", cpuCoreMHz='" + cpuCoreMHz + '\'' +
            '}';
    }
}
