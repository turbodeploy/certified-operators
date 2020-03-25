package com.vmturbo.repository.dto;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.Architecture;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo.DriverInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualizationType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Class that encapsulates the virtual machine data from TopologyEntityDTO.TypeSpecificInfo.
 * Make sure that all the fields which need to be saved in DTO have getters to ensure serialization/deserialization.
 */
@JsonInclude(Include.NON_EMPTY)
public class VirtualMachineInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private GuestOSRepoDTO guestOsInfo;

    private String tenancy;

    private List<IpAddressRepoDTO> ipAddressInfoList;

    private Integer numCpus;

    private LicenseModel licenseModel;

    private List<String> connectedNetworks;

    private Boolean hasEnaDriver;

    private Boolean hasNVMeDriver;

    private Architecture architecture;

    private VirtualizationType virtualizationType;


    public VirtualMachineInfoRepoDTO() {
        guestOsInfo = null;
        tenancy = null;
        licenseModel = LicenseModel.LICENSE_INCLUDED;
        ipAddressInfoList = Lists.newArrayList();
        connectedNetworks = Lists.newArrayList();
        hasEnaDriver = null;
        hasNVMeDriver = null;
        architecture = null;
        virtualizationType = null;
    }

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo,
                                         @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {
        if (!typeSpecificInfo.hasVirtualMachine()) {
            return;
        }
        VirtualMachineInfo vmInfo = typeSpecificInfo.getVirtualMachine();

        setGuestOsInfo(vmInfo.hasGuestOsInfo()
            ? new GuestOSRepoDTO(vmInfo.getGuestOsInfo().getGuestOsType(),
                vmInfo.getGuestOsInfo().getGuestOsName())
            : null);
        setTenancy(vmInfo.hasTenancy() ? vmInfo.getTenancy().toString() : null);
        setIpAddressInfoList(vmInfo.getIpAddressesList().stream()
                .map(ipAddrInfo -> new IpAddressRepoDTO(ipAddrInfo.getIpAddress(),
                        ipAddrInfo.getIsElastic()))
                .collect(Collectors.toList()));
        setNumCpus(vmInfo.hasNumCpus() ? vmInfo.getNumCpus() : null);
        setLicenseModel(vmInfo.getLicenseModel());

        setConnectedNetworks(vmInfo.getConnectedNetworksList());
        setHasEnaDriver(vmInfo.hasDriverInfo() && vmInfo.getDriverInfo().hasHasEnaDriver() ?
            vmInfo.getDriverInfo().getHasEnaDriver() : null);
        setHasNVMeDriver(vmInfo.hasDriverInfo() && vmInfo.getDriverInfo().hasHasNvmeDriver() ?
            vmInfo.getDriverInfo().getHasNvmeDriver() : null);
        setArchitecture(vmInfo.hasArchitecture() ? vmInfo.getArchitecture() : null);
        setVirtualizationType(vmInfo.hasVirtualizationType() ? vmInfo.getVirtualizationType() : null);
        serviceEntityRepoDTO.setVirtualMachineInfoRepoDTO(this);
    }

    @Override
    @Nonnull
    public TypeSpecificInfo createTypeSpecificInfo() {

        final VirtualMachineInfo.Builder vmBuilder = VirtualMachineInfo.newBuilder();

        if (getIpAddressInfoList() != null) {
            getIpAddressInfoList().stream()
                    .filter(ipAddressRepoDTO ->
                            ipAddressRepoDTO.getIpAddress() != null)
                    .map(ipAddressDTO -> IpAddress.newBuilder()
                            .setIpAddress(ipAddressDTO.getIpAddress())
                            .setIsElastic(ipAddressDTO.getElastic())
                            .build())
                    .forEach(vmBuilder::addIpAddresses);
        }
        if (getGuestOsInfo() != null) {
            final GuestOSRepoDTO guestOsRepoDTO = getGuestOsInfo();
            vmBuilder.setGuestOsInfo(OS.newBuilder()
                .setGuestOsType(guestOsRepoDTO.getGuestOsType())
                .setGuestOsName(guestOsRepoDTO.getGuestOsName()).build());
        }
        if (getTenancy() != null) {
            vmBuilder.setTenancy(Tenancy.valueOf(
                    getTenancy()));
        }
        if (getNumCpus() != null) {
            vmBuilder.setNumCpus(getNumCpus());
        }
        if (getConnectedNetworks() != null) {
            vmBuilder.addAllConnectedNetworks(getConnectedNetworks());
        }

        vmBuilder.setLicenseModel(getLicenseModel());
        if (getHasEnaDriver() != null || getHasNVMeDriver() != null) {
            DriverInfo.Builder driverInfo = DriverInfo.newBuilder();
            if (getHasEnaDriver() != null) {
                driverInfo.setHasEnaDriver(getHasEnaDriver());
            }
            if (getHasNVMeDriver() != null) {
                driverInfo.setHasNvmeDriver(getHasNVMeDriver());
            }
            vmBuilder.setDriverInfo(driverInfo);
        }
        if (getArchitecture() != null) {
            vmBuilder.setArchitecture(getArchitecture());
        }
        if (getVirtualizationType() != null) {
            vmBuilder.setVirtualizationType(getVirtualizationType());
        }

        return TypeSpecificInfo.newBuilder()
                .setVirtualMachine(vmBuilder)
                .build();
    }


    public GuestOSRepoDTO getGuestOsInfo() {
        return guestOsInfo;
    }

    public String getTenancy() {
        return tenancy;
    }

    public List<IpAddressRepoDTO> getIpAddressInfoList() {
        return ipAddressInfoList;
    }

    public Integer getNumCpus() {
        return numCpus;
    }

    public LicenseModel getLicenseModel() {
        return licenseModel;
    }

    public void setGuestOsInfo(GuestOSRepoDTO guestOsInfo) {
        this.guestOsInfo = guestOsInfo;
    }

    public void setTenancy(String tenancy) {
        this.tenancy = tenancy;
    }

    public void setIpAddressInfoList(List<IpAddressRepoDTO> ipAddressInfoList) {
        this.ipAddressInfoList = ipAddressInfoList;
    }

    public void setNumCpus(Integer numCpus) {
        this.numCpus = numCpus;
    }

    public void setLicenseModel(LicenseModel licenseModel) {
        this.licenseModel = licenseModel;
    }

    public List<String> getConnectedNetworks() {
        return connectedNetworks;
    }

    public void setConnectedNetworks(List<String> connectedNetworks) {
        this.connectedNetworks = connectedNetworks;
    }

    public Boolean getHasEnaDriver() {
        return hasEnaDriver;
    }

    public void setHasEnaDriver(final Boolean hasEnaDriver) {
        this.hasEnaDriver = hasEnaDriver;
    }

    public Boolean getHasNVMeDriver() {
        return hasNVMeDriver;
    }

    public void setHasNVMeDriver(final Boolean hasNVMeDriver) {
        this.hasNVMeDriver = hasNVMeDriver;
    }

    public Architecture getArchitecture() {
        return architecture;
    }

    public void setArchitecture(final Architecture architecture) {
        this.architecture = architecture;
    }

    public VirtualizationType getVirtualizationType() {
        return virtualizationType;
    }

    public void setVirtualizationType(final VirtualizationType virtualizationType) {
        this.virtualizationType = virtualizationType;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final VirtualMachineInfoRepoDTO that = (VirtualMachineInfoRepoDTO) o;
        return Objects.equals(guestOsInfo, that.guestOsInfo) &&
                Objects.equals(tenancy, that.tenancy) &&
                Objects.equals(ipAddressInfoList, that.ipAddressInfoList) &&
                Objects.equals(numCpus, that.numCpus) &&
                Objects.equals(licenseModel, that.licenseModel) &&
                Objects.equals(connectedNetworks, that.connectedNetworks) &&
                Objects.equals(hasNVMeDriver, that.hasNVMeDriver) &&
                Objects.equals(hasEnaDriver, that.hasEnaDriver) &&
                Objects.equals(architecture, that.architecture) &&
                Objects.equals(virtualizationType, that.virtualizationType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(guestOsInfo, tenancy, ipAddressInfoList, numCpus, licenseModel,
            connectedNetworks, hasNVMeDriver, hasEnaDriver, architecture, virtualizationType);
    }

    @Override
    public String toString() {
        return "VirtualMachineInfoRepoDTO{" +
                "guestOsInfo=" + guestOsInfo +
                ", tenancy='" + tenancy + '\'' +
                ", ipAddressInfoList=" + ipAddressInfoList +
                ", numCpus=" + numCpus +
                ", licenseModel=" + licenseModel +
                ", connectedNetworks=" + connectedNetworks +
                ", hasNVMeDriver=" + hasNVMeDriver +
                ", hasEnaDriver=" + hasEnaDriver +
                ", architecture=" + architecture +
                ", virtualizationType=" + virtualizationType +
                '}';
    }
}
