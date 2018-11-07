package com.vmturbo.repository.dto;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ServiceEntityRepoDTO {
    protected String uuid;

    protected String displayName;

    private String entityType;

    /**
     * This should be a {@link com.vmturbo.components.common.mapping.UIEnvironmentType}.
     */
    private String environmentType;

    private Float priceIndex;

    private Float priceIndexProjected;

    private String state;

    private String severity;

    private String oid;

    private Map<String, List<String>> tags;

    private List<String> providers;

    private List<CommoditiesBoughtRepoFromProviderDTO> commoditiesBoughtRepoFromProviderDTOList;

    private List<CommoditySoldRepoDTO> commoditySoldList;

    private List<ConnectedEntityRepoDTO> connectedEntityList;

    private List<Long> targetIds;

    private VirtualMachineInfoRepoDTO virtualMachineInfo;

    private ComputeTierInfoRepoDTO computeTierInfo;

    private VirtualVolumeInfoRepoDTO virtualVolumeInfo;

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public String getEnvironmentType() {
        return environmentType;
    }

    public void setEnvironmentType(String environmentType) {
        this.environmentType = environmentType;
    }

    public Float getPriceIndex() {
        return priceIndex;
    }

    public void setPriceIndex(Float priceIndex) {
        this.priceIndex = priceIndex;
    }

    public Float getPriceIndexProjected() {
        return priceIndexProjected;
    }

    public void setPriceIndexProjected(Float priceIndexProjected) {
        this.priceIndexProjected = priceIndexProjected;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public List<String> getProviders() {
        return providers;
    }

    public void setProviders(List<String> providers) {
        this.providers = providers;
    }

    public Map<String, List<String>> getTags() {
        return tags;
    }

    public void setTags(Map<String, List<String>> tags) {
        this.tags = tags;
    }

    public List<CommoditiesBoughtRepoFromProviderDTO> getCommoditiesBoughtRepoFromProviderDTOList() {
        return commoditiesBoughtRepoFromProviderDTOList;
    }

    public void setCommoditiesBoughtRepoFromProviderDTOList(List<CommoditiesBoughtRepoFromProviderDTO> commoditiesBoughtRepoFromProviderDTOList) {
        this.commoditiesBoughtRepoFromProviderDTOList = commoditiesBoughtRepoFromProviderDTOList;
    }

    public List<CommoditySoldRepoDTO> getCommoditySoldList() {
        return commoditySoldList;
    }

    public void setCommoditySoldList(List<CommoditySoldRepoDTO> commoditySoldList) {
        this.commoditySoldList = commoditySoldList;
    }

    public List<ConnectedEntityRepoDTO> getConnectedEntityList() {
        return connectedEntityList;
    }

    public void setConnectedEntityList(List<ConnectedEntityRepoDTO> connectedEntityList) {
        this.connectedEntityList = connectedEntityList;
    }

    public List<Long> getTargetIds() {
        return targetIds;
    }

    public void setTargetIds(List<Long> targetIds) {
        this.targetIds = targetIds;
    }

    public VirtualMachineInfoRepoDTO getVirtualMachineInfo() {
        return virtualMachineInfo;
    }

    public void setVirtualMachineInfo(final VirtualMachineInfoRepoDTO virtualMachineInfo) {
        this.virtualMachineInfo = virtualMachineInfo;
    }

    public ComputeTierInfoRepoDTO getComputeTierInfo() {
        return computeTierInfo;
    }

    public void setComputeTierInfo(final ComputeTierInfoRepoDTO computeTierInfoRepoDTO) {
        this.computeTierInfo = computeTierInfoRepoDTO;
    }

    public VirtualVolumeInfoRepoDTO getVirtualVolumeInfo() {
        return virtualVolumeInfo;
    }

    public void setVirtualVolumeInfo(final VirtualVolumeInfoRepoDTO virtualVolumeInfo) {
        this.virtualVolumeInfo = virtualVolumeInfo;
    }

    @Override
    public int hashCode() {
        return Objects.hash(commoditiesBoughtRepoFromProviderDTOList, commoditySoldList, displayName,
            entityType, environmentType, oid, priceIndex, priceIndexProjected,
            providers, severity, state, uuid, tags, connectedEntityList, targetIds,
            virtualMachineInfo, computeTierInfo, virtualVolumeInfo);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ServiceEntityRepoDTO)) {
            return false;
        }
        ServiceEntityRepoDTO that = (ServiceEntityRepoDTO) obj;
        return Objects.equals(commoditiesBoughtRepoFromProviderDTOList, that.commoditiesBoughtRepoFromProviderDTOList) &&
                Objects.equals(commoditySoldList, that.commoditySoldList) &&
                Objects.equals(displayName, that.displayName) &&
                Objects.equals(entityType, that.entityType) &&
                Objects.equals(environmentType, that.environmentType) &&
                Objects.equals(oid, that.oid) &&
                Objects.equals(priceIndex, that.priceIndex) &&
                Objects.equals(priceIndexProjected, that.priceIndexProjected) &&
                Objects.equals(providers, that.providers) &&
                Objects.equals(severity, that.severity) &&
                Objects.equals(state, that.state) &&
                Objects.equals(uuid, that.uuid) &&
                Objects.equals(tags, that.tags) &&
                Objects.equals(connectedEntityList, that.connectedEntityList) &&
                Objects.equals(targetIds, that.targetIds) &&
                Objects.equals(virtualMachineInfo, that.virtualMachineInfo) &&
                Objects.equals(computeTierInfo, that.computeTierInfo) &&
                Objects.equals(virtualVolumeInfo, that.virtualVolumeInfo);
    }

    @Override
    public String toString() {
        return "ServiceEntityRepoDTO [uuid=" + uuid + ", displayName=" + displayName
                + ", entityType=" + entityType + ", priceIndex=" + priceIndex
                + ", environmentType=" + environmentType
                + ", priceIndexProjected=" + priceIndexProjected
                + ", state=" + state + ", severity=" + severity + ", oid=" + oid
                + ", providers=" + providers + ", commoditiesBoughtRepoFromProviderDTOList="
                + commoditiesBoughtRepoFromProviderDTOList
                + ", commoditySoldList=" + commoditySoldList
                + ", connectedEntityList=" + connectedEntityList
                + ", targetIds=" + targetIds
                + ", virtualMachineInfo=" + virtualMachineInfo
                + ", computerTierInfo=" + computeTierInfo
                + ", virtualVolumeInfo=" + virtualVolumeInfo
                + "]";
    }
}