package com.vmturbo.repository.dto;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Class containing all the fields that need to be saved to ArangoDB, all the info comes from
 * TopologyEntityDTO.
 *
 * Note: Any change to the name of existing getter should also be reflected in
 * "groupBuilderUsecases.json", since the name of the field saved to ArangoDB is based on the
 * getter. For example: for "virtualMachineInfo", getter is "getVirtualMachineInfoRepoDTO", then it
 * is saved as "virtualMachineInfoRepoDTO". Maybe we can consider using @JsonProperty(...).
 * See https://github.com/FasterXML/jackson-databind#annotations-changing-property-names
 */
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

    // see ScopedEntity.targetIds for why we use string for targetIds here
    private List<String> targetIds;

    // TODO: consider having only one of these with a supertype - any entity will have at most one
    private ApplicationInfoRepoDTO applicationInfoRepoDTO;
    private DatabaseInfoRepoDTO databaseInfoRepoDTO;
    private ComputeTierInfoRepoDTO computeTierInfoRepoDTO;
    private PhysicalMachineInfoRepoDTO physicalMachineInfoRepoDTO;
    private StorageInfoRepoDTO storageInfoRepoDTO;
    private VirtualMachineInfoRepoDTO virtualMachineInfoRepoDTO;
    private VirtualVolumeInfoRepoDTO virtualVolumeInfoRepoDTO;
    private BusinessAccountInfoRepoDTO businessAccountInfoRepoDTO;

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

    public List<String> getTargetIds() {
        return targetIds;
    }

    public void setTargetIds(List<String> targetIds) {
        this.targetIds = targetIds;
    }

    public VirtualMachineInfoRepoDTO getVirtualMachineInfoRepoDTO() {
        return virtualMachineInfoRepoDTO;
    }

    public void setVirtualMachineInfoRepoDTO(final VirtualMachineInfoRepoDTO virtualMachineInfo) {
        this.virtualMachineInfoRepoDTO = virtualMachineInfo;
    }

    public ComputeTierInfoRepoDTO getComputeTierInfoRepoDTO() {
        return computeTierInfoRepoDTO;
    }

    public void setComputeTierInfoRepoDTO(final ComputeTierInfoRepoDTO computeTierInfoRepoDTO) {
        this.computeTierInfoRepoDTO = computeTierInfoRepoDTO;
    }

    public VirtualVolumeInfoRepoDTO getVirtualVolumeInfoRepoDTO() {
        return virtualVolumeInfoRepoDTO;
    }

    public void setVirtualVolumeInfoRepoDTO(final VirtualVolumeInfoRepoDTO virtualVolumeInfo) {
        this.virtualVolumeInfoRepoDTO = virtualVolumeInfo;
    }

    public BusinessAccountInfoRepoDTO getBusinessAccountInfoRepoDTO() {
        return businessAccountInfoRepoDTO;
    }

    public void setBusinessAccountInfoRepoDTO(final BusinessAccountInfoRepoDTO businessAccountInfoRepoDTO) {
        this.businessAccountInfoRepoDTO = businessAccountInfoRepoDTO;
    }

    @Override
    public int hashCode() {
        return Objects.hash(commoditiesBoughtRepoFromProviderDTOList, commoditySoldList, displayName,
            entityType, environmentType, oid, priceIndex, priceIndexProjected,
            providers, severity, state, uuid, tags, connectedEntityList, targetIds,
            virtualMachineInfoRepoDTO, computeTierInfoRepoDTO, virtualVolumeInfoRepoDTO,
            businessAccountInfoRepoDTO);
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
                Objects.equals(virtualMachineInfoRepoDTO, that.virtualMachineInfoRepoDTO) &&
                Objects.equals(computeTierInfoRepoDTO, that.computeTierInfoRepoDTO) &&
                Objects.equals(virtualVolumeInfoRepoDTO, that.virtualVolumeInfoRepoDTO) &&
                Objects.equals(businessAccountInfoRepoDTO, that.businessAccountInfoRepoDTO);
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
                + ", virtualMachineInfoRepoDTO=" + virtualMachineInfoRepoDTO
                + ", computerTierInfoRepoDTO=" + computeTierInfoRepoDTO
                + ", virtualVolumeInfoRepoDTO=" + virtualVolumeInfoRepoDTO
                + ", businessAccountInfoRepoDTO=" + businessAccountInfoRepoDTO
                + "]";
    }

    public ApplicationInfoRepoDTO getApplicationInfoRepoDTO() {
        return applicationInfoRepoDTO;
    }

    public void setApplicationInfoRepoDTO(final ApplicationInfoRepoDTO applicationInfoRepoDTO) {
        this.applicationInfoRepoDTO = applicationInfoRepoDTO;
    }

    public DatabaseInfoRepoDTO getDatabaseInfoRepoDTO() {
        return databaseInfoRepoDTO;
    }

    public void setDatabaseInfoRepoDTO(final DatabaseInfoRepoDTO databaseInfoRepoDTO) {
        this.databaseInfoRepoDTO = databaseInfoRepoDTO;
    }

    public StorageInfoRepoDTO getStorageInfoRepoDTO() {
        return storageInfoRepoDTO;
    }

    public void setStorageInfoRepoDTO(final StorageInfoRepoDTO storageInfoRepoDTO) {
        this.storageInfoRepoDTO = storageInfoRepoDTO;
    }

    public PhysicalMachineInfoRepoDTO getPhysicalMachineInfoRepoDTO() {
        return physicalMachineInfoRepoDTO;
    }

    public void setPhysicalMachineInfoRepoDTO(final PhysicalMachineInfoRepoDTO physicalMachineInfoRepoDTO) {
        this.physicalMachineInfoRepoDTO = physicalMachineInfoRepoDTO;
    }
}