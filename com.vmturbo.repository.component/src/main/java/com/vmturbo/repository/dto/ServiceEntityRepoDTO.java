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
    private Map<String, String> targetVendorIds;

    private EntityPipelineErrorsRepoDTO entityPipelineErrorsRepoDTO;

    // TODO: consider having only one of these with a supertype - any entity will have at most one
    private ApplicationInfoRepoDTO applicationInfoRepoDTO;
    private DatabaseInfoRepoDTO databaseInfoRepoDTO;
    private ComputeTierInfoRepoDTO computeTierInfoRepoDTO;
    private PhysicalMachineInfoRepoDTO physicalMachineInfoRepoDTO;
    private StorageInfoRepoDTO storageInfoRepoDTO;
    private DiskArrayInfoRepoDTO diskArrayInfoRepoDTO;
    private LogicalPoolInfoRepoDTO logicalPoolInfoRepoDTO;
    private StorageControllerInfoRepoDTO storageControllerInfoRepoDTO;
    private VirtualMachineInfoRepoDTO virtualMachineInfoRepoDTO;
    private VirtualVolumeInfoRepoDTO virtualVolumeInfoRepoDTO;
    private BusinessAccountInfoRepoDTO businessAccountInfoRepoDTO;
    private DesktopPoolInfoRepoDTO desktopPoolInfoRepoDTO;
    private BusinessUserInfoRepoDTO businessUserInfoRepoDTO;

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

    public Map<String, String> getTargetVendorIds() {
        return targetVendorIds;
    }

    public void setTargetVendorIds(Map<String, String> targetVendorIds) {
        this.targetVendorIds = targetVendorIds;
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

    public VirtualMachineInfoRepoDTO getVirtualMachineInfoRepoDTO() {
        return virtualMachineInfoRepoDTO;
    }

    public void setVirtualMachineInfoRepoDTO(final VirtualMachineInfoRepoDTO virtualMachineInfo) {
        this.virtualMachineInfoRepoDTO = virtualMachineInfo;
    }

    public PhysicalMachineInfoRepoDTO getPhysicalMachineInfoRepoDTO() {
        return physicalMachineInfoRepoDTO;
    }

    public void setPhysicalMachineInfoRepoDTO(final PhysicalMachineInfoRepoDTO physicalMachineInfoRepoDTO) {
        this.physicalMachineInfoRepoDTO = physicalMachineInfoRepoDTO;
    }

    public StorageInfoRepoDTO getStorageInfoRepoDTO() {
        return storageInfoRepoDTO;
    }

    public void setStorageInfoRepoDTO(final StorageInfoRepoDTO storageInfoRepoDTO) {
        this.storageInfoRepoDTO = storageInfoRepoDTO;
    }

    public DiskArrayInfoRepoDTO getDiskArrayInfoRepoDTO() {
        return diskArrayInfoRepoDTO;
    }

    public void setDiskArrayInfoRepoDTO(final DiskArrayInfoRepoDTO diskArrayInfoRepoDTO) {
        this.diskArrayInfoRepoDTO = diskArrayInfoRepoDTO;
    }

    public LogicalPoolInfoRepoDTO getLogicalPoolInfoRepoDTO() {
        return logicalPoolInfoRepoDTO;
    }

    public void setLogicalPoolInfoRepoDTO(final LogicalPoolInfoRepoDTO logicalPoolInfoRepoDTO) {
        this.logicalPoolInfoRepoDTO = logicalPoolInfoRepoDTO;
    }

    public StorageControllerInfoRepoDTO getStorageControllerInfoRepoDTO() {
        return storageControllerInfoRepoDTO;
    }

    public void setStorageControllerInfoRepoDTO(final StorageControllerInfoRepoDTO storageControllerInfoRepoDTO) {
        this.storageControllerInfoRepoDTO = storageControllerInfoRepoDTO;
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

    public EntityPipelineErrorsRepoDTO getEntityPipelineErrorsRepoDTO() {
        return entityPipelineErrorsRepoDTO;
    }

    public void setEntityPipelineErrorsRepoDTO(final EntityPipelineErrorsRepoDTO entityPipelineErrorsRepoDTO) {
        this.entityPipelineErrorsRepoDTO = entityPipelineErrorsRepoDTO;
    }

    public DesktopPoolInfoRepoDTO getDesktopPoolInfoRepoDTO() {
        return desktopPoolInfoRepoDTO;
    }

    public void setDesktopPoolInfoRepoDTO(DesktopPoolInfoRepoDTO desktopPoolInfoRepoDTO) {
        this.desktopPoolInfoRepoDTO = desktopPoolInfoRepoDTO;
    }

    public BusinessUserInfoRepoDTO getBusinessUserInfoRepoDTO() {
        return businessUserInfoRepoDTO;
    }

    public void setBusinessUserInfoRepoDTO(BusinessUserInfoRepoDTO businessUserInfoRepoDTO) {
        this.businessUserInfoRepoDTO = businessUserInfoRepoDTO;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ServiceEntityRepoDTO that = (ServiceEntityRepoDTO) o;
        return Objects.equals(uuid, that.uuid) &&
                Objects.equals(displayName, that.displayName) &&
                Objects.equals(entityType, that.entityType) &&
                Objects.equals(environmentType, that.environmentType) &&
                Objects.equals(priceIndex, that.priceIndex) &&
                Objects.equals(priceIndexProjected, that.priceIndexProjected) &&
                Objects.equals(state, that.state) &&
                Objects.equals(severity, that.severity) &&
                Objects.equals(oid, that.oid) &&
                Objects.equals(tags, that.tags) &&
                Objects.equals(providers, that.providers) &&
                Objects.equals(commoditiesBoughtRepoFromProviderDTOList,
                    that.commoditiesBoughtRepoFromProviderDTOList) &&
                Objects.equals(commoditySoldList, that.commoditySoldList) &&
                Objects.equals(connectedEntityList, that.connectedEntityList) &&
                Objects.equals(targetVendorIds, that.targetVendorIds) &&
                Objects.equals(applicationInfoRepoDTO, that.applicationInfoRepoDTO) &&
                Objects.equals(databaseInfoRepoDTO, that.databaseInfoRepoDTO) &&
                Objects.equals(computeTierInfoRepoDTO, that.computeTierInfoRepoDTO) &&
                Objects.equals(physicalMachineInfoRepoDTO, that.physicalMachineInfoRepoDTO) &&
                Objects.equals(storageInfoRepoDTO, that.storageInfoRepoDTO) &&
                Objects.equals(diskArrayInfoRepoDTO, that.diskArrayInfoRepoDTO) &&
                Objects.equals(logicalPoolInfoRepoDTO, that.logicalPoolInfoRepoDTO) &&
                Objects.equals(storageControllerInfoRepoDTO, that.storageControllerInfoRepoDTO) &&
                Objects.equals(virtualMachineInfoRepoDTO, that.virtualMachineInfoRepoDTO) &&
                Objects.equals(virtualVolumeInfoRepoDTO, that.virtualVolumeInfoRepoDTO) &&
                Objects.equals(businessAccountInfoRepoDTO, that.businessAccountInfoRepoDTO) &&
                Objects.equals(desktopPoolInfoRepoDTO, that.desktopPoolInfoRepoDTO) &&
                Objects.equals(businessUserInfoRepoDTO, that.businessUserInfoRepoDTO);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, displayName, entityType, environmentType, priceIndex,
            priceIndexProjected, state, severity, oid, tags, providers,
            commoditiesBoughtRepoFromProviderDTOList, commoditySoldList, connectedEntityList,
            targetVendorIds, applicationInfoRepoDTO, databaseInfoRepoDTO, computeTierInfoRepoDTO,
            physicalMachineInfoRepoDTO, storageInfoRepoDTO, diskArrayInfoRepoDTO,
            logicalPoolInfoRepoDTO, storageControllerInfoRepoDTO, virtualMachineInfoRepoDTO,
            virtualVolumeInfoRepoDTO, businessAccountInfoRepoDTO, desktopPoolInfoRepoDTO,
            businessUserInfoRepoDTO);
    }

    @Override
    public String toString() {
        return "ServiceEntityRepoDTO{" +
                "uuid='" + uuid + '\'' +
                ", displayName='" + displayName + '\'' +
                ", entityType='" + entityType + '\'' +
                ", environmentType='" + environmentType + '\'' +
                ", priceIndex=" + priceIndex +
                ", priceIndexProjected=" + priceIndexProjected +
                ", state='" + state + '\'' +
                ", severity='" + severity + '\'' +
                ", oid='" + oid + '\'' +
                ", tags=" + tags +
                ", providers=" + providers +
                ", commoditiesBoughtRepoFromProviderDTOList=" + commoditiesBoughtRepoFromProviderDTOList +
                ", commoditySoldList=" + commoditySoldList +
                ", connectedEntityList=" + connectedEntityList +
                ", targetVendorIds=" + targetVendorIds +
                ", applicationInfoRepoDTO=" + applicationInfoRepoDTO +
                ", databaseInfoRepoDTO=" + databaseInfoRepoDTO +
                ", computeTierInfoRepoDTO=" + computeTierInfoRepoDTO +
                ", physicalMachineInfoRepoDTO=" + physicalMachineInfoRepoDTO +
                ", storageInfoRepoDTO=" + storageInfoRepoDTO +
                ", diskArrayInfoRepoDTO=" + diskArrayInfoRepoDTO +
                ", logicalPoolInfoRepoDTO=" + logicalPoolInfoRepoDTO +
                ", storageControllerInfoRepoDTO=" + storageControllerInfoRepoDTO +
                ", virtualMachineInfoRepoDTO=" + virtualMachineInfoRepoDTO +
                ", virtualVolumeInfoRepoDTO=" + virtualVolumeInfoRepoDTO +
                ", businessAccountInfoRepoDTO=" + businessAccountInfoRepoDTO +
                ", businessUserInfoRepoDTO=" + businessUserInfoRepoDTO +
                ", desktopPoolInfoRepoDTO=" + desktopPoolInfoRepoDTO +
                ", entityPipelineErrorsRepoDTO=" + entityPipelineErrorsRepoDTO +
                '}';
    }
}