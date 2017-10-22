package com.vmturbo.repository.dto;

import java.util.List;
import java.util.Objects;

public class ServiceEntityRepoDTO {
    protected String uuid;

    protected String displayName;

    private String entityType;

    private Float priceIndex;

    private Float priceIndexProjected;

    private String state;

    private String severity;

    private String oid;

    private List<String> providers;

    private List<CommoditiesBoughtRepoFromProviderDTO> commoditiesBoughtRepoFromProviderDTOList;

    private List<CommoditySoldRepoDTO> commoditySoldList;

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

    @Override
    public int hashCode() {
        return Objects.hash(commoditiesBoughtRepoFromProviderDTOList, commoditySoldList, displayName,
                            entityType, oid, priceIndex, priceIndexProjected, providers, severity,
                            state, uuid);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ServiceEntityRepoDTO)) {
            return false;
        }
        ServiceEntityRepoDTO that = (ServiceEntityRepoDTO)obj;
        return Objects.equals(commoditiesBoughtRepoFromProviderDTOList, that.commoditiesBoughtRepoFromProviderDTOList) &&
               Objects.equals(commoditySoldList, that.commoditySoldList) &&
               Objects.equals(displayName, that.displayName) &&
               Objects.equals(entityType, that.entityType) &&
               Objects.equals(oid, that.oid) &&
               Objects.equals(priceIndex, that.priceIndex) &&
               Objects.equals(priceIndexProjected, that.priceIndexProjected) &&
               Objects.equals(providers, that.providers) &&
               Objects.equals(severity, that.severity) &&
               Objects.equals(state, that.state) &&
               Objects.equals(uuid, that.uuid);
    }

    @Override
    public String toString() {
        return "ServiceEntityRepoDTO [uuid=" + uuid + ", displayName=" + displayName
                + ", entityType=" + entityType + ", priceIndex=" + priceIndex
                + ", priceIndexProjected=" + priceIndexProjected
                + ", state=" + state + ", severity=" + severity + ", oid=" + oid
                + ", providers=" + providers + ", commoditiesBoughtRepoFromProviderDTOList="
                + commoditiesBoughtRepoFromProviderDTOList + ", commoditySoldList=" + commoditySoldList + "]";
    }


}