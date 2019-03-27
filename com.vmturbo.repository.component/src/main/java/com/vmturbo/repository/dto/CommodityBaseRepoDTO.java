package com.vmturbo.repository.dto;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_EMPTY)
public abstract class CommodityBaseRepoDTO {

    private String uuid;

    private String type;

    private String key;

    private double used;

    private double peak;

    private String providerOid;

    private String ownerOid;

    private double scalingFactor;

    private String displayName;

    private List<String> aggregates;

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public double getUsed() {
        return used;
    }

    public void setUsed(double used) {
        this.used = used;
    }

    public double getPeak() {
        return peak;
    }

    public void setPeak(double peak) {
        this.peak = peak;
    }

    public String getProviderOid() {
        return providerOid;
    }

    public void setProviderOid(String providerOid) {
        this.providerOid = providerOid;
    }

    public String getOwnerOid() {
        return ownerOid;
    }

    public void setOwnerOid(String ownerOid) {
        this.ownerOid = ownerOid;
    }

    public void setScalingFactor(double scalingFactor) {
        this.scalingFactor = scalingFactor;
    }

    public double getScalingFactor() {
        return this.scalingFactor;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(final String displayName) {
        this.displayName = displayName;
    }

    public List<String> getAggregates() {
        return aggregates;
    }

    public void setAggregates(final List<String> aggregates) {
        this.aggregates = aggregates;
    }

    @Override
    public String toString() {
        return "CommodityBaseRepoDTO{" +
                "uuid='" + uuid + '\'' +
                ", type='" + type + '\'' +
                ", key='" + key + '\'' +
                ", used=" + used +
                ", peak=" + peak +
                ", providerOid='" + providerOid + '\'' +
                ", ownerOid='" + ownerOid + '\'' +
                ", scalingFactor=" + scalingFactor +
                ", displayName='" + displayName + '\'' +
                ", aggregates=" + aggregates +
                '}';
    }
}