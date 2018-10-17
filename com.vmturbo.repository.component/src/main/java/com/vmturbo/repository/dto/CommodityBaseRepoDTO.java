package com.vmturbo.repository.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.MoreObjects;

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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("uuid", uuid)
                .add("type", type)
                .add("key", key)
                .add("used", used)
                .add("peak", peak)
                .add("providerOid", providerOid)
                .add("ownerOid", ownerOid)
                .add("scalingFactor", scalingFactor)
                .toString();
    }
}