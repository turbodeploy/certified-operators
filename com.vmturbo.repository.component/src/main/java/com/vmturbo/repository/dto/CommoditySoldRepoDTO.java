package com.vmturbo.repository.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_EMPTY)
public class CommoditySoldRepoDTO extends CommodityBaseRepoDTO {
    private double capacity;

    private double effectiveCapacityPercentage;

    private double reservedCapacity;

    private boolean isResizeable;

    private boolean isThin;

    private float capacityIncrement;

    public double getCapacity() {
        return capacity;
    }

    public void setCapacity(double capacity) {
        this.capacity = capacity;
    }

    public double getEffectiveCapacityPercentage() {
        return effectiveCapacityPercentage;
    }

    public void setEffectiveCapacityPercentage(double effectiveCapacityPercentage) {
        this.effectiveCapacityPercentage = effectiveCapacityPercentage;
    }

    public double getReservedCapacity() {
        return reservedCapacity;
    }

    public void setReservedCapacity(double reservedCapacity) {
        this.reservedCapacity = reservedCapacity;
    }

    public boolean isResizeable() {
        return isResizeable;
    }

    public void setResizeable(boolean isResizeable) {
        this.isResizeable = isResizeable;
    }

    public boolean isThin() {
        return isThin;
    }

    public void setThin(boolean isThin) {
        this.isThin = isThin;
    }

    public void setCapacityIncrement(float increment) {
        this.capacityIncrement = increment;
    }

    public float getCapacityIncrement() {
        return this.capacityIncrement;
    }
}
