package com.vmturbo.extractor.schema.json.export;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Class representing the commodity which will be extracted to external system. It's used for both
 * sold commodity and bought commodity. In most cases, it either contains current/capacity/utilization
 * for sold commodity or consumed for bought commodity. But it may contain both, if an entity buys
 * and sells same type of commodity.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class Commodity {
    // sold
    private Double current;
    private Double capacity;
    private Double utilization;
    // bought
    private Double consumed;

    public Double getCurrent() {
        return current;
    }

    public void setCurrent(Double current) {
        this.current = current;
    }

    public Double getCapacity() {
        return capacity;
    }

    public void setCapacity(Double capacity) {
        this.capacity = capacity;
    }

    public Double getUtilization() {
        return utilization;
    }

    public void setUtilization(Double utilization) {
        this.utilization = utilization;
    }

    public Double getConsumed() {
        return consumed;
    }

    public void setConsumed(Double consumed) {
        this.consumed = consumed;
    }
}
