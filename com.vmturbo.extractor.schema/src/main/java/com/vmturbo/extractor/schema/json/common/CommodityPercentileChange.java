package com.vmturbo.extractor.schema.json.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Information about the percentile changes of a particular commodity caused by an action.
 *
 * <p/>Note: The name of the commodity is not present, because the commodity name will typically
 * be the key in a map, and the {@link CommodityPercentileChange} will be embedded in the value.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class CommodityPercentileChange {
    private Integer observationPeriodDays;
    private Integer aggressiveness;
    private Double before;
    private Double after;

    public Double getAfter() {
        return after;
    }

    public void setAfter(Double afterPercentile) {
        this.after = afterPercentile;
    }

    public Double getBefore() {
        return before;
    }

    public void setBefore(Double beforePercentile) {
        this.before = beforePercentile;
    }

    public Integer getObservationPeriodDays() {
        return observationPeriodDays;
    }

    public void setObservationPeriodDays(Integer observationPeriodDays) {
        this.observationPeriodDays = observationPeriodDays;
    }

    public Integer getAggressiveness() {
        return aggressiveness;
    }

    public void setAggressiveness(Integer aggressiveness) {
        this.aggressiveness = aggressiveness;
    }

    /**
     * Return whether all fields are non-null.
     *
     * @return True if all fields are set.
     */
    public boolean allFieldsSet() {
        return observationPeriodDays != null
            && aggressiveness != null
            && before != null
            && after != null;
    }
}
