package com.vmturbo.extractor.schema.json.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Settings for an entity.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class EntitySettings {
    private Integer percentileAggressiveness;
    private Integer percentileObservationPeriodDays;

    public Integer getPercentileAggressiveness() {
        return percentileAggressiveness;
    }

    public void setPercentileAggressiveness(Integer percentileAggressiveness) {
        this.percentileAggressiveness = percentileAggressiveness;
    }

    public Integer getPercentileObservationPeriodDays() {
        return percentileObservationPeriodDays;
    }

    public void setPercentileObservationPeriodDays(Integer percentileObservationPeriodDays) {
        this.percentileObservationPeriodDays = percentileObservationPeriodDays;
    }
}
