package com.vmturbo.extractor.schema.json.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Describing the commodity change in resize action.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class CommodityChange {
    private Float from;
    private Float to;
    private String unit;
    private String attribute;
    // target is set for nested actions inside atomic resize, since it's different from main target
    private ActionEntity target;

    private CommodityPercentileChange percentileChange;

    public Float getFrom() {
        return from;
    }

    public void setFrom(Float from) {
        this.from = from;
    }

    public Float getTo() {
        return to;
    }

    public void setTo(Float to) {
        this.to = to;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public ActionEntity getTarget() {
        return target;
    }

    public void setTarget(ActionEntity target) {
        this.target = target;
    }

    public CommodityPercentileChange getPercentileChange() {
        return percentileChange;
    }

    public void setPercentileChange(CommodityPercentileChange percentileChange) {
        this.percentileChange = percentileChange;
    }
}