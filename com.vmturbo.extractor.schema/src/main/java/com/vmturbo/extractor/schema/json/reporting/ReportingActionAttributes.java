package com.vmturbo.extractor.schema.json.reporting;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import com.vmturbo.extractor.schema.json.common.ActionAttributes;
import com.vmturbo.extractor.schema.json.common.CommodityChange;
import com.vmturbo.extractor.schema.json.common.MoveChange;

/**
 * Action attributes for the ATTRS columns in the pending_action and completed_action tables.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class ReportingActionAttributes extends ActionAttributes {
    // mapping from entity type to provider change
    private Map<String, MoveChange> moveInfo;
    // mapping from commodity type to commodity change
    private Map<String, CommodityChange> resizeInfo;
    // provider change, same as moveInfo but with different field name to match SCALE action type
    private Map<String, MoveChange> scaleInfo;

    public Map<String, MoveChange> getMoveInfo() {
        return moveInfo;
    }

    public void setMoveInfo(Map<String, MoveChange> moveInfo) {
        this.moveInfo = moveInfo;
    }

    public Map<String, CommodityChange> getResizeInfo() {
        return resizeInfo;
    }

    public void setResizeInfo(Map<String, CommodityChange> resizeInfo) {
        this.resizeInfo = resizeInfo;
    }

    public Map<String, MoveChange> getScaleInfo() {
        return scaleInfo;
    }

    public void setScaleInfo(Map<String, MoveChange> scaleInfo) {
        this.scaleInfo = scaleInfo;
    }
}
