package com.vmturbo.extractor.schema.json.export;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import com.vmturbo.extractor.schema.json.common.ActionAttributes;
import com.vmturbo.extractor.schema.json.common.CommodityChange;
import com.vmturbo.extractor.schema.json.common.DeleteInfo;
import com.vmturbo.extractor.schema.json.common.MoveChange;
import com.vmturbo.extractor.schema.json.reporting.ReportingActionAttributes;

/**
 * Action attributes for the exported action. This is designed to be different from the attributes
 * defined in {@link ReportingActionAttributes} while sharing some common fields like
 * {@link DeleteInfo}. The attributes for exported action are flattened in a way to allow
 * customers to create reports more easily in their own BI tools.
 * For example: the commodity type is now a field in {@link CommodityChange} rather than the keys
 * of a map like we do in {@link ReportingActionAttributes}. If an atomic resize action contains
 * multiple commodity changes, then we will export two actions with same oid but different
 * CommodityChange. Same rules apply to compound move actions.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class ExporterActionAttributes extends ActionAttributes {
    // provider change
    private MoveChange moveInfo;
    // commodity change
    private CommodityChange resizeInfo;
    // provider change, same as moveInfo but with different field name to match SCALE action type
    private MoveChange scaleInfo;

    public MoveChange getMoveInfo() {
        return moveInfo;
    }

    public void setMoveInfo(MoveChange moveInfo) {
        this.moveInfo = moveInfo;
    }

    public CommodityChange getResizeInfo() {
        return resizeInfo;
    }

    public void setResizeInfo(CommodityChange resizeInfo) {
        this.resizeInfo = resizeInfo;
    }

    public MoveChange getScaleInfo() {
        return scaleInfo;
    }

    public void setScaleInfo(MoveChange scaleInfo) {
        this.scaleInfo = scaleInfo;
    }
}
