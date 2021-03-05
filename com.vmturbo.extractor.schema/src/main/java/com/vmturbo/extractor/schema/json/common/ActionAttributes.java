package com.vmturbo.extractor.schema.json.common;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Action attributes for the ATTRS columns in the pending_action and completed_action tables.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class ActionAttributes {
    // mapping from entity type to provider change
    private Map<String, MoveChange> moveInfo;
    // mapping from commodity type to commodity change
    private Map<String, CommodityChange> resizeInfo;
    // info for delete action
    private DeleteInfo deleteInfo;
    // info for buyRI action
    private BuyRiInfo buyRiInfo;

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

    public DeleteInfo getDeleteInfo() {
        return deleteInfo;
    }

    public void setDeleteInfo(DeleteInfo deleteInfo) {
        this.deleteInfo = deleteInfo;
    }

    public BuyRiInfo getBuyRiInfo() {
        return buyRiInfo;
    }

    public void setBuyRiInfo(BuyRiInfo buyRiInfo) {
        this.buyRiInfo = buyRiInfo;
    }
}
