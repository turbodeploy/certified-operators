package com.vmturbo.extractor.schema.json.common;

/**
 * Common action attributes between reporting and exporter.
 */
public class ActionAttributes {
    // info for delete action
    private DeleteInfo deleteInfo;
    // info for buyRI action
    private BuyRiInfo buyRiInfo;

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
