package com.vmturbo.extractor.schema.json.common;

/**
 * Common action attributes between reporting and exporter.
 */
public class ActionAttributes {

    /**
     * For some actions we populate the impact of the action on the target entity.
     *
     * <p/>For PROVISION, SCALE, and ACTIVATE we populate the before/after metrics on the target entity.
     */
    private ActionImpactedEntity target;

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

    public ActionImpactedEntity getTarget() {
        return target;
    }

    public void setTarget(ActionImpactedEntity target) {
        this.target = target;
    }

}
