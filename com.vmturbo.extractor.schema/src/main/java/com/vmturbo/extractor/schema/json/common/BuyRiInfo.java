package com.vmturbo.extractor.schema.json.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Describing the details in buyRI action. These fields are based on those defined in
 * {@link com.vmturbo.common.protobuf.action.ActionDTO.BuyRI}.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class BuyRiInfo {
    private Integer count;
    private ActionEntity computeTier;
    private ActionEntity region;
    private ActionEntity masterAccount;
    private ActionEntity target;

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public ActionEntity getComputeTier() {
        return computeTier;
    }

    public void setComputeTier(ActionEntity computeTier) {
        this.computeTier = computeTier;
    }

    public ActionEntity getRegion() {
        return region;
    }

    public void setRegion(ActionEntity region) {
        this.region = region;
    }

    public void setMasterAccount(ActionEntity masterAccount) {
        this.masterAccount = masterAccount;
    }

    public ActionEntity getMasterAccount() {
        return masterAccount;
    }

    public void setTarget(ActionEntity target) {
        this.target = target;
    }

    public ActionEntity getTarget() {
        return target;
    }
}