package com.vmturbo.extractor.schema.json.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * The affected costs for actions, including costs before/after actions.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class ActionImpactedCosts {

    private ActionImpactedCost beforeActions;
    private ActionImpactedCost afterActions;

    public ActionImpactedCost getBeforeActions() {
        return beforeActions;
    }

    public void setBeforeActions(ActionImpactedCost beforeActions) {
        this.beforeActions = beforeActions;
    }

    public ActionImpactedCost getAfterActions() {
        return afterActions;
    }

    public void setAfterActions(ActionImpactedCost afterActions) {
        this.afterActions = afterActions;
    }

    /**
     * The affected costs for actions.
     */
    @JsonInclude(Include.NON_EMPTY)
    @JsonPropertyOrder(alphabetic = true)
    public static class ActionImpactedCost {

        private Float onDemandRate;
        private Float onDemandCost;
        private Float riCoveragePercentage;

        public Float getOnDemandRate() {
            return onDemandRate;
        }

        public void setOnDemandRate(Float onDemandRate) {
            this.onDemandRate = onDemandRate;
        }

        public Float getOnDemandCost() {
            return onDemandCost;
        }

        public void setOnDemandCost(Float onDemandCost) {
            this.onDemandCost = onDemandCost;
        }

        public Float getRiCoveragePercentage() {
            return riCoveragePercentage;
        }

        public void setRiCoveragePercentage(Float riCoveragePercentage) {
            this.riCoveragePercentage = riCoveragePercentage;
        }
    }
}

