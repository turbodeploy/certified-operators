package com.vmturbo.platform.analysis.utilities.exceptions;

/**
 * Throw this exception if an action can't be replayed.
 */
public class ActionCantReplayException extends Exception {

    private final int missingCommodityId;

    /**
     * Constructor.
     *
     * @param missingCommodityId missing commodity id
     */
    public ActionCantReplayException(int missingCommodityId) {
        this.missingCommodityId = missingCommodityId;
    }

    /**
     * Return missing commodity id.
     *
     * @return missing commodity id
     */
    public int getMissingCommodityId() {
        return missingCommodityId;
    }
}
