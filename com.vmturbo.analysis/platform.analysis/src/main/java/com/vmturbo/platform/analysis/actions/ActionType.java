package com.vmturbo.platform.analysis.actions;

/**
 * Action Type of an action.
 */
public enum ActionType {

    /**
     * Move.
     */
    MOVE,
    /**
     * Resize.
     */
    RESIZE,
    /**
     * Provision By Demand.
     */
    PROVISION_BY_DEMAND,
    /**
     * Provision By Supply.
     */
    PROVISION_BY_SUPPLY,
    /**
     * Activate.
     */
    ACTIVATE,
    /**
     * Deactivate.
     */
    DEACTIVATE,
    /**
     * Reconfigure Consumer.
     */
    RECONFIGURE_CONSUMER,
    /**
     * Compound Move.
     */
    COMPOUND_MOVE,
    /**
     * Reconfigure Provider Addition.
     */
    RECONFIGURE_PROVIDER_ADDITION,
    /**
     * Reconfigure Provider Removal.
     */
    RECONFIGURE_PROVIDER_REMOVAL,
    /**
     * Unknown.
     */
    UNKNOWN

}
