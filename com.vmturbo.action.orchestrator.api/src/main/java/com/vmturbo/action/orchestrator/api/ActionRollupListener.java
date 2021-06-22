package com.vmturbo.action.orchestrator.api;

import com.vmturbo.action.orchestrator.api.export.ActionRollupExport.ActionRollupNotification;

/**
 * Listens to action rollup notifications and tracks historical pending action counts.
 */
public interface ActionRollupListener {

    /**
     * Process an incoming ActionRollupNotification.
     *
     * @param rollupNotification the rollup to process.
     */
    void onRollup(ActionRollupNotification rollupNotification);

}
