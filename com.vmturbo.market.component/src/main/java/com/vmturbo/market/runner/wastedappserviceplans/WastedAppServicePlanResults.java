package com.vmturbo.market.runner.wastedappserviceplans;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;

/**
 * Object representing the results of the wasted ASP analysis.
 */
public class WastedAppServicePlanResults {

    /**
     * Singleton instance to represent no results.
     */
    public static final WastedAppServicePlanResults EMPTY = new WastedAppServicePlanResults(Collections.emptyList());

    private final Collection<Action> actions;

    WastedAppServicePlanResults(List<Action> actions) {
        this.actions = actions;
    }

    public Collection<Action> getActions() {
        return actions;
    }
}
