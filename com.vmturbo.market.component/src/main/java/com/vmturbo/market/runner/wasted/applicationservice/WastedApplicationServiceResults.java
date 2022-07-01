package com.vmturbo.market.runner.wasted.applicationservice;

import java.util.Collections;
import java.util.List;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.market.runner.wasted.WastedEntityResults;

/**
 * Object representing the results of the wasted Application Service analysis.
 * This is currently used for Azure App Service Plans but in the future could be extended
 * to support similar services like AWS ElasticBeanstalk, GCP App Engine, DO App Platform, etc.
 */
public class WastedApplicationServiceResults extends WastedEntityResults {

    /**
     * Singleton instance to represent no results.
     */
    public static final WastedApplicationServiceResults
            EMPTY = new WastedApplicationServiceResults(Collections.emptyList());

    /**
     * Create new WastedResults.
     * @param actions delete actions for wasted entities.
     */
    WastedApplicationServiceResults(List<Action> actions) {
        super(actions);
    }
}
