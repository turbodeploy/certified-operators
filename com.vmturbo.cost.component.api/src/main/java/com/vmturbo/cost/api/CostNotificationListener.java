package com.vmturbo.cost.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;

/**
 * The listener for the cost component.
 */
public interface CostNotificationListener {

    /**
     * It receives the messages from the market in the final step of running plan.
     *
     * @param costNotification The cost notification
     */
    void onCostNotificationReceived(@Nonnull CostNotification costNotification);

}
