package com.vmturbo.market.component.api;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;

/**
 * An object that listens to the market's notifications about
 * actions to take to bring the system into an optimal state.
 */
public interface ActionsListener {

    /**
     * Callback receiving the actions the market computed.
     *
     * @param actionPlan The actions recommended by the market.
     * @param tracingContext Distributed tracing context.
     * @throws InterruptedException if thread has been interrupted
     */
    void onActionsReceived(@Nonnull final ActionPlan actionPlan,
                           @Nonnull final SpanContext tracingContext) throws InterruptedException;

}
