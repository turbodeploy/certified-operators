package com.vmturbo.market.component.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification;

/**
 * Listener for {@link AnalysisStatusNotification} events.
 */
public interface AnalysisStatusNotificationListener {
    /**
     * Notifies the status of a market analysis run.
     * For Plans, it should correspond to PlanStatus and in real-time to AnalysisStatus.
     *
     * @param analysisStatus The status of the analysis run.
     */
    void onAnalysisStatusNotification(@Nonnull AnalysisStatusNotification analysisStatus);
}
