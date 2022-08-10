package com.vmturbo.cost.component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.annotation.Nonnull;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.TopologyOnDemandCostChunk;
import com.vmturbo.cost.api.TopologyCostListener;

/**
 * Listener to test TopologyCostListener.
 */
public class TestTopologyCostListener implements TopologyCostListener {

    private final CountDownLatch latch;
    private final List<TopologyOnDemandCostChunk> chunks = new ArrayList<>();

    /**
     * Construct test listener.
     *
     * @param latch latch to count down messages received
     */
    public TestTopologyCostListener(@Nonnull final CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void onTopologyCostReceived(@NotNull TopologyOnDemandCostChunk costChunk) {
        chunks.add(costChunk);
        latch.countDown();
    }

    public List<TopologyOnDemandCostChunk> getChunks() {
        return chunks;
    }
}
