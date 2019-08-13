package com.vmturbo.topology.processor.history.timeslot;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.topology.processor.history.EntityCommodityFieldReference;
import com.vmturbo.topology.processor.history.EntityCommodityReferenceWithBuilder;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.IHistoryLoadingTask;

/**
 * Loader of time slot data from historydb.
 * TODO dmitry provide dbvalue class
 */
public class TimeSlotLoadingTask implements IHistoryLoadingTask<Void> {
    private final StatsHistoryServiceBlockingStub statsHistoryClient;

    public TimeSlotLoadingTask(StatsHistoryServiceBlockingStub statsHistoryClient) {
        this.statsHistoryClient = statsHistoryClient;
    }

    @Override
    public Map<EntityCommodityFieldReference, Void>
        load(Collection<EntityCommodityReferenceWithBuilder> commodities)
                    throws HistoryCalculationException {
        // TODO dmitry make a request to historydb for given commodities
        return Collections.emptyMap();
    }

}
