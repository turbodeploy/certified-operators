package com.vmturbo.extractor.topology.fetcher;

import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.topology.fetcher.TopDownCostFetcherFactory.TopDownCostData;

/**
 * The per-topology fetcher for top-down cost data.
 */
public class TopDownCostFetcher extends DataFetcher<TopDownCostData> {

    private final TopDownCostFetcherFactory fetcherFactory;

    /**
     * Constructor.
     *
     *  @param timer a {@link MultiStageTimer} to collect timing information for this fetcher.
     * @param consumer the consumer which will consume the response of this fetcher
     * @param fetcherFactory Which is used to access the top down cost data, cached across broadcasts.
     */
    public TopDownCostFetcher(@Nonnull MultiStageTimer timer,
            @Nonnull Consumer<TopDownCostData> consumer,
            @Nonnull final TopDownCostFetcherFactory fetcherFactory) {
        super(timer, consumer);
        this.fetcherFactory = fetcherFactory;
    }

    @Override
    protected TopDownCostData fetch() {
        return fetcherFactory.getMostRecentData();
    }
}
