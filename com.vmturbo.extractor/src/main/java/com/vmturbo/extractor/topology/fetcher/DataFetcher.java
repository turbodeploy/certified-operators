package com.vmturbo.extractor.topology.fetcher;

import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.utils.MultiStageTimer;

/**
 * Abstract class to fetch different aspects of entity/group data from other components.
 *
 * @param <R> type of the data to fetch.
 */
public abstract class DataFetcher<R> {

    /**
     * Consumer which will consume the fetched data.
     */
    protected Consumer<R> consumer;

    /**
     * A {@link MultiStageTimer} to collect timing information for this fetcher.
     */
    protected MultiStageTimer timer;

    /**
     * Constructor.
     *
     * @param timer a {@link MultiStageTimer} to collect timing information for this fetcher.
     * @param consumer the consumer which will consume the response of this fetcher
     */
    protected DataFetcher(@Nonnull MultiStageTimer timer,
                          @Nonnull Consumer<R> consumer) {
        this.timer = timer;
        this.consumer = consumer;
    }

    /**
     * Name of this fetcher. Use class name by default.
     *
     * @return name of the fetcher
     */
    protected String getName() {
        return getClass().getName();
    }

    /**
     * Fetch data and then consume it.
     */
    public void fetchAndConsume() {
        timer.start(getName());
        R data = fetch();
        consumer.accept(data);
        timer.stop();
    }

    /**
     * Fetch data from other components.
     *
     * @return {@link R}
     */
    protected abstract R fetch();
}