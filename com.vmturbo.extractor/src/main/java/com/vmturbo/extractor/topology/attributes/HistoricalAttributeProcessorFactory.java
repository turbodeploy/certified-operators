package com.vmturbo.extractor.topology.attributes;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Factory class for {@link HistoricalAttributeProcessor}s.
 *
 * <p/>Factories contain state that's persisted across broadcasts - namely the hashes we use
 * to detect whether an attribute value has changed between topologies.
 *
 * @param <V> The value type.
 */
abstract class HistoricalAttributeProcessorFactory<V> {
    protected final Logger logger = LogManager.getLogger(getClass());

    private final Long2LongMap hashByEntityIdMap = new Long2LongOpenHashMap();

    private final Clock clock;

    private final long forceUpdateIntervalMs;

    /**
     * We always initialize to 0. This means that after restart we will force updates
     * immediately.
     */
    private final AtomicLong lastUpdateMs = new AtomicLong(0);

    HistoricalAttributeProcessorFactory(@Nonnull Clock clock, long forceUpdateInterval,
            TimeUnit forceUpdateIntervalUnits) {
        this.clock = clock;
        this.forceUpdateIntervalMs = forceUpdateIntervalUnits.toMillis(forceUpdateInterval);
    }

    @Nullable
    HistoricalAttributeProcessor<V> newProcessor(DbEndpoint dbEndpoint) {
        // If time to force updates, clear the hash by entity map.
        final long now = clock.millis();
        final boolean forceUpdate = now - lastUpdateMs.longValue() >= forceUpdateIntervalMs;
        if (forceUpdate) {
            this.hashByEntityIdMap.clear();
        }
        return newProcessorInternal(dbEndpoint, hashByEntityIdMap, newHashes -> {
            hashByEntityIdMap.putAll(newHashes);
            if (forceUpdate) {
                // Now that we have successfully written the attributes, update the "last update"
                // time.
                this.lastUpdateMs.set(now);
            }
        });
    }

    @Nullable
    abstract HistoricalAttributeProcessor<V> newProcessorInternal(DbEndpoint dbEndpoint,
            Long2LongMap hashByEntityIdMap,
            Consumer<Long2LongMap> onSuccess);
}
