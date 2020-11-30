package com.vmturbo.action.orchestrator.store.identity;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.identity.IdentityService;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Identity service is an instance to assign (or retrieve from the underlying store) OIDs for the
 * the specified objects.
 *
 * @param <T> type of an objects this identity store is supposed to assign OIDs to.
 * @param <S> type of the object that oid cache use as the key.
 * @param <M> type of a model. Model is a representation of the input object {@code T}
 *         that is immutable and implements {@link #hashCode()} and {@link #equals(Object)} in a way
 *         usable to recognize objects of type {@code T}
 */
public class IdentityServiceImpl<T, S, M> implements IdentityService<T> {

    private final Logger logger = LogManager.getLogger(getClass());
    private final IdentityDataStore<M> identityStore;
    private final Function<T, M> extractor;
    private final Function<M, S> cacheKeyCreator;
    @GuardedBy("lock")
    private final Map<S, Pair<Long, Long>> oidsCache = new HashMap<>();
    private final Object lock = new Object();
    private final Clock clock;
    private final long timeToCacheMillis;

    /**
     * Constructs identity service.
     *
     * @param identityStore identity data store to use
     * @param extractor identity model creator
     * @param cacheKeyCreator function that creates the cache key
     * @param clock clock to use (for cache cleanup)
     * @param timeToCacheMillis time of cache entries to leave after the last hit
     */
    public IdentityServiceImpl(@Nonnull IdentityDataStore<M> identityStore,
            @Nonnull Function<T, M> extractor, @Nonnull Function<M, S> cacheKeyCreator,
            @Nonnull Clock clock, long timeToCacheMillis) {
        this.identityStore = Objects.requireNonNull(identityStore);
        this.extractor = Objects.requireNonNull(extractor);
        this.cacheKeyCreator = Objects.requireNonNull(cacheKeyCreator);
        this.clock = Objects.requireNonNull(clock);
        this.timeToCacheMillis = timeToCacheMillis;
        if (timeToCacheMillis < 0) {
            throw new IllegalArgumentException("timeToCacheMillis must be a positive value");
        }
    }

    @Override
    @Nonnull
    public List<Long> getOidsForObjects(@Nonnull List<T> businessObjects) {
        logger.trace("Calculating OIDs for {} objects", businessObjects::size);
        final List<M> models =
                businessObjects.stream().map(extractor::apply).collect(Collectors.toList());
        logger.trace("Converted objects to following models: {}", models::toString);
        synchronized (lock) {
            final List<M> uncached = models.stream()
                    .filter(model -> !oidsCache.containsKey(cacheKeyCreator.apply(model)))
                    .collect(Collectors.toList());
            logger.debug("In-memory cache missed for {} of {} models", uncached::size,
                    businessObjects::size);
            final Map<M, Long> fromDb = identityStore.fetchOids(uncached);
            logger.debug("Fetched additional {} models' OIDs from DB", fromDb::size);
            final Map<M, Long> newModels = new HashMap<>();
            for (M model : uncached) {
                if (!fromDb.containsKey(model)) {
                    newModels.put(model, IdentityGenerator.next());
                }
            }
            logger.trace("The following new models found: {}", newModels::toString);
            logger.debug("Persisting {} new models to the DB", newModels::size);
            identityStore.persistModels(newModels);
            final long currentTime = clock.millis();
            addToCache(fromDb, currentTime);
            addToCache(newModels, currentTime);
            return models.stream()
                    .map(model -> getFromCache(model, currentTime))
                    .collect(Collectors.toList());
        }
    }

    @GuardedBy("lock")
    private void addToCache(@Nonnull Map<M, Long> models, long currentTime) {
        for (Entry<M, Long> entry : models.entrySet()) {
            oidsCache.put(cacheKeyCreator.apply(entry.getKey()), Pair.create(entry.getValue(),
                currentTime));
        }
    }

    @GuardedBy("lock")
    private long getFromCache(@Nonnull M model, long currentTime) {
        final Pair<Long, Long> result =
            Objects.requireNonNull(oidsCache.get(cacheKeyCreator.apply(model)));
        if (result.getSecond() != currentTime) {
            oidsCache.put(cacheKeyCreator.apply(model), Pair.create(result.getFirst(), currentTime));
        }
        return result.getFirst();
    }

    /**
     * Method prunes obsolete cache entries to free memory. It is using {@link #timeToCacheMillis}
     * value to identity whether entry is obsolete.
     */
    public void pruneObsoleteCache() {
        final long purgeTime = clock.millis() - timeToCacheMillis;
        synchronized (lock) {
            final int cacheSize = oidsCache.size();
            oidsCache.entrySet().removeIf(entry -> entry.getValue().getSecond() < purgeTime);
            final int afterCleanupSize = oidsCache.size();
            logger.debug("Removed {} timed out cache entries: {} -> {}",
                    cacheSize - afterCleanupSize, cacheSize, afterCleanupSize);
        }
    }
}
