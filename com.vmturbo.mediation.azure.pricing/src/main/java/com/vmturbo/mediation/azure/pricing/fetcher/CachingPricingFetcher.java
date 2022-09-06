package com.vmturbo.mediation.azure.pricing.fetcher;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.probe.ProxyAwareAccount;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 * Implements the PricingFileFetcher interface by wrapping another PricingFileFetcher
 * and providing caching of downloaded files.
 *
 * @param <A> The account type of the caching and wrapped pricing fetchers.
 */
public class CachingPricingFetcher<A extends ProxyAwareAccount> implements PricingFileFetcher<A> {
    private static final Logger LOGGER = LogManager.getLogger();

    private final ConcurrentMap<Object, CacheEntry> cache = new ConcurrentHashMap<>();
    private final PricingFileFetcher<A> underlyingFetcher;
    private final Duration refreshAfter;
    private final Duration expireAfter;

    private CachingPricingFetcher(@Nonnull PricingFileFetcher<A> fetcherToWrap,
            @Nonnull Duration refreshAfter,
            @Nonnull Duration expireAfter) {
        this.underlyingFetcher = fetcherToWrap;
        this.refreshAfter = refreshAfter;
        this.expireAfter = expireAfter;
    }

    @Override
    @Nonnull
    public Object getCacheKey(@Nonnull A account) {
        return underlyingFetcher.getCacheKey(account);
    }

    @Override
    @Nullable
    public Pair<Path, String> fetchPricing(@Nonnull A account,
            @Nonnull IPropertyProvider propertyProvider) throws Exception {
        Object key = underlyingFetcher.getCacheKey(account);

        try (CacheEntry entry = cache.computeIfAbsent(key, k -> new CacheEntry()).locked()) {
            StringBuilder sb = new StringBuilder();
            Instant now = Instant.now();

            if (entry.cachedFile != null && entry.lastUpdate != null) {
                Instant expiredAt = entry.lastUpdate.plus(expireAfter);
                if (now.isAfter(expiredAt)) {
                    LOGGER.info("Removing cached file {} for {}, expired at {}",
                            entry.cachedFile, key, expiredAt);
                    try {
                        Files.delete(entry.cachedFile);
                    } catch (IOException ex) {
                        LOGGER.warn("Error deleting expired cached file {} for {}",
                                entry.cachedFile, key, ex);
                    }
                    entry.cachedFile = null;

                    sb.append("Cached data expired at ");
                    sb.append(expiredAt);
                    sb.append("\n");
                } else if (now.isAfter(entry.lastUpdate.plus(refreshAfter))) {
                    try {
                        final Pair<Path, String> result = underlyingFetcher.fetchPricing(account, propertyProvider);

                        LOGGER.info("Removing replaced cached file {} for {}",
                                entry.cachedFile, key);
                        try {
                            Files.delete(entry.cachedFile);
                        } catch (IOException ex) {
                            LOGGER.warn("Error deleting replaced cached file {} for {}",
                                    entry.cachedFile, key, ex);
                        }

                        entry.lastUpdate = now;
                        entry.cachedFile = result.getFirst();

                        return result;
                    } catch (Exception ex) {
                        LOGGER.warn(
                                "Failed to refresh data for {}, falling back to cached data from {}",
                                key, entry.lastUpdate, ex);

                        if (Files.exists(entry.cachedFile)) {
                            return new Pair<>(entry.cachedFile,
                                    "Failed to refresh data, falling back to cached data from " + entry.lastUpdate);
                        } else {
                            entry.cachedFile = null;
                            throw new FileNotFoundException("Failed to refresh, and existing cached file "
                                + entry.cachedFile + " is missing");
                        }
                    }
                } else {
                    if (Files.exists(entry.cachedFile)) {
                        return new Pair<>(entry.cachedFile, "Using cached data from " + expiredAt);
                    } else {
                        entry.cachedFile = null;
                        throw new FileNotFoundException("Cached file " + entry.cachedFile + " is missing");
                    }
                }
            }

            Pair<Path, String> result = underlyingFetcher.fetchPricing(account, propertyProvider);
            entry.lastUpdate = Instant.now();
            entry.cachedFile = result.getFirst();
            sb.append(result.getSecond());

            return new Pair<>(entry.cachedFile, sb.toString());
        }
    }

    /**
     * Get a builder for constructing an instance of this class.
     *
     * @param <A> type account type for the CachingPricingFetcher to build
     * @return the builder object
     */
    @Nonnull
    public static <A extends ProxyAwareAccount> Builder<A> newBuilder() {
        return new Builder<A>();
    }

    /**
     * Builder class for CachingPricingFetcher.
     *
     * @param <A> The type of account for which to build a pricing fetcher.
     */
    public static class Builder<A extends ProxyAwareAccount> {
        Duration refreshAfterDuration = Duration.ofDays(1);
        Duration expireAfterDuration = Duration.ofDays(7);

        /**
         * Set the refresh time for the cache. Fetches before the refresh time use
         * cached data. Between the refresh time and the expiry time, the cache
         * will attempt to get a new pricing file using the wrapped fetcher, but
         * if it fails a warning is logged and the cached file will be returned.
         *
         * @param duration how long must pass between the last time pricing was
         * fetched from the underlying loader and when the cache will start trying
         * to refresh the cache.
         * @return the builder, for chained calls.
         */
        @Nonnull
        public Builder<A> refreshAfter(@Nonnull Duration duration) {
            refreshAfterDuration = duration;
            return this;
        }

        /**
         * Set the expiry time for the cache. Fetches after this time will
         * call the wrapped fetcher to update the cache, and if it fails,
         * the fetch will fail. Set this to a time duration after which it's
         * too risky to continue using stale data.
         *
         * @param duration the time duration after which cached files expire and
         * will not be used.
         * @return the builder, for chained calls.
         */
        @Nonnull
        public Builder<A> expireAfter(@Nonnull Duration duration) {
            expireAfterDuration = duration;
            return this;
        }

        /**
         * Build the CachingPricingFetcher.
         *
         * @param fetcherToWrap the underlying fetcher to wrap with caching logic.
         * @return the new CachingPricingFetcher.
         */
        @Nonnull
        public CachingPricingFetcher<A> build(@Nonnull PricingFileFetcher<A> fetcherToWrap) {
            return new CachingPricingFetcher<A>(fetcherToWrap, refreshAfterDuration, expireAfterDuration);
        }
    }

    /**
     * An entry within the cache.
     */
    private static class CacheEntry implements AutoCloseable {
        private final Lock lock = new ReentrantLock();
        private Instant lastUpdate = null;
        private Path cachedFile = null;

        CacheEntry() {}

        public CacheEntry locked() {
            lock.lock();
            return this;
        }

        @Override
        public void close() throws Exception {
            lock.unlock();
        }
    }
}
