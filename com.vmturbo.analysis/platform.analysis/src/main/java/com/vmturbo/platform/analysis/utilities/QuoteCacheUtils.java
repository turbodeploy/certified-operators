package com.vmturbo.platform.analysis.utilities;

import com.google.common.annotations.VisibleForTesting;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.vmturbo.platform.analysis.actions.Move;

/**
 * Operations relating to {@link QuoteCache} that may appear frequently in client code and we may
 * want to factor out but are not considered part of the cache itself.
 */
public class QuoteCacheUtils {
    /**
     * Workaround to simultaneously a) get 100% test coverage b) avoid complains by ReviewBot.
     */
    @VisibleForTesting
    QuoteCacheUtils() {
        // do nothing
    }

    /**
     * Invalidate the cached quote entries associated with the {@link Move#getSource() source} and
     * {@link Move#getDestination() destination} of a {@link Move} action.
     *
     * @param cache The cache object whose entries should be invalidated.
     * @param move The move action that was the reason for the invalidation.
     */
    public static void invalidate(@Nullable QuoteCache cache, @NonNull Move move) {
        if (cache != null) {
            if (move.getSource() != null) {
                cache.invalidate(move.getSource().getEconomyIndex());
            }
            if (move.getDestination() != null) {
                cache.invalidate(move.getDestination().getEconomyIndex());
            }
        }
    }
} // end QuoteCacheUtils
