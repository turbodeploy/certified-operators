package com.vmturbo.cost.component.scope;

import java.util.LongSummaryStatistics;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;

import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.vmturbo.cloud.common.data.stats.LongStatistics;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.cloud.common.persistence.DataQueueStats;

/**
 * Persistence statistics for all the  scope id replacement data segments.
 */
@HiddenImmutableImplementation
@Value.Immutable
public interface ScopeIdReplacementPersistenceSummary {

    /**
     * Returns the number of alias oids with valid scope data.
     *
     * @return the number of alias oids with valid scope data.
     */
    LongStatistics numAliasOidWithValidScopes();

    /**
     * Returns the number of real oids persisted to cloud_scope table.
     *
     * @return the number of real oids persisted to cloud_scope table.
     */
    LongStatistics numRealOidScopesPersisted();

    /**
     * Returns the number of update queries created for scope id replacement.
     *
     * @return the number of update queries created for scope id replacement.
     */
    LongStatistics numUpdateQueriesCreated();

    /**
     * Returns the number of scope id replacements logged.
     *
     * @return the number of scope id replacements logged.
     */
    LongStatistics numScopeIdReplacementsLogged();

    /**
     * Collector for {@link ScopeIdReplacementPersistenceSummary} to aggregate stats across data segments.
     */
    class Collector implements DataQueueStats.DataSpecificStatsCollector<ScopeIdReplacementPersistenceStats,
        ScopeIdReplacementPersistenceSummary> {

        private final ReadWriteLock collectionLock = new ReentrantReadWriteLock();
        private final LongSummaryStatistics numAliasOidWithValidScopes = new LongSummaryStatistics();
        private final LongSummaryStatistics numRealOidScopesPersisted = new LongSummaryStatistics();
        private final LongSummaryStatistics numUpdateQueriesCreated = new LongSummaryStatistics();
        private final LongSummaryStatistics numScopeIdReplacementsLogged = new LongSummaryStatistics();

        @Override
        public void collect(@NotNull ScopeIdReplacementPersistenceStats scopeIdReplacementPersistenceStats) {
            try {
                collectionLock.writeLock().lock();
                numAliasOidWithValidScopes.accept(scopeIdReplacementPersistenceStats.numAliasOidWithValidScopes());
                numRealOidScopesPersisted.accept(scopeIdReplacementPersistenceStats.numRealOidScopesPersisted());
                numUpdateQueriesCreated.accept(scopeIdReplacementPersistenceStats.numUpdateQueriesCreated());
                numScopeIdReplacementsLogged.accept(scopeIdReplacementPersistenceStats.numScopeIdReplacementsLogged());
            } finally {
                collectionLock.writeLock().unlock();
            }
        }

        @Nullable
        @Override
        public ScopeIdReplacementPersistenceSummary toSummary() {
           try {
               collectionLock.readLock().lock();
               return ImmutableScopeIdReplacementPersistenceSummary.builder()
                   .numAliasOidWithValidScopes(LongStatistics.fromLongSummary(numAliasOidWithValidScopes))
                   .numRealOidScopesPersisted(LongStatistics.fromLongSummary(numRealOidScopesPersisted))
                   .numUpdateQueriesCreated(LongStatistics.fromLongSummary(numUpdateQueriesCreated))
                   .numScopeIdReplacementsLogged(LongStatistics.fromLongSummary(numScopeIdReplacementsLogged))
                   .build();
           } finally {
               collectionLock.readLock().unlock();
           }
        }

        /**
         * Creates a new collector.
         * @return The newly created collector.
         */
        @Nonnull
        public static Collector create() {
            return new Collector();
        }
    }
}