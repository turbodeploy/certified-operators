package com.vmturbo.cost.component.scope;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.impl.TableImpl;

import com.vmturbo.cloud.common.scope.CloudScopeIdentity;
import com.vmturbo.cloud.common.scope.CloudScopeIdentity.CloudScopeType;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityStore;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.cost.component.TableDiagsRestorable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.CloudScopeRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.jooq.filter.JooqFilterMapper;

/**
 * SQL implementation of {@link CloudScopeIdentityStore}.
 */
public class SqlCloudScopeIdentityStore implements CloudScopeIdentityStore {

    private final Logger logger = LogManager.getLogger();

    private final JooqFilterMapper<CloudScopeIdentityFilter> filterMapper = JooqFilterMapper.<CloudScopeIdentityFilter>builder()
            .addInCollection(CloudScopeIdentityFilter::scopeIds, Tables.CLOUD_SCOPE.SCOPE_ID)
            .addEnumInShortCollection(CloudScopeIdentityFilter::scopeTypes, Tables.CLOUD_SCOPE.SCOPE_TYPE)
            .addInCollection(CloudScopeIdentityFilter::accountIds, Tables.CLOUD_SCOPE.ACCOUNT_ID)
            .addInCollection(CloudScopeIdentityFilter::regionIds, Tables.CLOUD_SCOPE.REGION_ID)
            .addInCollection(CloudScopeIdentityFilter::cloudServiceIds, Tables.CLOUD_SCOPE.CLOUD_SERVICE_ID)
            .addInCollection(CloudScopeIdentityFilter::resourceGroupIds, Tables.CLOUD_SCOPE.RESOURCE_GROUP_ID)
            .addInCollection(CloudScopeIdentityFilter::serviceProviderIds, Tables.CLOUD_SCOPE.SERVICE_PROVIDER_ID)
            .build();

    private final Set<Long> scopePersistenceCache = Sets.newConcurrentHashSet();

    private final boolean persistenceCacheEnabled;

    private final DSLContext dslContext;

    private final PersistenceRetryPolicy persistenceRetryPolicy;

    private final int batchStoreSize;

    private LocalDate scopeCacheDate = LocalDate.now();

    private final CloudScopeDiagsHelper cloudScopeDiagsHelper;

    private boolean exportCloudCostDiags = true;

    /**
     * Constructs a new {@link SqlCloudScopeIdentityStore} instance.
     * @param dslContext The DSL context.
     * @param persistenceRetryPolicy The persistence retry policy.
     * @param persistenceCacheEnabled Whether the persistence cache is enabled.
     * @param batchStoreSize The batch size for persisting scope identities.
     */
    public SqlCloudScopeIdentityStore(@Nonnull DSLContext dslContext,
                                      @Nonnull PersistenceRetryPolicy persistenceRetryPolicy,
                                      boolean persistenceCacheEnabled,
                                      int batchStoreSize) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.persistenceRetryPolicy = Objects.requireNonNull(persistenceRetryPolicy);
        this.persistenceCacheEnabled = persistenceCacheEnabled;
        this.batchStoreSize = batchStoreSize;
        this.cloudScopeDiagsHelper = new CloudScopeDiagsHelper(dslContext);
    }


    @Override
    public void saveScopeIdentities(@Nonnull List<CloudScopeIdentity> scopeIdentityList) {

        final RetryPolicy<Void> retryPolicy = RetryPolicy.<Void>builder()
                .withMaxRetries(persistenceRetryPolicy.maxRetries())
                .withDelay(persistenceRetryPolicy.minRetryDelay(),
                        persistenceRetryPolicy.maxRetryDelay())
                .onFailedAttempt((attemptedEvent) ->
                        logger.warn("Scope identity persistence attempt failed (attempt count = {})",
                                attemptedEvent.getAttemptCount(), attemptedEvent.getLastException()))
                .build();

        Failsafe.with(retryPolicy).run(() -> persistScopeIdentities(scopeIdentityList));
    }

    @Override
    public List<CloudScopeIdentity> getIdentitiesByFilter(@Nonnull CloudScopeIdentityFilter scopeIdentityFilter) {

        Preconditions.checkNotNull(scopeIdentityFilter, "Filter cannot be null");

        try (Stream<CloudScopeRecord> recordStream = dslContext.selectFrom(Tables.CLOUD_SCOPE)
                .where(filterMapper.generateConditions(scopeIdentityFilter))
                .stream()) {

            return recordStream.map(this::createIdentityFromRecord)
                    .collect(ImmutableList.toImmutableList());
        }
    }

    private void persistScopeIdentities(@Nonnull List<CloudScopeIdentity> scopeIdentityList) {

        final Stopwatch stopwatch = Stopwatch.createStarted();

        final List<CloudScopeIdentity> scopeIdentityPersistenceList;
        if (persistenceCacheEnabled) {
            checkCacheExpiration();

            scopeIdentityPersistenceList = scopeIdentityList.stream()
                    .filter(scopeIdentity -> !scopePersistenceCache.contains(scopeIdentity.scopeId()))
                    .collect(ImmutableList.toImmutableList());
        } else {
            scopeIdentityPersistenceList = scopeIdentityList;
        }

        Iterables.partition(scopeIdentityPersistenceList, batchStoreSize).forEach(scopeIdentitiesBatch -> {

            final List<Query> recordInsertList = scopeIdentitiesBatch.stream()
                    .map(this::createRecordFromIdentity)
                    .map(cloudScopeRecord -> dslContext.insertInto(Tables.CLOUD_SCOPE)
                            .set(cloudScopeRecord)
                            .onDuplicateKeyUpdate()
                            .setNull(Tables.CLOUD_SCOPE.UPDATE_TS)
                            // Update all other records
                            .set(cloudScopeRecord))
                    .collect(ImmutableList.toImmutableList());

            dslContext.batch(recordInsertList).execute();

            if (persistenceCacheEnabled) {
                scopeIdentitiesBatch.forEach(scopeIdentity -> scopePersistenceCache.add(scopeIdentity.scopeId()));
            }
        });

        logger.info("Persisted {} cloud scope records in {}", scopeIdentityPersistenceList.size(), stopwatch);
    }

    /**
     * Clears the persistence cache, which is utilized to avoid unnecessary DB upsert operations for recently
     * persisted scope identities.
     */
    public void cleanPersistenceCache() {
        synchronized (scopeCacheDate) {
            logger.info("Clearing scope persistence cache for {}. Cache size is {}", scopeCacheDate, scopePersistenceCache.size());

            scopePersistenceCache.clear();
            scopeCacheDate = LocalDate.now();
        }
    }

    private void checkCacheExpiration() {

        synchronized (scopeCacheDate) {
            if (scopeCacheDate.isBefore(LocalDate.now())) {
                cleanPersistenceCache();
            }
        }
    }

    private CloudScopeRecord createRecordFromIdentity(@Nonnull CloudScopeIdentity scopeIdentity) {

        final CloudScopeRecord record = new CloudScopeRecord();
        record.setScopeId(scopeIdentity.scopeId());
        record.setScopeType((short)scopeIdentity.scopeType().ordinal());
        record.setResourceId(scopeIdentity.resourceId());
        record.setResourceType(scopeIdentity.hasResourceInfo() ? (short)scopeIdentity.resourceType().ordinal() : null);
        record.setAccountId(scopeIdentity.accountId());
        record.setRegionId(scopeIdentity.regionId());
        record.setCloudServiceId(scopeIdentity.cloudServiceId());
        record.setAvailabilityZoneId(scopeIdentity.zoneId());
        record.setResourceGroupId(scopeIdentity.resourceGroupId());
        record.setServiceProviderId(scopeIdentity.serviceProviderId());

        return record;
    }

    private CloudScopeIdentity createIdentityFromRecord(@Nonnull CloudScopeRecord scopeRecord) {
        return CloudScopeIdentity.builder()
                .scopeId(scopeRecord.getScopeId())
                .scopeType(CloudScopeType.values()[scopeRecord.getScopeType()])
                .resourceId(scopeRecord.getResourceId())
                .resourceType(scopeRecord.getResourceType() != null
                        ? EntityType.forNumber(scopeRecord.getResourceType())
                        : null)
                .accountId(scopeRecord.getAccountId())
                .regionId(scopeRecord.getRegionId())
                .cloudServiceId(scopeRecord.getCloudServiceId())
                .zoneId(scopeRecord.getAvailabilityZoneId())
                .resourceGroupId(scopeRecord.getResourceGroupId())
                .serviceProviderId(scopeRecord.getServiceProviderId())
                .build();
    }

    @Override
    public Set<Diagnosable> getDiagnosables(boolean collectHistoricalStats) {
        HashSet<Diagnosable> storesToSave = new HashSet<>();
        storesToSave.add(cloudScopeDiagsHelper);
        return storesToSave;
    }

    @Override
    public void setExportCloudCostDiags(boolean exportCloudCostDiags) {
        this.exportCloudCostDiags = exportCloudCostDiags;
    }

    @Override
    public boolean getExportCloudCostDiags() {
        return this.exportCloudCostDiags;
    }

    /**
     * Helper class for dumping cloud scope db records to exported topology.
     */
    private final class CloudScopeDiagsHelper implements
            TableDiagsRestorable<Object, CloudScopeRecord> {
        private static final String cloudScopeDumpFile = "cloudScope_dump";

        private final DSLContext dsl;

        CloudScopeDiagsHelper(@Nonnull final DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext getDSLContext() {
            return dsl;
        }

        @Override
        public TableImpl<CloudScopeRecord> getTable() {
            return Tables.CLOUD_SCOPE;
        }

        @Nonnull
        @Override
        public String getFileName() {
            return cloudScopeDumpFile;
        }

        @Nonnull
        @Override
        public void collectDiags(@Nonnull final DiagnosticsAppender appender) {
            if (exportCloudCostDiags) {
                TableDiagsRestorable.super.collectDiags(appender);
            } else {
                return;
            }
        }
    }
}
