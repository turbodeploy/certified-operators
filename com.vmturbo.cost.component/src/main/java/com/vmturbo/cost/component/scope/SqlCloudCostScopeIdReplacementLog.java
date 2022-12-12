package com.vmturbo.cost.component.scope;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.InsertReturningStep;
import org.jooq.Record3;
import org.jooq.UpdateConditionStep;
import org.jooq.UpdateSetMoreStep;
import org.jooq.impl.DSL;

import com.vmturbo.cloud.common.persistence.DataBatcher;
import com.vmturbo.cloud.common.persistence.DataQueue;
import com.vmturbo.cloud.common.persistence.DataQueueConfiguration;
import com.vmturbo.cloud.common.persistence.DataQueueFactory;
import com.vmturbo.cloud.common.scope.CloudScopeIdentity;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityStore;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.CloudCostDailyRecord;
import com.vmturbo.cost.component.db.tables.records.ScopeIdReplacementLogRecord;

/**
 * Persist and maintain log of scope id replacements in the Cloud Cost data.
 */
public class SqlCloudCostScopeIdReplacementLog implements ScopeIdReplacementLog, RequiresDataInitialization {

    private static final Logger logger = LogManager.getLogger();
    private static final short CLOUD_COST_REPLACEMENT_LOG_ID = 1;

    private final DSLContext dslContext;
    private final SqlOidMappingStore oidMappingStore;
    private final CloudScopeIdentityStore cloudScopeIdentityStore;
    private final DataQueueConfiguration dataQueueConfiguration;
    private final Map<Long, Set<OidMapping>> replacedScopeIdsToOidMappings = new ConcurrentHashMap<>();
    private final int batchSize;
    private final Duration persistenceTimeout;

    /**
     * Creates an instance of {@link SqlCloudCostScopeIdReplacementLog}.
     *
     * @param dslContext instance for executing sql queries.
     * @param dataQueueConfiguration config for {@link DataQueue}.
     */
    public SqlCloudCostScopeIdReplacementLog(@Nonnull final DSLContext dslContext,
                                             @Nonnull final SqlOidMappingStore oidMappingStore,
                                             @Nonnull final CloudScopeIdentityStore cloudScopeIdentityStore,
                                             @Nonnull final DataQueueConfiguration dataQueueConfiguration,
                                             final int batchSize,
                                             @Nonnull final Duration persistenceTimeout) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.oidMappingStore = Objects.requireNonNull(oidMappingStore);
        this.cloudScopeIdentityStore = Objects.requireNonNull(cloudScopeIdentityStore);
        this.dataQueueConfiguration = Objects.requireNonNull(dataQueueConfiguration);
        this.batchSize = batchSize;
        this.persistenceTimeout = Objects.requireNonNull(persistenceTimeout);
        logger.info("Created SqlCloudCostScopeIdReplacementLog instance with batchSize: {}, persistenceTimeout: {}, "
        + " concurrency: {}", batchSize, persistenceTimeout, dataQueueConfiguration.concurrency());
    }

    @Override
    public void initialize() throws InitializationException {
        try {
            replacedScopeIdsToOidMappings.clear();
            replacedScopeIdsToOidMappings.putAll(
                dslContext.select(Tables.OID_MAPPING.REAL_ID, Tables.OID_MAPPING.ALIAS_ID,
                        Tables.OID_MAPPING.FIRST_DISCOVERED_TIME_MS_UTC)
                    .from(Tables.SCOPE_ID_REPLACEMENT_LOG.join(Tables.OID_MAPPING)
                        .on(Tables.SCOPE_ID_REPLACEMENT_LOG.REAL_ID.eq(Tables.OID_MAPPING.REAL_ID)))
                    .where(Tables.SCOPE_ID_REPLACEMENT_LOG.REPLACEMENT_LOG_ID.eq(getLogId()))
                    .fetch()
                    .stream()
                    .map(this::recordToOidMapping)
                    .collect(Collectors.groupingBy(mapping -> mapping.oidMappingKey().aliasOid(), Collectors.toSet())));
        } catch (Exception ex) {
            logger.error("Cache initialization failed for SqlCloudCostScopeIdReplacementLog.", ex);
        }
        logger.info("Initialized SqlCloudCostScopeIdReplacementLog cache, size: {}",
            replacedScopeIdsToOidMappings.size());
    }

    @Override
    public long getReplacedScopeId(final long scopeId, @Nonnull Instant sampleTimeUtcMs) {

        final Set<OidMapping> replacementsForScopeId = replacedScopeIdsToOidMappings.get(scopeId);

        if (replacementsForScopeId != null && !replacementsForScopeId.isEmpty()) {

            final PriorityQueue<OidMapping> maxPriorityQueue = new PriorityQueue<>(replacementsForScopeId.size(),
                Comparator.comparing(OidMapping::firstDiscoveredTimeMsUtc).reversed());
            maxPriorityQueue.addAll(replacementsForScopeId);

            // starting with the most recent replaced oid mapping, find the mapping which is just before the provided
            // sampleTimeUtcMs
            while (!maxPriorityQueue.isEmpty()) {
                final OidMapping oidMapping = maxPriorityQueue.poll();
                final Instant mappingFirstDiscoveredTime = oidMapping.firstDiscoveredTimeMsUtc()
                    .truncatedTo(ChronoUnit.DAYS);
                if (sampleTimeUtcMs.isAfter(mappingFirstDiscoveredTime)
                    || sampleTimeUtcMs.equals(mappingFirstDiscoveredTime)) {
                    final long replacedOid = oidMapping.oidMappingKey().realOid();
                    logger.trace("Replaced scopeId {} with {} for sample time: {}", scopeId, replacedOid,
                        sampleTimeUtcMs);
                    return replacedOid;
                }
            }
        }
        return scopeId;
    }

    @Override
    public short getLogId() {
        return CLOUD_COST_REPLACEMENT_LOG_ID;
    }

    /**
     * Persist scope id replacements for cloud cost data.
     *
     * @return Optional of {@link ScopeIdReplacementPersistenceSummary} containing persistence stats.
     * @throws Exception if error encountered during persistence.
     */
    @Nonnull
    public Optional<ScopeIdReplacementPersistenceSummary> persistScopeIdReplacements() throws Exception {
        final Collection<OidMapping> newOidMappings = oidMappingStore.getNewOidMappings(replacedOidMappings());
        logger.info("Starting scope id replacement for {} new mappings", newOidMappings.size());
        if (newOidMappings.isEmpty()) {
            return Optional.empty();
        }

        final DataQueue<OidMappingsBatch, ScopeIdReplacementPersistenceSummary> dataQueue
            = createDataQueue();

        Lists.partition(new ArrayList<>(newOidMappings), batchSize)
            .forEach(subList -> {
                final OidMappingsBatch oidMappingsBatch = new OidMappingsBatch(subList.stream()
                    .collect(Collectors.groupingBy(mapping -> mapping.oidMappingKey().aliasOid(), Collectors.toSet())));
                dataQueue.addData(oidMappingsBatch);
            });

        try {
            return Optional.ofNullable(dataQueue.drainAndClose(persistenceTimeout).dataStats());
        } catch (Exception e) {
            logger.error("Exception while persisting scope id replacements.", e);
            throw e;
        }
    }

    private ScopeIdReplacementPersistenceStats persistScopeIdReplacements(
        final List<OidMappingsBatch> dataBatch) {

        final ImmutableScopeIdReplacementPersistenceStats.Builder statsBuilder =
            ImmutableScopeIdReplacementPersistenceStats.builder();

        for (final OidMappingsBatch dataSegment : dataBatch) {
            // retrieve CloudScopeIdentity instances for all alias oids (expected to be present as they are
            // persisted during billed cost upload)
            final Map<Long, CloudScopeIdentity> aliasScopes = retrieveCloudScopeIdentities(dataSegment.getAliasOids());
            statsBuilder.numAliasOidWithValidScopes(aliasScopes.size());
            logger.info("Retrieved {} alias id scopes.", aliasScopes.size());
            // use the alias CloudScopeIdentity instances to create and persist CloudScopes for real oids
            final Set<OidMapping> mappingsWithPersistedRealOids =
                persistRealOidScope(dataSegment.getOidMappingsForAliasOids(aliasScopes.keySet()), aliasScopes);
            statsBuilder.numRealOidScopesPersisted(mappingsWithPersistedRealOids.size());
            logger.info("Persisted {} real id scopes.", mappingsWithPersistedRealOids.size());
            // Create update queries for the new oid mappings via UpdateQueryContextGenerator
            final UpdateQueryContextGenerator generator = new UpdateQueryContextGenerator(
                replacedScopeIdsToOidMappings, mappingsWithPersistedRealOids);

            final List<UpdateConditionStep<CloudCostDailyRecord>> queries = generator.getUpdateQueryContexts().stream()
                .map(this::toUpdateQuery)
                .collect(Collectors.toList());
            statsBuilder.numUpdateQueriesCreated(queries.size());
            logger.info("Created the following update queries: {}", queries);
            // execute update queries and register the replaced oid mappings to replacement log as a single transaction
            dslContext.transaction(config -> {
                final DSLContext context = DSL.using(config);
                context.batch(queries).execute();
                registerReplacedOidMappings(mappingsWithPersistedRealOids, context);
            });

            statsBuilder.numScopeIdReplacementsLogged(mappingsWithPersistedRealOids.size());
        }

        return statsBuilder.build();
    }

    private Collection<OidMappingKey> replacedOidMappings() {
        return replacedScopeIdsToOidMappings.values().stream()
            .flatMap(Collection::stream)
            .map(OidMapping::oidMappingKey)
            .collect(Collectors.toSet());
    }

    private DataQueue<OidMappingsBatch, ScopeIdReplacementPersistenceSummary> createDataQueue() {
        return new DataQueueFactory.DefaultDataQueueFactory().createQueue(
            new DataBatcher.DirectDataBatcher<>(), this::persistScopeIdReplacements,
            ScopeIdReplacementPersistenceSummary.Collector.create(), dataQueueConfiguration);
    }

    private UpdateConditionStep<CloudCostDailyRecord> toUpdateQuery(
        final UpdateQueryContextGenerator.UpdateQueryContext updateQueryContext) {

        final OidMapping currentOidMapping = updateQueryContext.getCurrentOidMapping();
        // Use the realOid of the currentOidMapping to set the scope_id column for the update query
        final UpdateSetMoreStep<CloudCostDailyRecord> updateSetMoreStep = dslContext.update(Tables.CLOUD_COST_DAILY)
            .set(Tables.CLOUD_COST_DAILY.SCOPE_ID, currentOidMapping.oidMappingKey().realOid());

        // where condition on scope_id column: if prior replacement exists, use the realOid of the prior replacement
        // for the where condition, else use the aliasOid of the current mapping
        final long scopeIdToReplace = updateQueryContext.getLastReplacedOidMappingForAlias()
            .map(mapping -> mapping.oidMappingKey().realOid()).orElse(currentOidMapping.oidMappingKey().aliasOid());
        final UpdateConditionStep<CloudCostDailyRecord> updateConditionStep = updateSetMoreStep
            .where(Tables.CLOUD_COST_DAILY.SCOPE_ID.eq(scopeIdToReplace));

        // where condition on sample_ms_utc (start window inclusive): only records starting at the first discovered time
        // of the current mapping have to be updated
        updateConditionStep.and(Tables.CLOUD_COST_DAILY.SAMPLE_MS_UTC
            .greaterOrEqual(currentOidMapping.firstDiscoveredTimeMsUtc().truncatedTo(ChronoUnit.DAYS)));

        // where condition for sample_ms_utc (end window exclusive): if there is a next mapping for which replacement
        // will be done with the same alias oid, use the first discovered time of that mapping as the end window
        // exclusive, since records on and after that time will be updated in as a separate update query
        final Optional<OidMapping> nextOidMappingForAlias = updateQueryContext.getNextOidMappingForAlias();
        nextOidMappingForAlias.ifPresent(oidMapping -> updateConditionStep.and(Tables.CLOUD_COST_DAILY.SAMPLE_MS_UTC
            .lessThan(oidMapping.firstDiscoveredTimeMsUtc().truncatedTo(ChronoUnit.DAYS))));

       return  updateConditionStep;
    }

    private Map<Long, CloudScopeIdentity> retrieveCloudScopeIdentities(final Collection<Long> scopeIds) {
        return cloudScopeIdentityStore.getIdentitiesByFilter(CloudScopeIdentityStore.CloudScopeIdentityFilter.builder()
            .addAllScopeIds(scopeIds).build()).stream()
            .collect(Collectors.toMap(CloudScopeIdentity::scopeId, Function.identity()));
    }

    private Set<OidMapping> persistRealOidScope(final Collection<OidMapping> mappings,
                                     final Map<Long, CloudScopeIdentity> aliasScopes) {
        final Set<OidMapping> oidMappingsWithRealOidPersisted = new HashSet<>();
        final List<CloudScopeIdentity> scopesToPersist = mappings.stream()
            .map(mapping -> {
                final OidMappingKey key = mapping.oidMappingKey();
                final CloudScopeIdentity aliasScope = aliasScopes.get(key.aliasOid());
                if (aliasScope != null) {
                    oidMappingsWithRealOidPersisted.add(mapping);
                    return CloudScopeIdentity.builder().populateFromCloudScope(aliasScope.toCloudScope())
                        .scopeId(key.realOid())
                        .resourceId(key.realOid())
                        .scopeType(CloudScopeIdentity.CloudScopeType.RESOURCE)
                        .build();
                }
                return null;
            }).filter(Objects::nonNull)
            .collect(Collectors.toList());
        cloudScopeIdentityStore.saveScopeIdentities(scopesToPersist);
        return oidMappingsWithRealOidPersisted;
    }

    private void registerReplacedOidMappings(final Set<OidMapping> oidMappings, final DSLContext innerDslContext) {
        final Set<InsertReturningStep<ScopeIdReplacementLogRecord>> inserts =
            oidMappings.stream().map(mapping -> dslContext.insertInto(Tables.SCOPE_ID_REPLACEMENT_LOG)
                    .columns(Tables.SCOPE_ID_REPLACEMENT_LOG.REPLACEMENT_LOG_ID,
                        Tables.SCOPE_ID_REPLACEMENT_LOG.REAL_ID)
                    .values(getLogId(), mapping.oidMappingKey().realOid()).onDuplicateKeyIgnore())
                .collect(Collectors.toSet());

        innerDslContext.batch(inserts).execute();
        oidMappings.forEach(mapping -> {
            replacedScopeIdsToOidMappings.compute(
                mapping.oidMappingKey().aliasOid(), (oid, currentlyRegisteredMappings) -> {
                    if (currentlyRegisteredMappings != null) {
                        currentlyRegisteredMappings.add(mapping);
                        return currentlyRegisteredMappings;
                    } else {
                        return new HashSet<>(Collections.singleton(mapping));
                    }
                });
        });
    }

    private OidMapping recordToOidMapping(final Record3<Long, Long, Instant> record) {
        return ImmutableOidMapping.builder()
            .oidMappingKey(ImmutableOidMappingKey.builder()
                .realOid(record.component1()).aliasOid(record.component2()).build())
            .firstDiscoveredTimeMsUtc(record.component3())
            .build();
    }
}