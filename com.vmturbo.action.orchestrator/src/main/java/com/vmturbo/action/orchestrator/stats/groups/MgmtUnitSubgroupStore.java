package com.vmturbo.action.orchestrator.stats.groups;

import static com.vmturbo.action.orchestrator.db.Tables.MGMT_UNIT_SUBGROUP;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.db.tables.records.MgmtUnitSubgroupRecord;
import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.action.orchestrator.stats.aggregator.GlobalActionAggregator;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup.MgmtUnitSubgroupKey;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.MgmtUnitSubgroupFilter;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Responsible for storing and retrieving {@link MgmtUnitSubgroup}s to/from the underlying database.
 */
public class MgmtUnitSubgroupStore {

    private static final Logger logger = LogManager.getLogger();

    private static final short UNSET_ENUM_VALUE = -1;

    private final DSLContext dsl;

    public MgmtUnitSubgroupStore(@Nonnull final DSLContext dsl) {
        this.dsl = Objects.requireNonNull(dsl);
    }

    /**
     * Ensure that a set of {@link MgmtUnitSubgroupKey}s exist in the database, and return the
     * {@link MgmtUnitSubgroup} the keys represent. If an {@link MgmtUnitSubgroupKey} does not match an
     * existing {@link MgmtUnitSubgroup}, this method will create the {@link MgmtUnitSubgroup} record.
     *
     * @param keys A set of {@link MgmtUnitSubgroupKey}s.
     * @return key -> {@link MgmtUnitSubgroup}. Each key in the input should have an associated
     *         {@link MgmtUnitSubgroup}.
     */
    @Nonnull
    public Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> ensureExist(@Nonnull final Set<MgmtUnitSubgroupKey> keys) {
        if (keys.isEmpty()) {
            return Collections.emptyMap();
        }

        return dsl.transactionResult(transactionContext -> {
            final DSLContext transactionDsl = DSL.using(transactionContext);

            final int[] inserted = transactionDsl.batch(keys.stream()
                .map(key -> transactionDsl.insertInto(MGMT_UNIT_SUBGROUP)
                    .set(keyToRecord(key))
                    .onDuplicateKeyIgnore())
                .collect(Collectors.toList()))
                .execute();

            final int insertedSum = IntStream.of(inserted).sum();
            if (insertedSum > 0) {
                logger.info("Inserted {} action groups.", insertedSum);
            }

            final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> allExistingMgmtUnits =
                transactionDsl.selectFrom(MGMT_UNIT_SUBGROUP)
                    .fetch()
                    .stream()
                    .map(this::recordToGroup)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toMap(MgmtUnitSubgroup::key, Function.identity()));

            logger.debug("A total of {} mgmt unit subgroups now exist.", allExistingMgmtUnits.size());

            return Maps.filterKeys(allExistingMgmtUnits, keys::contains);
        });
    }

    @Nonnull
    private Optional<MgmtUnitSubgroup> recordToGroup(@Nonnull final MgmtUnitSubgroupRecord record) {
        final ImmutableMgmtUnitSubgroupKey.Builder keyBuilder = ImmutableMgmtUnitSubgroupKey.builder()
            .mgmtUnitId(record.getMgmtUnitId())
            .mgmtUnitType(ManagementUnitType.forNumber(record.getMgmtUnitType()));
        if (record.getEntityType() != null && record.getEntityType() != UNSET_ENUM_VALUE) {
            keyBuilder.entityType(record.getEntityType());
        }
        if (record.getEnvironmentType() != null) {
            EnvironmentType environmentType = EnvironmentType.forNumber(record.getEnvironmentType());
            if (environmentType != null) {
                keyBuilder.environmentType(environmentType);
            } else if (record.getEnvironmentType() != UNSET_ENUM_VALUE) {
                logger.error("Unrecognized environment type in database record: {}",
                    record.getEnvironmentType());
            }
        }

        try {
            return Optional.of(ImmutableMgmtUnitSubgroup.builder()
                .id(record.getId())
                .key(keyBuilder.build())
                .build());
        } catch (IllegalStateException e) {
            logger.error("Failed to build subgroup out of database record. Error: {}",
                e.getLocalizedMessage());
            return Optional.empty();
        }
    }

    @Nonnull
    private MgmtUnitSubgroupRecord keyToRecord(@Nonnull final MgmtUnitSubgroupKey key) {
        MgmtUnitSubgroupRecord record = new MgmtUnitSubgroupRecord();
        // Leave record ID unset - database auto-increment takes care of ID assignment.

        record.setMgmtUnitId(key.mgmtUnitId());
        record.setMgmtUnitType((short)key.mgmtUnitType().getNumber());
        if (key.entityType().isPresent()) {
            record.setEntityType(key.entityType().get().shortValue());
        } else {
            record.setEntityType(UNSET_ENUM_VALUE);
        }

        record.setEnvironmentType((short)key.environmentType().getNumber());
        return record;
    }

    /**
     * Query the {@link MgmtUnitSubgroupStore} to get the subgroups that match a filter.
     *
     * @param mgmtUnitSubgroupFilter A {@link MgmtUnitSubgroupFilter} indicating which
     *                               management unit to target, and which subgroups to return.
     * @param groupBy The {@link GroupBy} indicating how to group final results. This may affect
     *                which management units we need to query.
     * @return A {@link QueryResult} if one or more mgmt unit subgroups match the filter. Empty
     *         otherwise.
     */
    @Nonnull
    public Optional<QueryResult> query(@Nonnull final MgmtUnitSubgroupFilter mgmtUnitSubgroupFilter,
                                       @Nonnull final HistoricalActionStatsQuery.GroupBy groupBy) {
        final List<Condition> conditions = new ArrayList<>();

        final List<Long> targetMgmtUnitId;
        if (mgmtUnitSubgroupFilter.getMarket()) {
            targetMgmtUnitId =
                Collections.singletonList(GlobalActionAggregator.GLOBAL_MGMT_UNIT_ID);
            if (groupBy == GroupBy.BUSINESS_ACCOUNT_ID) {
                // This is a pretty specific case - if we are going to want to group the resulting
                // stats by business account ID, we shouldn't look for the "global" management unit.
                // We should look for all the individual management unit subgroups associated with
                // business accounts.
                conditions.add(MGMT_UNIT_SUBGROUP.MGMT_UNIT_TYPE.eq((short)ManagementUnitType.BUSINESS_ACCOUNT.getNumber()));
            } else if (groupBy == GroupBy.RESOURCE_GROUP_ID) {
                conditions.add(MGMT_UNIT_SUBGROUP.MGMT_UNIT_TYPE.eq((short)ManagementUnitType.RESOURCE_GROUP.getNumber()));
            } else {
                conditions.add(MGMT_UNIT_SUBGROUP.MGMT_UNIT_ID.eq(GlobalActionAggregator.GLOBAL_MGMT_UNIT_ID));
            }
        } else if (mgmtUnitSubgroupFilter.hasMgmtUnitId()) {
            targetMgmtUnitId = Collections.singletonList(mgmtUnitSubgroupFilter.getMgmtUnitId());
            conditions.add(MGMT_UNIT_SUBGROUP.MGMT_UNIT_ID.eq(mgmtUnitSubgroupFilter.getMgmtUnitId()));
        } else if (mgmtUnitSubgroupFilter.hasMgmtUnits()) {
            targetMgmtUnitId = mgmtUnitSubgroupFilter.getMgmtUnits().getMgmtUnitIdsList();
            conditions.add(MGMT_UNIT_SUBGROUP.MGMT_UNIT_ID.in(targetMgmtUnitId));
        } else {
            logger.error("Invalid filter does not target the market or a specific mgmt unit: {}",
                mgmtUnitSubgroupFilter);
            return Optional.empty();
        }

        logger.debug("Querying for subgroups in mgmt unit: {}", targetMgmtUnitId);

        if (mgmtUnitSubgroupFilter.getEntityTypeList().isEmpty()) {
            // Special case - unset entity type, look in the "global" record.
            conditions.add(MGMT_UNIT_SUBGROUP.ENTITY_TYPE.eq(UNSET_ENUM_VALUE));
        } else {
            if (mgmtUnitSubgroupFilter.getEntityTypeCount() > 1) {
                // We allow this, because it's supported in the external API and the error is non-fatal.
                // But the action counts, investment, and savings will likely be wrong because
                // "MOVE" actions will be double-counted.
                logger.warn("Filter: {} targetting {} entity types." +
                        " Combining results of multiple entity types may lead to inflated counts.",
                    mgmtUnitSubgroupFilter, mgmtUnitSubgroupFilter.getEntityTypeCount());
                Metrics.MULTI_ENTITY_TYPE_COUNTER.increment();
            }
            conditions.add(MGMT_UNIT_SUBGROUP.ENTITY_TYPE.in(
                Collections2.transform(mgmtUnitSubgroupFilter.getEntityTypeList(),
                    Integer::shortValue)));
        }

        if (mgmtUnitSubgroupFilter.hasEnvironmentType() &&
                mgmtUnitSubgroupFilter.getEnvironmentType() != EnvironmentType.HYBRID) {
            conditions.add(MGMT_UNIT_SUBGROUP.ENVIRONMENT_TYPE.eq(
                (short)mgmtUnitSubgroupFilter.getEnvironmentType().getNumber()));
        }

        logger.trace("Using the following conditions for the mgmt unit subgroup " +
            "filter {}\nConditions:{}", mgmtUnitSubgroupFilter, conditions);

        try (final DataMetricTimer timer = Metrics.QUERY_HISTOGRAM.startTimer()) {

            final Set<MgmtUnitSubgroup> matchingSubgroups = dsl.selectFrom(MGMT_UNIT_SUBGROUP)
                .where(conditions)
                .fetch()
                .stream()
                .map(this::recordToGroup)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());

            if (matchingSubgroups.isEmpty()) {
                return Optional.empty();
            } else {
                final ImmutableQueryResult.Builder builder = ImmutableQueryResult.builder();

                // We check explicitly for the global mgmt unit id because in the global case
                // we are aggregating information across multiple management units.
                if (!Collections.singletonList(GlobalActionAggregator.GLOBAL_MGMT_UNIT_ID).equals(targetMgmtUnitId)) {
                    builder.mgmtUnit(targetMgmtUnitId);
                }

                // In the future if we want to group results by environment type or entity type we will
                // need to return the full MgmtUnitSubgroup objects as part of the query result.
                // For now just the ID is sufficient.
                matchingSubgroups.forEach(subgroup -> builder.putMgmtUnitSubgroups(subgroup.id(), subgroup));

                return Optional.of(builder.build());
            }
        }
    }

    /**
     * The result of querying the {@link MgmtUnitSubgroupStore}.
     */
    @Value.Immutable
    public interface QueryResult {

        /**
         * The IDs of the mgmt unit targeted by the query. Empty if describing the whole market.
         * @return list of management units target by query.
         */
        List<Long> mgmtUnit();

        /**
         * The mgmt unit subgroups targeted by the query, arranged by ID. Should be non-empty.
         *
         * @return (subgroup id) -> {@link MgmtUnitSubgroup}).
         */
        Map<Integer, MgmtUnitSubgroup> mgmtUnitSubgroups();
    }

    static class Metrics {
        static final DataMetricCounter MULTI_ENTITY_TYPE_COUNTER = DataMetricCounter.builder()
            .withName("ao_mu_subgroup_multi_entity_type_query_count")
            .withHelp("Number of mgmt subunit queries that target multiple entity types.")
            .build()
            .register();

        static final DataMetricHistogram QUERY_HISTOGRAM = DataMetricHistogram.builder()
            .withName("ao_mu_subgroup_store_query_duration_seconds")
            .withHelp("Duration of mgmt subunit queries, in seconds.")
            .withBuckets(0.1, 0.5, 1, 5)
            .build()
            .register();
    }
}
