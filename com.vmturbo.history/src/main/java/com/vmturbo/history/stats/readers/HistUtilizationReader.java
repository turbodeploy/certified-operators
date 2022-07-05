/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats.readers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.HistoryUtilizationType;
import com.vmturbo.history.schema.abstraction.tables.HistUtilization;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;
import com.vmturbo.history.stats.INonPaginatingStatsReader;
import com.vmturbo.history.stats.live.LiveStatsStore;

/**
 * {@link HistUtilizationReader} reads from {@link HistUtilization#HIST_UTILIZATION} table.
 */
public class HistUtilizationReader implements INonPaginatingStatsReader<HistUtilizationRecord> {
    private final int entitiesPerChunk;
    private final LiveStatsStore liveStatsStore;
    private final DSLContext dsl;

    public HistUtilizationReader(@Nonnull DSLContext dsl, int entitiesPerChunk, LiveStatsStore liveStatsStore) {
        this.dsl = Objects.requireNonNull(dsl);
        this.entitiesPerChunk = entitiesPerChunk;
        this.liveStatsStore = liveStatsStore;
    }

    @Nonnull
    @Override
    public List<HistUtilizationRecord> getRecords(@Nonnull Set<String> entityIds,
                    @Nonnull StatsFilter statsFilter) throws DataAccessException {
        final Map<Integer, Collection<Integer>> propertyTypeToUtilizationTypes =
                        getPropertyTypeToUtilizationTypes(statsFilter);
        final boolean histUtilizationRequestRequired = propertyTypeToUtilizationTypes.values().stream()
                .anyMatch(s -> !s.isEmpty());
        if (histUtilizationRequestRequired) {
            List<HistUtilizationRecord> result =
                            getDataFromHistUtilization(entityIds, propertyTypeToUtilizationTypes);

            Map<Integer, Set<Long>> commodityToProviderIds = new HashMap<>();
            statsFilter.getCommodityRequestsList().forEach(commodityRequest -> {
                final String commodityName = commodityRequest.getCommodityName();
                final int commodityOid = UICommodityType.fromString(commodityName).typeNumber();
                commodityRequest.getPropertyValueFilterList().stream()
                   .filter(propertyValueFilter -> propertyValueFilter.hasProperty() && StringConstants.PRODUCER_UUID.equals(propertyValueFilter.getProperty()))
                   .forEach(propertyValueFilter -> {
                       Set<Long> providerIds = commodityToProviderIds.computeIfAbsent(commodityOid, (k) -> new HashSet<>());
                       providerIds.add(Long.valueOf(propertyValueFilter.getValue()));
                   });
            });

            if (!commodityToProviderIds.isEmpty()) {
                List<HistUtilizationRecord> filteredResult = new ArrayList<>();
                for (HistUtilizationRecord record : result) {
                    final int propertyTypeId = record.getPropertyTypeId();
                    if (commodityToProviderIds.containsKey(propertyTypeId)) {
                        final Long producerOid = record.getProducerOid();
                        if (commodityToProviderIds.get(propertyTypeId).contains(producerOid)) {
                            filteredResult.add(record);
                        }
                    } else {
                        filteredResult.add(record);
                    }
                }
                result = filteredResult;
            }
            result.addAll(getLiveStats(entityIds, statsFilter));
            return Collections.unmodifiableList(result);
        }
        return Collections.unmodifiableList(getLiveStats(entityIds, statsFilter));
    }

    @Nonnull
    private List<HistUtilizationRecord> getDataFromHistUtilization(
                    @Nonnull Collection<String> entityIds,
                    @Nonnull Map<Integer, Collection<Integer>> propertyTypeToUtilizationTypes) {
        try {
            final Condition condition =
                            getPropertyToUtilizationTypeConditions(propertyTypeToUtilizationTypes);

            final List<HistUtilizationRecord> result = getChunkedEntityIds(entityIds).stream()
                            .map(chunk -> getHistUtilizationRecordsPage(chunk, dsl, condition))
                            .flatMap(Collection::stream).collect(Collectors.toList());
            return result;
        } catch (DataAccessException e) {
            throw new DataAccessException("Failed in connection auto-close", e);
        }
    }

    @Nonnull
    private List<List<String>> getChunkedEntityIds(@Nonnull Collection<String> entityIds) {
        final List<String> orderedEntityIds = new ArrayList<>(entityIds);
        if (entityIds.size() < entitiesPerChunk) {
            return Collections.singletonList(orderedEntityIds);
        }
        return Lists.partition(orderedEntityIds, entitiesPerChunk);
    }

    @Nonnull
    private static Result<HistUtilizationRecord> getHistUtilizationRecordsPage(
                    @Nonnull Collection<String> entityIds, @Nonnull DSLContext context,
                    @Nonnull Condition propertyToUtilizationTypeCondition) {
        final Collection<Condition> conditions = new HashSet<>();
        conditions.add(propertyToUtilizationTypeCondition);
        if (!entityIds.isEmpty()) {
            final Condition oidCondition = HistUtilization.HIST_UTILIZATION.OID
                            .in(entityIds.stream().map(Long::valueOf).collect(Collectors.toSet()));
            conditions.add(oidCondition);
        }
        final SelectConditionStep<HistUtilizationRecord> request =
                        context.selectFrom(HistUtilization.HIST_UTILIZATION)
                                        .where(DSL.and(conditions));
        return request.fetch();
    }

    @Nonnull
    private static Condition getPropertyToUtilizationTypeConditions(
                    @Nonnull Map<Integer, Collection<Integer>> propertyTypeToUtilizationTypes) {
        final Collection<Condition> result = new HashSet<>();
        propertyTypeToUtilizationTypes.forEach((propertyType, utilizationTypes) -> {
            final Condition propertyTypeCondition =
                            HistUtilization.HIST_UTILIZATION.PROPERTY_TYPE_ID.eq(propertyType);
            if (utilizationTypes.isEmpty()) {
                result.add(propertyTypeCondition);
                return;
            }
            final Condition valueTypeCondition =
                            HistUtilization.HIST_UTILIZATION.VALUE_TYPE.in(utilizationTypes);
            result.add(DSL.and(propertyTypeCondition, valueTypeCondition));
        });
        return DSL.or(result);
    }

    @Nonnull
    private static Map<Integer, Collection<Integer>> getPropertyTypeToUtilizationTypes(
                    @Nonnull StatsFilter statsFilter) {
        final Map<Integer, Collection<Integer>> propertyTypeToUtilizationTypes = new HashMap<>();
        for (CommodityRequest request : statsFilter.getCommodityRequestsList()) {
            final int propertyId =
                            UICommodityType.fromString(request.getCommodityName()).typeNumber();
            Collection<Integer> utilizationTypes = getRequestedIds(request.getGroupByList(),
                            HistoryUtilizationType::ordinal, HistoryUtilizationType.values());
            if (!utilizationTypes.isEmpty()) {
                // populate property to utilization mapping only for records with a constraint to lookup.
                propertyTypeToUtilizationTypes.computeIfAbsent(propertyId, k -> new HashSet<>())
                        .addAll(utilizationTypes);
            }
        }
        return propertyTypeToUtilizationTypes;
    }

    @Nonnull
    private static <E extends Enum<E>> Collection<Integer> getRequestedIds(
                    @Nonnull Iterable<String> parameters, @Nonnull Function<E, Integer> idGetter,
                    @Nonnull E... values) {
        final Collection<Integer> result = new HashSet<>();
        for (String parameter : parameters) {
            for (E value : values) {
                if (parameter.toLowerCase().contains(value.name().toLowerCase())) {
                    result.add(idGetter.apply(value));
                }
            }
        }
        return result;
    }

    /**
     * Lookup caches in the LiveStatsStore and retrieve smoothedUsage stats for the requested commodities on entities specified.
     *
     * @param entityOids set of entities to retrieve smoothed usage for.
     * @param statsFilter contains the commodity and the type of history requested for each.
     * @return List of {@link HistUtilizationRecord} containing the smoothedUsage stats for the requested commodities on entities specified.
     */
    public List<HistUtilizationRecord> getLiveStats(@Nonnull Set<String> entityOids, @Nonnull StatsFilter statsFilter) {
        List<HistUtilizationRecord> records = new ArrayList<>();
        List<Integer> commTypes = new ArrayList<>();
        for (CommodityRequest request : statsFilter.getCommodityRequestsList()) {
            if (request.hasHistoryType()
                    && request.getHistoryType().toLowerCase().contains(HistoryUtilizationType.Smoothed.toString().toLowerCase())) {
                commTypes.add(UICommodityType.fromString(request.getCommodityName()).typeNumber());
            }
        }
        if (!commTypes.isEmpty()) {
            for (String id : entityOids) {
                long entityOid = new Long(id);
                Map<Integer, Double> entitySmoothedUsageStats = liveStatsStore.getEntityUsageStats(entityOid);
                Map<Integer, Double> entityCapacityStats = liveStatsStore.getEntityCapacityStats(entityOid);
                for (int commType : commTypes) {
                    if (entitySmoothedUsageStats.containsKey(commType)) {
                        records.add(new HistUtilizationRecord(entityOid, 0L, commType, 0, "",
                                HistoryUtilizationType.Smoothed.ordinal(), 0,
                                entitySmoothedUsageStats.get(commType),
                                entityCapacityStats.get(commType)));
                    }
                }
            }

        }
        return records;
    }
}
