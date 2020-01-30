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
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.HistUtilization;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;
import com.vmturbo.history.stats.HistoryUtilizationType;
import com.vmturbo.history.stats.INonPaginatingStatsReader;

/**
 * {@link HistUtilizationReader} reads from {@link HistUtilization#HIST_UTILIZATION} table.
 */
public class HistUtilizationReader implements INonPaginatingStatsReader<HistUtilizationRecord> {
    private final HistorydbIO historydbIO;
    private final int entitiesPerChunk;

    public HistUtilizationReader(@Nonnull HistorydbIO historydbIO, int entitiesPerChunk) {
        this.historydbIO = Objects.requireNonNull(historydbIO);
        this.entitiesPerChunk = entitiesPerChunk;
    }

    @Nonnull
    @Override
    public List<HistUtilizationRecord> getRecords(@Nonnull Set<String> entityIds,
                    @Nonnull StatsFilter statsFilter) throws VmtDbException {
        final Map<Integer, Collection<Integer>> propertyTypeToUtilizationTypes =
                        getPropertyTypeToUtilizationTypes(statsFilter);
        final boolean histUtilizationRequestRequired = propertyTypeToUtilizationTypes.values().stream()
                        .noneMatch(Collection::isEmpty);
        if (histUtilizationRequestRequired) {
            final List<HistUtilizationRecord> result =
                            getDataFromHistUtilization(entityIds, propertyTypeToUtilizationTypes);
            return Collections.unmodifiableList(result);
        }
        return Collections.emptyList();
    }

    @Nonnull
    private List<HistUtilizationRecord> getDataFromHistUtilization(
                    @Nonnull Collection<String> entityIds,
                    @Nonnull Map<Integer, Collection<Integer>> propertyTypeToUtilizationTypes)
                    throws VmtDbException {
        try (DSLContext context = DSL.using(historydbIO.transConnection())) {
            final Collection<Condition> conditions =
                            getPropertyToUtilizationTypeConditions(propertyTypeToUtilizationTypes);
            final List<HistUtilizationRecord> result = getChunkedEntityIds(entityIds).stream()
                            .map(chunk -> getHistUtilizationRecordsPage(chunk, context, conditions))
                            .flatMap(Collection::stream).collect(Collectors.toList());
            return Collections.unmodifiableList(result);
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
                    @Nonnull Collection<Condition> conditions) {
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
    private static Collection<Condition> getPropertyToUtilizationTypeConditions(
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
        return result;
    }

    @Nonnull
    private static Map<Integer, Collection<Integer>> getPropertyTypeToUtilizationTypes(
                    @Nonnull StatsFilter statsFilter) {
        final Map<Integer, Collection<Integer>> propertyTypeToUtilizationTypes = new HashMap<>();
        for (CommodityRequest request : statsFilter.getCommodityRequestsList()) {
            final int propertyId =
                            UICommodityType.fromString(request.getCommodityName()).typeNumber();
            final Collection<Integer> utilizationTypes = getRequestedIds(request.getGroupByList(),
                            HistoryUtilizationType::ordinal, HistoryUtilizationType.values());
            propertyTypeToUtilizationTypes.computeIfAbsent(propertyId, k -> new HashSet<>())
                            .addAll(utilizationTypes);
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
                if (parameter.contains(value.name().toLowerCase())) {
                    result.add(idGetter.apply(value));
                }
            }
        }
        return Collections.unmodifiableCollection(result);
    }
}
