package com.vmturbo.history.stats;

import static com.vmturbo.history.schema.abstraction.Tables.MKT_SNAPSHOTS_STATS;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.SelectConditionStep;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.history.schema.abstraction.tables.records.MktSnapshotsStatsRecord;

/**
 * Read records from the PLAN topologies stats table, mkt_snapshots_stats.
 **/
public class PlanStatsReader {

    private final DSLContext dsl;

    public PlanStatsReader(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Retrieve stats records for the specified plan, based on the provided filters.
     *
     * @param topologyContextId the plan ID
     * @param commodityRequests a list of named commodities that have been requested
     * @param globalFilter the global filter to apply to all returned stats
     * @return a list of of Market Snapshot Stats Records containing the result
     * @throws DataAccessException when an error occurs while executing the query
     */
    public @Nonnull List<MktSnapshotsStatsRecord> getStatsRecords(
            long topologyContextId,
            @Nonnull List<CommodityRequest> commodityRequests,
            @Nonnull GlobalFilter globalFilter) throws DataAccessException {
        SelectConditionStep<MktSnapshotsStatsRecord> queryBuilder =
                dsl.selectFrom(MKT_SNAPSHOTS_STATS)
                        .where(MKT_SNAPSHOTS_STATS.MKT_SNAPSHOT_ID.equal(topologyContextId));

        List<CommodityRequest> namedCommodityRequests = commodityRequests.stream()
                .filter(CommodityRequest::hasCommodityName)
                .collect(Collectors.toList());
        // If there are any named commodities, then we consider only the named commodities.
        // Otherwise, we do a search for all commodity names but possibly still filtered by related
        // entity type.
        if (namedCommodityRequests.isEmpty()) {
            // An empty list of commodity names means we're looking for all commodities.

            // Respect any relatedEntityTypes defined globally for this request
            final Set<Short> relatedEntityTypes =
                globalFilter.getRelatedEntityTypeList().stream()
                    .map(ApiEntityType::fromString)
                    .map(ApiEntityType::typeNumber)
                    .map(Integer::shortValue)
                    .collect(Collectors.toCollection(HashSet::new));

            // Setting relatedEntityType in an unnamed commodity is another way to define it globally
            relatedEntityTypes.addAll(commodityRequests.stream()
                .filter(CommodityRequest::hasRelatedEntityType)
                .map(CommodityRequest::getRelatedEntityType)
                .map(ApiEntityType::fromString)
                .map(ApiEntityType::typeNumber)
                .map(Integer::shortValue)
                .collect(Collectors.toSet()));

            if (!relatedEntityTypes.isEmpty()) {
                queryBuilder = queryBuilder.and(MKT_SNAPSHOTS_STATS.ENTITY_TYPE.in(relatedEntityTypes));
            }
            return dsl.fetch(queryBuilder);
        }
        // A non-empty list means we're looking for specific ones.
        List<String> remainingCommodityNames = Lists.newArrayList();
        Condition compoundCommodityCondition = null;
        for (CommodityRequest commodityRequest : namedCommodityRequests) {
            final String commodityName = commodityRequest.getCommodityName();
            if (commodityRequest.hasRelatedEntityType()) {
                // Create a separate condition for this commodity request.
                // This is necessary because the caller has requested to limit the scope for
                // this specific commodity request to only include the provided related entity
                // type.
                final short relatedType = Optional.of(commodityRequest.getRelatedEntityType())
                    .map(ApiEntityType::fromString)
                    .map(ApiEntityType::typeNumber)
                    .map(Integer::shortValue)
                    .get();
                Condition currentCondition =
                        MKT_SNAPSHOTS_STATS.PROPERTY_TYPE.eq(commodityName)
                                .and(MKT_SNAPSHOTS_STATS.ENTITY_TYPE.eq(relatedType));

                compoundCommodityCondition = compoundCommodityCondition == null
                        ? currentCondition
                        : compoundCommodityCondition.or(currentCondition);
            } else {
                remainingCommodityNames.add(commodityName);
            }
        }

        // Add any commodities not already handled by a sub-condition to the main condition
        if (!remainingCommodityNames.isEmpty()) {
            Condition remainingNamesCondition = MKT_SNAPSHOTS_STATS.PROPERTY_TYPE.in(remainingCommodityNames);
            compoundCommodityCondition = compoundCommodityCondition == null
                    ? remainingNamesCondition
                    : compoundCommodityCondition.or(remainingNamesCondition);
        }

        queryBuilder = queryBuilder.and(compoundCommodityCondition);
        return dsl.fetch(queryBuilder);
    }
}
