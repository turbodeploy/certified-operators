package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.BUY_RESERVED_INSTANCE;
import static org.jooq.impl.DSL.sum;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.Record4;
import org.jooq.Result;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.cost.component.db.tables.records.BuyReservedInstanceRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.cost.component.reserved.instance.filter.BuyReservedInstanceCostFilter;
import com.vmturbo.cost.component.reserved.instance.filter.BuyReservedInstanceFilter;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisRecommendation;

/**
 * This class is used to store the reserved instance to buy for plans or for real time
 */
public class BuyReservedInstanceStore implements BuyReservedInstanceCostStore {

    private final static Logger logger = LogManager.getLogger();

    private final IdentityProvider identityProvider;

    private static final String RI_AMORTIZED_SUM = "ri_amortized_sum";

    private static final String RI_RECURRING_COST_SUM = "ri_recurring_cost_sum";

    private static final String RI_FIXED_COST_SUM = "ri_fixed_cost_sum";

    private static final String RI_AMORTIZED_COST = "ri_amortized_cost";

    private static final String RI_RECURRING_COST = "ri_recurring_cost";

    private static final String RI_FIXED_COST = "ri_fixed_cost";

    private final DSLContext dsl;

    public BuyReservedInstanceStore(@Nonnull final DSLContext dsl,
                                    @Nonnull final IdentityProvider identityProvider) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.dsl = Objects.requireNonNull(dsl);
    }

    @Nonnull
    public List<ReservedInstanceBought> getBuyReservedInstances(
            @Nonnull final BuyReservedInstanceFilter filter) {
        List<BuyReservedInstanceRecord> buyRiRecords =  dsl.selectFrom(BUY_RESERVED_INSTANCE)
                .where(filter.getConditions())
                .fetch();
        return buyRiRecords.stream().map(this::reservedInstancesToProto)
                .collect(Collectors.toList());
    }

    @Override
    public Cost.ReservedInstanceCostStat getReservedInstanceAggregatedAmortizedCost(
                    @Nonnull BuyReservedInstanceCostFilter filter) {
        final Result<Record3<BigDecimal, BigDecimal, BigDecimal>> riAggregatedCostResult =
                        dsl.select(sum(BUY_RESERVED_INSTANCE.PER_INSTANCE_AMORTIZED_COST_HOURLY.mul(BUY_RESERVED_INSTANCE.COUNT))
                                        .as(RI_AMORTIZED_SUM),
                                        sum(BUY_RESERVED_INSTANCE.PER_INSTANCE_RECURRING_COST_HOURLY.mul(BUY_RESERVED_INSTANCE.COUNT)).as(RI_RECURRING_COST_SUM),
                                        sum(BUY_RESERVED_INSTANCE.PER_INSTANCE_FIXED_COST.mul(BUY_RESERVED_INSTANCE.COUNT)).as(RI_FIXED_COST_SUM))
                                        .from(BUY_RESERVED_INSTANCE)
                                        .where(filter.getConditions()).fetch();

        final Cost.ReservedInstanceCostStat buyReservedInstanceCostStat = Cost.ReservedInstanceCostStat.newBuilder()
                        .setAmortizedCost(riAggregatedCostResult
                                        .getValues(RI_AMORTIZED_SUM, Double.class).stream()
                                        .filter(s -> s != null).findFirst().orElse(0D))
                        .setFixedCost(riAggregatedCostResult
                                        .getValues(RI_FIXED_COST_SUM, Double.class).stream()
                                        .filter(s -> s != null).findFirst().orElse(0D))
                        .setRecurringCost(riAggregatedCostResult
                                        .getValues(RI_RECURRING_COST_SUM, Double.class)
                                        .stream().filter(s -> s != null).findFirst().orElse(0D))
                        .setSnapshotTime(now()).build();

        return buyReservedInstanceCostStat;
    }

    /**
     * Get the Reserved instance cost stats for each RI Type. The amortized, fixed and recurring costs are
     * multiplied by the number of counts of the RI Type to get the total costs for that RI Type.
     *
     * @param filter filter of type BuyReservedInstanceCostFilter.
     * @return List of type ReservedInstanceCostStat containing the cost stats.
     */
    public List<Cost.ReservedInstanceCostStat> getReservedInstanceCostStats(@Nonnull
                    BuyReservedInstanceCostFilter filter) {
        final Result<Record4<Long, Double, Double, Double>> buyRICostResult =
                        dsl.select(BUY_RESERVED_INSTANCE.RESERVED_INSTANCE_SPEC_ID,
                                        BUY_RESERVED_INSTANCE.PER_INSTANCE_AMORTIZED_COST_HOURLY
                                                        .mul(BUY_RESERVED_INSTANCE.COUNT)
                                                        .as(RI_AMORTIZED_COST),
                                        BUY_RESERVED_INSTANCE.PER_INSTANCE_FIXED_COST
                                                        .mul(BUY_RESERVED_INSTANCE.COUNT)
                                                        .as(RI_FIXED_COST),
                                        BUY_RESERVED_INSTANCE.PER_INSTANCE_RECURRING_COST_HOURLY
                                                        .mul(BUY_RESERVED_INSTANCE.COUNT)
                                                        .as(RI_RECURRING_COST))
                                        .from(BUY_RESERVED_INSTANCE).where(filter.getConditions())
                                        .fetch();

        final List<Cost.ReservedInstanceCostStat> riCostStats = new ArrayList<>(buyRICostResult.size());
        for (Record4<Long, Double, Double, Double> record : buyRICostResult) {
            final Cost.ReservedInstanceCostStat riCostStat = Cost.ReservedInstanceCostStat.newBuilder()
                            .setReservedInstanceOid(
                                            record.get(BUY_RESERVED_INSTANCE.RESERVED_INSTANCE_SPEC_ID))
                            .setRecurringCost(record.get(RI_RECURRING_COST, Double.class))
                            .setFixedCost(record.get(RI_FIXED_COST, Double.class))
                            .setAmortizedCost(record.get(RI_AMORTIZED_COST, Double.class))
                            .setSnapshotTime(now()).build();
            riCostStats.add(riCostStat);
        }
        return riCostStats;
    }

    private static long now() {
        return Clock.systemUTC().instant().toEpochMilli();
    }

    /**
     * Method to query the BuyReservedInstance Cost Stats. If GroupBy is based on SNAPSHOT_TIME, a single
     * ReservedInstanceCostStat object is returned with the aggregated fixed, recurring and amortized costs
     * for the RIs within the scope.
     * If GroupBy is set to NONE, stats for individual RIs are returned.
     *
     * @param buyReservedInstanceCostFilter Filter of type BuyReservedInstanceCostFilter.
     * @return List of type Cost.ReservedInstanceCostStat.
     */
    @Nonnull
    public List<Cost.ReservedInstanceCostStat> queryBuyReservedInstanceCostStats(@Nonnull BuyReservedInstanceCostFilter buyReservedInstanceCostFilter) {
        final Cost.GetReservedInstanceCostStatsRequest.GroupBy groupBy =
                        buyReservedInstanceCostFilter.getGroupBy();
        if (Cost.GetReservedInstanceCostStatsRequest.GroupBy.SNAPSHOT_TIME == groupBy) {
            final Cost.ReservedInstanceCostStat buyReservedInstanceStoreReservedInstanceCosts =
                            getReservedInstanceAggregatedAmortizedCost(buyReservedInstanceCostFilter);
            return Collections.singletonList(buyReservedInstanceStoreReservedInstanceCosts);
        } else if (Cost.GetReservedInstanceCostStatsRequest.GroupBy.NONE == groupBy) {
            return getReservedInstanceCostStats(buyReservedInstanceCostFilter);
        }
        return Collections.EMPTY_LIST;
    }

    /**
     * Replaces all the Buy RIs for a topology context id. All the new Buy RIs are assumed to be
     * of the same topology context id.
     * @param newRecommendations new Buy RI recommendations
     * @param topologyContextId the topology context id
     */
    public void udpateBuyReservedInstances(@Nonnull final Collection<ReservedInstanceAnalysisRecommendation> newRecommendations,
                                           final long topologyContextId) {
        // First, delete the existing buy RIs for the given topologyContextId. (Deleting applies
        // to real time buy RIs). Then create the new buy RIs.
        if (newRecommendations.isEmpty()) {
            return;
        }
        deleteBuyReservedInstances(topologyContextId);
        createBuyReservedInstances(newRecommendations, topologyContextId);
    }

    /**
     * Creates BuyReservedInstanceRecords from a collection of new {@link ReservedInstanceAnalysisRecommendation}
     * and inserts into table.
     *
     * @param recommendations a list of {@link ReservedInstanceAnalysisRecommendation} to buy.
     * @param topologyContextId the topology context id
     */
    private void createBuyReservedInstances(@Nonnull final Collection<ReservedInstanceAnalysisRecommendation> recommendations,
                                            final long topologyContextId) {
        final List<BuyReservedInstanceRecord> buyRiRecordsToAdd =
            recommendations.stream()
                        .map(r -> createBuyReservedInstance(r, topologyContextId))
                        .collect(Collectors.toList());
        dsl.batchInsert(buyRiRecordsToAdd).execute();
    }

    /**
     * Deletes reservedInstancesToBuy from a list of new {@link ReservedInstanceBoughtInfo}
     * and inserts into table.
     *
     * @param topologyContextId this id identifies the topology.
     */
    private void deleteBuyReservedInstances(final long topologyContextId) {
        dsl.deleteFrom(BUY_RESERVED_INSTANCE).where(BUY_RESERVED_INSTANCE.TOPOLOGY_CONTEXT_ID
                .eq(topologyContextId)).execute();
    }

    /**
     * Create a new {@link ReservedInstanceBoughtRecord} based on input {@link ReservedInstanceBoughtInfo}.
     *
     * @param recommendation the recommendation
     * @param topologyContextId the topology context id
     * @return The BuyReservedInstanceRecord created in the database
     */
    private BuyReservedInstanceRecord createBuyReservedInstance(
            @Nonnull final ReservedInstanceAnalysisRecommendation recommendation,
            final long topologyContextId) {
        ReservedInstanceBoughtInfo riBoughtInfo = recommendation.getRiBoughtInfo();
        ReservedInstanceSpec riSpec = recommendation.getRiSpec();
        recommendation.setBuyRiId(identityProvider.next());

        logger.debug("FixedCost: {}, Recurring Cost: {}, Term: {}, AmortizedCost: {}", () -> riBoughtInfo
                                        .getReservedInstanceBoughtCost()
                                        .getFixedCost()
                                        .getAmount(),
                        () -> riBoughtInfo.getReservedInstanceBoughtCost().getRecurringCostPerHour().getAmount(),
                        () -> riSpec.getReservedInstanceSpecInfo().getType().getTermYears(),
                        () -> recommendation.getRiHourlyCost());
        return dsl.newRecord(BUY_RESERVED_INSTANCE, new BuyReservedInstanceRecord(
                        recommendation.getBuyRiId(),
                        topologyContextId,
                        riBoughtInfo.getBusinessAccountId(),
                        riSpec.getReservedInstanceSpecInfo().getRegionId(),
                        riBoughtInfo.getReservedInstanceSpec(),
                        riBoughtInfo.getNumBought(),
                        riBoughtInfo,
                        riBoughtInfo.getReservedInstanceBoughtCost().getFixedCost().getAmount(),
                        riBoughtInfo.getReservedInstanceBoughtCost().getRecurringCostPerHour().getAmount(),
                        recommendation.getRiHourlyCost()));
    }

    /**
     * Convert {@link BuyReservedInstanceRecord} to {@link ReservedInstanceBought}
     *
     * @param buyRiRecord {@link BuyReservedInstanceRecord}.
     * @return a {@link ReservedInstanceBought}.
     */
    private ReservedInstanceBought reservedInstancesToProto(
            @Nonnull final BuyReservedInstanceRecord buyRiRecord) {
        return ReservedInstanceBought.newBuilder()
                .setId(buyRiRecord.getId())
                .setReservedInstanceBoughtInfo(buyRiRecord.getReservedInstanceBoughtInfo())
                .build();
    }
}
