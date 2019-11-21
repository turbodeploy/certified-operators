package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.BUY_RESERVED_INSTANCE;
import static org.jooq.impl.DSL.sum;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Result;

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

    private final DSLContext dsl;

    public BuyReservedInstanceStore(@Nonnull final DSLContext dsl,
                                    @Nonnull final IdentityProvider identityProvider) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.dsl = Objects.requireNonNull(dsl);
    }

    public Collection<ReservedInstanceBought> getBuyReservedInstances(
            @Nonnull final BuyReservedInstanceFilter filter) {
        List<BuyReservedInstanceRecord> buyRiRecords =  dsl.selectFrom(BUY_RESERVED_INSTANCE)
                .where(filter.getConditions())
                .fetch();
        return buyRiRecords.stream().map(this::reservedInstancesToProto)
                .collect(Collectors.toList());
    }

    @Override
    public double getReservedInstanceAggregatedAmortizedCost(
                    @Nonnull BuyReservedInstanceCostFilter filter) {
        final Result<Record1<BigDecimal>> riAggregatedCostResult =
                        dsl.select(sum(BUY_RESERVED_INSTANCE.PER_INSTANCE_AMORTIZED_COST_HOURLY.mul(BUY_RESERVED_INSTANCE.COUNT))
                                        .as(RI_AMORTIZED_SUM)).from(BUY_RESERVED_INSTANCE)
                                        .where(filter.getConditions()).fetch();
        final List<Double> aggregatedRICostList =
                        riAggregatedCostResult.getValues(RI_AMORTIZED_SUM, Double.class);
        return aggregatedRICostList.stream().filter(s -> s != null).collect(Collectors.toList()).stream().findFirst().orElse(0D);
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
