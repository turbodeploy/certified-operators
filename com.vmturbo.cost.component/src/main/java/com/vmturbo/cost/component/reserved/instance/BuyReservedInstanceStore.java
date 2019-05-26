package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.BUY_RESERVED_INSTANCE;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_BOUGHT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.immutables.value.Value.NaturalOrder;
import org.jooq.Condition;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.BuyReservedInstanceRecord;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisRecommendation;

/**
 * This class is used to store the reserved instance to buy for plans or for real time
 */
public class BuyReservedInstanceStore {

    private final static Logger logger = LogManager.getLogger();

    private final IdentityProvider identityProvider;

    private final DSLContext dsl;

    public BuyReservedInstanceStore(@Nonnull final DSLContext dsl,
                                    @Nonnull final IdentityProvider identityProvider) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.dsl = Objects.requireNonNull(dsl);
    }

    public Collection<ReservedInstanceBought> getBuyReservedInstances(
            @Nonnull final GetBuyReservedInstancesByFilterRequest request) {
        List<BuyReservedInstanceRecord> buyRiRecords =  dsl.selectFrom(BUY_RESERVED_INSTANCE)
                .where(generateConditions(request))
                .fetch();
        return buyRiRecords.stream().map(this::reservedInstancesToProto)
                .collect(Collectors.toList());
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

        return dsl.newRecord(BUY_RESERVED_INSTANCE, new BuyReservedInstanceRecord(
                recommendation.getBuyRiId(),
                topologyContextId,
                riBoughtInfo.getBusinessAccountId(),
                riSpec.getReservedInstanceSpecInfo().getRegionId(),
                riBoughtInfo.getReservedInstanceSpec(),
                riBoughtInfo.getNumBought(),
                riBoughtInfo));
    }

    private List<Condition> generateConditions(@Nonnull final GetBuyReservedInstancesByFilterRequest request) {
        final List<Condition> conditions = new ArrayList<>();
        if (request.hasTopologyContextId()) {
            conditions.add(Tables.BUY_RESERVED_INSTANCE.TOPOLOGY_CONTEXT_ID.eq(
                    request.getTopologyContextId()));
        }

        if (request.hasRegionFilter() && !request.getRegionFilter().getRegionIdList().isEmpty()) {
            conditions.add(Tables.BUY_RESERVED_INSTANCE.REGION_ID.in(
                    request.getRegionFilter().getRegionIdList()));
        }

        if (request.hasAccountFilter() && !request.getAccountFilter().getAccountIdList().isEmpty()) {
            conditions.add(Tables.BUY_RESERVED_INSTANCE.BUSINESS_ACCOUNT_ID.in(
                    request.getAccountFilter().getAccountIdList()));
        }

        if (request.getBuyRiIdCount() > 0) {
            conditions.add(Tables.BUY_RESERVED_INSTANCE.ID.in(
                    request.getBuyRiIdList()));
        }
        return conditions;
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