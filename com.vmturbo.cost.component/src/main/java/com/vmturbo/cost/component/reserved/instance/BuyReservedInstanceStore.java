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

/**
 * This class is used to store the reserved instance to buy for plans or for real time
 */
public class BuyReservedInstanceStore {

    private final static Logger logger = LogManager.getLogger();

    private final IdentityProvider identityProvider;

    private final DSLContext dsl;

    @Value.Immutable
    public interface BuyReservedInstanceInfo {
        long getTopologyContextId();
        ReservedInstanceBoughtInfo getRiBoughtInfo();
        ReservedInstanceSpec getRiSpec();
    }

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
     * @param newReservedInstancesToBuy
     */
    public void udpateBuyReservedInstances(@Nonnull final Collection<BuyReservedInstanceInfo> newReservedInstancesToBuy) {
        // First, delete the existing buy RIs for the given topologyContextId. (Deleting applies
        // to real time buy RIs). Then create the new buy RIs.
        if (newReservedInstancesToBuy.isEmpty()) {
            return;
        }
        deleteBuyReservedInstances(newReservedInstancesToBuy.iterator().next().getTopologyContextId());
        createBuyReservedInstances(newReservedInstancesToBuy);
    }

    /**
     * Creates BuyReservedInstanceRecords from a collection of new {@link ReservedInstanceBoughtInfo}
     * and inserts into table.
     *
     * @param reservedInstancesToBuy a list of {@link ReservedInstanceBoughtInfo} to buy.
     */
    private void createBuyReservedInstances(@Nonnull final Collection<BuyReservedInstanceInfo> reservedInstancesToBuy) {
        final List<BuyReservedInstanceRecord> buyRisRecordsToAdd =
                reservedInstancesToBuy.stream()
                        .map(this::createBuyReservedInstance)
                        .collect(Collectors.toList());
        dsl.batchInsert(buyRisRecordsToAdd).execute();
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
     * @param buyRiInfo the buyRiInfo which consists of RIBoughtInfo and RISpec
     * @return The BuyReservedInstanceRecord created in the database
     */
    private BuyReservedInstanceRecord createBuyReservedInstance(
            @Nonnull final BuyReservedInstanceInfo buyRiInfo) {
        ReservedInstanceBoughtInfo riBoughtInfo = buyRiInfo.getRiBoughtInfo();
        ReservedInstanceSpec riSpec = buyRiInfo.getRiSpec();

        return dsl.newRecord(BUY_RESERVED_INSTANCE, new BuyReservedInstanceRecord(
                identityProvider.next(),
                buyRiInfo.getTopologyContextId(),
                riBoughtInfo.getBusinessAccountId(),
                riSpec.getReservedInstanceSpecInfo().getRegionId(),
                riBoughtInfo.getReservedInstanceSpec(),
                riBoughtInfo.getNumBought(),
                riBoughtInfo));
    }

    private List<Condition> generateConditions(@Nonnull final GetBuyReservedInstancesByFilterRequest request) {
        final List<Condition> conditions = new ArrayList<>();
        if (!request.hasTopologyContextId()) {
            conditions.add(Tables.BUY_RESERVED_INSTANCE.TOPOLOGY_CONTEXT_ID.eq(
                    request.getTopologyContextId()));
        }

        if (!request.hasRegionFilter() && !request.getRegionFilter().getRegionIdList().isEmpty()) {
            conditions.add(Tables.BUY_RESERVED_INSTANCE.REGION_ID.in(
                    request.getRegionFilter().getRegionIdList()));
        }

        if (!request.hasAccountFilter() && !request.getAccountFilter().getAccountIdList().isEmpty()) {
            conditions.add(Tables.BUY_RESERVED_INSTANCE.BUSINESS_ACCOUNT_ID.in(
                    request.getAccountFilter().getAccountIdList()));
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