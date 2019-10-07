package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_BOUGHT;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_SPEC;
import static org.jooq.impl.DSL.sum;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Result;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;

/**
 * This class is used to update reserved instance table by latest reserved instance bought data which
 * comes from Topology Processor. And it use "probeReservedInstanceId" to tell if the latest reserved instance
 * is same with current existing reserved instance record or not.
 */
public class ReservedInstanceBoughtStore {

    private static final Logger logger = LogManager.getLogger();

    private final IdentityProvider identityProvider;

    private final DSLContext dsl;

    // A temporary column name used for query reserved instance count map.
    private static final String RI_SUM_COUNT = "ri_sum_count";

    private final Flux<ReservedInstanceBoughtChangeType> updateEventFlux;

    /**
     * The statusEmitter is used to push updates to the statusFlux subscribers.
     */
    private FluxSink<ReservedInstanceBoughtChangeType> updateEventEmitter;

    /**
     * TODO: JavaDoc: What is this enum needed for?
     */
    public enum ReservedInstanceBoughtChangeType {
        UPDATED;
    }

    public ReservedInstanceBoughtStore(@Nonnull final DSLContext dsl,
                                       @Nonnull final IdentityProvider identityProvider) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.dsl = Objects.requireNonNull(dsl);
        // create a flux that a listener can subscribe to group store update events on.
        updateEventFlux = Flux.create(emitter -> updateEventEmitter = emitter);
        // start publishing immediately w/o waiting for a consumer to signal demand.
        updateEventFlux.publish().connect();
    }

    /**
     * Only used for JUnit tests.
     */
    @VisibleForTesting
    ReservedInstanceBoughtStore() {
        this.identityProvider = null;
        this.dsl = null;
        // create a flux that a listener can subscribe to group store update events on.
        updateEventFlux = null;
    }

    /**
     * Get all {@link ReservedInstanceBought} from reserved instance table.
     *
     * @param filter {@link ReservedInstanceBoughtFilter} which contains all filter condition.
     * @return a list of {@link ReservedInstanceBought}.
     */
    public List<ReservedInstanceBought> getReservedInstanceBoughtByFilter(
            @Nonnull final ReservedInstanceBoughtFilter filter) {
        return getReservedInstanceBoughtByFilterWithContext(dsl, filter);
    }

    public List<ReservedInstanceBought> getReservedInstanceBoughtByFilterWithContext(
            @Nonnull final DSLContext context,
            @Nonnull final ReservedInstanceBoughtFilter filter) {
        return internalGet(context, filter).stream()
                .map(this::reservedInstancesToProto)
                .collect(Collectors.toList());
    }

    /**
     * Get the sum count of reserved instance bought by different compute tier type.
     *
     * @param filter a {@link ReservedInstanceBoughtFilter} contains all filter condition of the request.
     * @return a Map which key is compute tier id and value is the sum count of reserved instance bought
     * which belong to this type computer tier.
     */
    public Map<Long, Long> getReservedInstanceCountMap(@Nonnull final ReservedInstanceBoughtFilter filter) {
        final Result<Record2<Long, Long>> riCountMap = dsl.select(RESERVED_INSTANCE_SPEC.TIER_ID,
                (sum(RESERVED_INSTANCE_BOUGHT.COUNT)).cast(Long.class).as(RI_SUM_COUNT))
                .from(RESERVED_INSTANCE_BOUGHT)
                .join(RESERVED_INSTANCE_SPEC)
                .on(RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID.eq(RESERVED_INSTANCE_SPEC.ID))
                .where(filter.getConditions())
                .groupBy(RESERVED_INSTANCE_SPEC.TIER_ID)
                .fetch();
        return riCountMap.intoMap(riCountMap.field(RESERVED_INSTANCE_SPEC.TIER_ID),
                (riCountMap.field(RI_SUM_COUNT)).cast(Long.class));
    }

    /**
     * Get the sum count of reserved instance bought by RI spec ID.
     *
     * @return a Map which key is reservedInstance spec ID and value is the sum count of reserved instance bought
     * which belong to this spec.
     */
    public Map<String, Long> getReservedInstanceCountByRISpecIdMap() {
        final Result<Record2<ReservedInstanceBoughtInfo, Long>> riCountMap =
                dsl.select(RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_BOUGHT_INFO,
                        (sum(RESERVED_INSTANCE_BOUGHT.COUNT)).cast(Long.class).as(RI_SUM_COUNT))
        .from(RESERVED_INSTANCE_BOUGHT).groupBy(RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID).fetch();
        Map<ReservedInstanceBoughtInfo, Long> riSpecMap = riCountMap.intoMap(riCountMap
                        .field(RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_BOUGHT_INFO),
                (riCountMap.field(RI_SUM_COUNT).cast(Long.class)));
        Map<String, Long> countsByTemplate = new HashMap<>();
        for (ReservedInstanceBoughtInfo riInfo: riSpecMap.keySet()) {
            String key = riInfo.getDisplayName();
            if (countsByTemplate.containsKey(riInfo.getDisplayName())) {
                countsByTemplate.put(key, countsByTemplate.get(key) + riSpecMap.get(riInfo));
            } else {
                countsByTemplate.put(key, riSpecMap.get(riInfo));
            }
        }
        return countsByTemplate;
    }

    public void updateReservedInstanceBought(
            @Nonnull final List<ReservedInstanceBoughtInfo> newReservedInstances) {
        updateReservedInstanceBought(dsl, newReservedInstances);
    }

    /**
     * Input a list of latest {@link ReservedInstanceBoughtInfo}, it will update reserved instance table
     * based on "probeReservedInstanceId" field to tell if two reserved instance bought are same or
     * not. And for new added reserved instance data which is not exist in current table, those data will
     * be insert to table as new records. For those table records which also appeared in latest reserved instance
     * bought data, they will be updated based on the latest data. For those table records which is not
     * appeared in latest reserved instance bought data, they will be deleted from table.
     *
     * @param context {@link DSLContext} transactional context.
     * @param newReservedInstances a list of {@link ReservedInstanceBoughtInfo}.
     */
    public void updateReservedInstanceBought(
            @Nonnull final DSLContext context,
            @Nonnull final List<ReservedInstanceBoughtInfo> newReservedInstances) {
        logger.info("Updating reserved instance bought...");
        final List<ReservedInstanceBoughtRecord> existingReservedInstances =
                internalGet(context, ReservedInstanceBoughtFilter.newBuilder()
                        .build());
        final Map<String, ReservedInstanceBoughtRecord> existingRIsProbeKeyToRecord =
                existingReservedInstances.stream()
                        .collect(Collectors.toMap(ri -> ri.getProbeReservedInstanceId(),
                                Function.identity()));
        final Map<String, ReservedInstanceBoughtInfo> newRIsProbeKeysToRI = newReservedInstances.stream()
                .collect(Collectors.toMap(ReservedInstanceBoughtInfo::getProbeReservedInstanceId,
                        Function.identity()));
        final List<ReservedInstanceBoughtInfo> reservedInstanceToAdd =
                Sets.difference(newRIsProbeKeysToRI.keySet(),
                        existingRIsProbeKeyToRecord.keySet()).stream()
                        .map(newRIsProbeKeysToRI::get)
                        .collect(Collectors.toList());
        final Map<String, ReservedInstanceBoughtInfo> reservedInstanceUpdates =
                Sets.intersection(newRIsProbeKeysToRI.keySet(), existingRIsProbeKeyToRecord.keySet()).stream()
                        .collect(Collectors.toMap(
                                key -> existingRIsProbeKeyToRecord.get(key).getProbeReservedInstanceId(),
                                newRIsProbeKeysToRI::get));
        final Set<ReservedInstanceBoughtRecord> reservedInstanceToRemove =
                Sets.difference(existingRIsProbeKeyToRecord.keySet(), newRIsProbeKeysToRI.keySet()).stream()
                        .map(existingRIsProbeKeyToRecord::get)
                        .collect(Collectors.toSet());
        internalInsert(context, reservedInstanceToAdd);
        internalUpdate(context, reservedInstanceUpdates, existingRIsProbeKeyToRecord);
        internalDelete(context, reservedInstanceToRemove);

        // Means the RI Buy inventory has changed.
        if (reservedInstanceToAdd.size() > 0 || reservedInstanceToRemove.size() > 0) {
            updateEventEmitter.next(ReservedInstanceBoughtChangeType.UPDATED);
        }
        logger.info("Finished updating reserved instance bought.");
    }

    /**
     * Get the Reserved Instance Bought store update event stream. A listener can .subscribe() to the {@link Flux} that
     * is returned and get access to any update events in the RI Inventory.
     * @return flux for Reserved Instance Bought store update event
     */
    public Flux<ReservedInstanceBoughtChangeType> getUpdateEventStream() {
        return updateEventFlux;
    }

    /**
     * Insert a list of new {@link ReservedInstanceBoughtInfo} into table.
     *
     * @param context {@link DSLContext} transactional context.
     * @param reservedInstancesToAdd a list of new {@link ReservedInstanceBoughtInfo}.
     */
    private void internalInsert(@Nonnull final DSLContext context,
                                @Nonnull final List<ReservedInstanceBoughtInfo> reservedInstancesToAdd) {
        final List<ReservedInstanceBoughtRecord> reservedInstancesRecordToAdd = reservedInstancesToAdd.stream()
                .map(ri -> createNewReservedInstance(context, ri))
                .collect(Collectors.toList());
        context.batchInsert(reservedInstancesRecordToAdd).execute();
    }

    /**
     * Update existing {@link ReservedInstanceBoughtRecord} based on latest {@link ReservedInstanceBoughtInfo} data.
     *
     * @param context {@link DSLContext} transactional context.
     * @param reservedInstanceInfoMap A Map which key is "probeReservedInstanceId", value is
     *                                {@link ReservedInstanceBoughtInfo}.
     * @param reservedInstanceRecordMap A Map which key is "ReservedInstanceInfo", value is
     *                                  {@link ReservedInstanceBoughtRecord}.
     */
    private void internalUpdate(@Nonnull final DSLContext context,
                                @Nonnull final Map<String, ReservedInstanceBoughtInfo> reservedInstanceInfoMap,
                                @Nonnull final Map<String, ReservedInstanceBoughtRecord> reservedInstanceRecordMap) {
        final List<ReservedInstanceBoughtRecord> reservedInstanceRecordsUpdates =
                reservedInstanceInfoMap.entrySet().stream().map(
                        entry -> updateReservedInstanceRecord(entry.getValue(),
                                reservedInstanceRecordMap.get(entry.getKey())))
                .collect(Collectors.toList());
        context.batchUpdate(reservedInstanceRecordsUpdates).execute();
    }

    /**
     * Delete a list of {@link ReservedInstanceBoughtRecord} from table.
     *
     * @param context DSL context.
     * @param reservedInstanceToRemove set of ReservedInstanceBoughtRecord to be removed.
     */
    private void internalDelete(@Nonnull final DSLContext context,
                                @Nonnull final Set<ReservedInstanceBoughtRecord> reservedInstanceToRemove) {
        context.batchDelete(reservedInstanceToRemove).execute();
    }

    /**
     * Get a list of {@link ReservedInstanceBoughtRecord} filter by the input {@link ReservedInstanceBoughtFilter}.
     *
     * @param context {@link DSLContext} transactional context.
     * @param filter {@link ReservedInstanceBoughtFilter} contains all filter condition.
     * @return a list of matched {@link ReservedInstanceBoughtRecord}.
     */
    private List<ReservedInstanceBoughtRecord> internalGet(
            @Nonnull final DSLContext context,
            @Nonnull final ReservedInstanceBoughtFilter filter) {
        if (filter.isJoinWithSpecTable()) {
            return context.select(RESERVED_INSTANCE_BOUGHT.fields())
                    .from(RESERVED_INSTANCE_BOUGHT)
                    .join(RESERVED_INSTANCE_SPEC)
                    .on(RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID.eq(RESERVED_INSTANCE_SPEC.ID))
                    .where(filter.getConditions())
                    .fetch()
                    .into(RESERVED_INSTANCE_BOUGHT);
        } else {
            return context.selectFrom(RESERVED_INSTANCE_BOUGHT)
                    .where(filter.getConditions())
                    .fetch();
        }
    }

    /**
     * Create a new {@link ReservedInstanceBoughtRecord} based on input {@link ReservedInstanceBoughtInfo}.
     *
     * @param context context for DSL.
     * @param reservedInstanceInfo bought information on an RI.
     * @return a record for the ReservedInstance that was bought
     */
    private ReservedInstanceBoughtRecord createNewReservedInstance(
            @Nonnull final DSLContext context,
            @Nonnull final ReservedInstanceBoughtInfo reservedInstanceInfo) {
        return context.newRecord(RESERVED_INSTANCE_BOUGHT, new ReservedInstanceBoughtRecord(
                identityProvider.next(),
                reservedInstanceInfo.getBusinessAccountId(),
                reservedInstanceInfo.getProbeReservedInstanceId(),
                reservedInstanceInfo.getReservedInstanceSpec(),
                reservedInstanceInfo.getAvailabilityZoneId(),
                reservedInstanceInfo,
                reservedInstanceInfo.getNumBought()));
    }

    /**
     * Update {@link ReservedInstanceBoughtRecord} data based on input {@link ReservedInstanceBoughtInfo}.
     *
     * @param reservedInstanceInfo a {@link ReservedInstanceBoughtInfo}.
     * @param reservedInstanceRecord a {@link ReservedInstanceBoughtRecord}.
     * @return a {@link ReservedInstanceBoughtRecord}.
     */
    private ReservedInstanceBoughtRecord updateReservedInstanceRecord(
            @Nonnull final ReservedInstanceBoughtInfo reservedInstanceInfo,
            @Nonnull final ReservedInstanceBoughtRecord reservedInstanceRecord) {
        reservedInstanceRecord.setBusinessAccountId(reservedInstanceInfo.getBusinessAccountId());
        reservedInstanceRecord.setAvailabilityZoneId(reservedInstanceInfo.getAvailabilityZoneId());
        reservedInstanceRecord.setProbeReservedInstanceId(reservedInstanceInfo.getProbeReservedInstanceId());
        reservedInstanceRecord.setReservedInstanceSpecId(reservedInstanceInfo.getReservedInstanceSpec());
        reservedInstanceRecord.setReservedInstanceBoughtInfo(reservedInstanceInfo);
        return reservedInstanceRecord;
    }

    /**
     * Convert {@link ReservedInstanceBoughtRecord} to {@link ReservedInstanceBought}.
     *
     * @param reservedInstanceRecord {@link ReservedInstanceBoughtRecord}.
     * @return a {@link ReservedInstanceBought}.
     */
    private ReservedInstanceBought reservedInstancesToProto(
            @Nonnull final ReservedInstanceBoughtRecord reservedInstanceRecord) {
        return ReservedInstanceBought.newBuilder()
                .setId(reservedInstanceRecord.getId())
                .setReservedInstanceBoughtInfo(reservedInstanceRecord.getReservedInstanceBoughtInfo())
                .build();
    }
}
