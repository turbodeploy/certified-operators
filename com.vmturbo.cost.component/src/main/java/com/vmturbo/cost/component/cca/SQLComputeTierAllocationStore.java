package com.vmturbo.cost.component.cca;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierAllocationDatapoint;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.EntityComputeTierAllocation;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableEntityComputeTierAllocation;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableTimeInterval;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter;
import com.vmturbo.cloud.commitment.analysis.demand.store.ComputeTierAllocationStore;
import com.vmturbo.cloud.commitment.analysis.demand.store.EntityComputeTierAllocationFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.EntityCloudScopeRecord;
import com.vmturbo.cost.component.db.tables.records.EntityComputeTierAllocationRecord;
import com.vmturbo.cost.component.entity.scope.SQLCloudScopedStore;
import com.vmturbo.cost.component.topology.TopologyInfoTracker;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * A SQL implementation of {@link ComputeTierAllocationStore}. This store uses cloud scope consolidation,
 * storing the attributes of {@link com.vmturbo.cost.component.entity.scope.EntityCloudScope} within
 * {@link Tables#ENTITY_CLOUD_SCOPE}. An assumption is made the {@link Tables#ENTITY_COMPUTE_TIER_ALLOCATION}
 * has a foreign key relationship with the {@link Tables#ENTITY_CLOUD_SCOPE} table.
 */
public class SQLComputeTierAllocationStore extends SQLCloudScopedStore implements ComputeTierAllocationStore {

    private static final ZoneId UTC_ZONE_ID = ZoneId.from(ZoneOffset.UTC);

    /**
     * A summary metric collecting the total duration in persisting demand record.
     */
    private static final DataMetricSummary TOTAL_PERSISTENCE_DURATION_SUMMARY_METRIC =
            DataMetricSummary.builder()
                    .withName("cost_compute_tier_allocation_persistence_duration_seconds")
                    .withHelp("Total time to persist compute tier allocation records.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    private static final DataMetricSummary EXTENSION_DURATION_SUMMARY_METRIC =
            DataMetricSummary.builder()
                    .withName("cost_compute_tier_allocation_extension_duration_seconds")
                    .withHelp("Total time to extend compute tier allocation records.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    private static final DataMetricSummary EXTENSION_COUNT_SUMMARY_METRIC =
            DataMetricSummary.builder()
                    .withName("cost_compute_tier_allocation_extension_count")
                    .withHelp("The number of records extended in a single persistence round.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    private static final DataMetricSummary NEW_ALLOCATION_COUNT_SUMMARY_METRIC =
            DataMetricSummary.builder()
                    .withName("cost_compute_tier_allocation_new_count")
                    .withHelp("The number of records for which a new record is created in a single persistence round")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    private static final String entityTierDemandDumpFile = "EntityComputeTierAllocation_dump";

    private final Logger logger = LogManager.getLogger();

    private final TopologyInfoTracker topologyInfoTracker;

    private final int batchExtensionSize;

    private final int batchInsertionSize;

    private final AtomicBoolean initialized = new AtomicBoolean(false);


    /**
     * Construct a new instance of {@link SQLComputeTierAllocationStore}.
     * @param dslContext The {@link DSLContext} to use for all queries.
     * @param topologyInfoTracker The {@link TopologyInfoTracker}, use to link allocations from
     *                            consecutive topologies.
     * @param batchUpdateSize The number of records to update in a single query.
     * @param batchInsertSize The number of new records to create in a single query.
     */
    public SQLComputeTierAllocationStore(@Nonnull DSLContext dslContext,
                                         @Nonnull TopologyInfoTracker topologyInfoTracker,
                                         int batchUpdateSize,
                                         int batchInsertSize) {

        super(dslContext);

        this.topologyInfoTracker = Objects.requireNonNull(topologyInfoTracker);
        this.batchExtensionSize = batchUpdateSize;
        this.batchInsertionSize = batchInsertSize;
    }

    /**
     * Persists the allocation data points to the {@link Tables#ENTITY_COMPUTE_TIER_ALLOCATION} table. In
     * writing the data points to the table, if this is the first write on startup, all data points are
     * inserted as new records into the table. If a previous set of data points has been persisted since
     * the store was initialized, the store will first try to "extend" previous records.
     *
     * <p>In extending records, the store will check if the compute tier demand matches for an entity from
     * the topology immediately prior to {@code topologyInfo}. The prior topology is determined through
     * {@link TopologyInfoTracker}. If an entity does not have a record from the previous topology or
     * the compute tier demand differs, a new record will be inserted into the DB.
     *
     * @param topologyInfo The {@link TopologyInfo} associated with the allocated demand.
     * @param allocationDatapoints The compute tier allocation demand. Allocated demand should be only
     */
    @Override
    public void persistAllocations(@Nonnull final TopologyInfo topologyInfo,
                                   @Nonnull Collection<ComputeTierAllocationDatapoint> allocationDatapoints) {


        try (DataMetricTimer persistenceDurationTimer = TOTAL_PERSISTENCE_DURATION_SUMMARY_METRIC.startTimer()) {

            // First, index the records by the entity OID - this is used later in checking for previously
            // recorded records for each entity.
            final Map<Long, ComputeTierAllocationDatapoint> allocationsByEntityOid = allocationDatapoints.stream()
                    .collect(ImmutableMap.toImmutableMap(
                            ComputeTierAllocationDatapoint::entityOid,
                            Function.identity()));

            // Extend any previously recorded records, if the store has been initialized. On first
            // persistence of records after initialization, the store will insert new records for
            // each datapoint.
            final Set<Long> extendedEntityRecords = initialized.getAndSet(true)
                    ? extendEntityRecords(topologyInfo, allocationsByEntityOid)
                    : Collections.EMPTY_SET;


            // Determine the set of data points to insert as new records by taking the difference
            // of the total minus the extended record set.
            final Set<Long> newAllocationEntityOids = Sets.difference(
                    allocationsByEntityOid.keySet(), extendedEntityRecords);

            // The start time and end time of new records will be set to the creation
            // time of the associated topology.
            final LocalDateTime newRecordTimestamp = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(topologyInfo.getCreationTime()), UTC_ZONE_ID);

            // partition the set of data points to insert
            Iterables.partition(newAllocationEntityOids, batchInsertionSize).forEach(batchEntityOids -> {

                Set<ComputeTierAllocationDatapoint> batchAllocations = batchEntityOids.stream()
                        .map(allocationsByEntityOid::get)
                        .collect(ImmutableSet.toImmutableSet());

                insertAllocations(batchAllocations, newRecordTimestamp);
            });

            // Record the number of records extended and inserted.
            EXTENSION_COUNT_SUMMARY_METRIC.observe((double)extendedEntityRecords.size());
            NEW_ALLOCATION_COUNT_SUMMARY_METRIC.observe((double)newAllocationEntityOids.size());

            logger.info("Finished persisting {} data points in {}", allocationDatapoints.size(),
                    Duration.ofSeconds((long)persistenceDurationTimer.getTimeElapsedSecs()));
        }

    }

    /**
     * {@inheritDoc}
     *
     * @param filter The filter for returned {@link EntityComputeTierAllocation} records.
     * @return A {@link Stream} containing all {@link EntityComputeTierAllocation} records matching
     * the {@code filter}.
     */
    @Override
    public Stream<EntityComputeTierAllocation> streamAllocations(@Nonnull final EntityComputeTierAllocationFilter filter) {
        return dslContext.select(DSL.asterisk())
                .from(Tables.ENTITY_COMPUTE_TIER_ALLOCATION)
                .join(Tables.ENTITY_CLOUD_SCOPE)
                .onKey()
                .where(generateConditionsFromFilter(filter))
                .stream()
                .map(this::createAllocationFromRecord);
    }

    /**
     * Deletes records matching {@code filter}. The {@link Tables#ENTITY_CLOUD_SCOPE} records will not be
     * deleted as this is an expensive operation requiring checks for other foreign key references. While
     * MySQL has the IGNORE modifier to allow for a delete query in which constraint violations are ignored,
     * this is not part of the SQL standard. Therefore, any delete query must contain only unreferenced
     * rows within {@link Tables#ENTITY_CLOUD_SCOPE}.
     *
     * @param filter The filter used to scope the {@link EntityComputeTierAllocation} records to delete.
     * @return The number of {@link Tables#ENTITY_COMPUTE_TIER_ALLOCATION} rows deleted.
     */
    @Override
    public int deleteAllocations(@Nonnull final EntityComputeTierAllocationFilter filter) {


        // jOOQ currently does not support a delete from a single table with a join from another.
        // Therefore, we first query with a join on the cloud scope store to correctly filter the
        // records. See https://github.com/jOOQ/jOOQ/issues/3266 for support in jOOQ.
        final Result<Record2<Long, LocalDateTime>> recordsToDelete =
                dslContext.select(Tables.ENTITY_COMPUTE_TIER_ALLOCATION.ENTITY_OID, Tables.ENTITY_COMPUTE_TIER_ALLOCATION.START_TIME)
                        .from(Tables.ENTITY_COMPUTE_TIER_ALLOCATION)
                        .leftJoin(Tables.ENTITY_CLOUD_SCOPE)
                        .onKey()
                        .where(generateConditionsFromFilter(filter))
                        .fetch();

        dslContext.delete(Tables.ENTITY_COMPUTE_TIER_ALLOCATION)
                .where(DSL.row(Tables.ENTITY_COMPUTE_TIER_ALLOCATION.ENTITY_OID, Tables.ENTITY_COMPUTE_TIER_ALLOCATION.START_TIME)
                        .in(recordsToDelete))
                .execute();

        return recordsToDelete.size();

    }

    /**
     * Stream records that are exactly equal to the target {@code endTime}. This is usefuly in querying
     * for records from a prior topology, where it is expected the end time of the records will match
     * the creation time of the prior topology.
     *
     * @param endTime The target end time in UTC.
     * @return A {@link Stream} containing the {@link EntityComputeTierAllocationRecord} instances with
     * an end time matching {@code endTime}.
     */
    @Nonnull
    private Stream<EntityComputeTierAllocationRecord> streamRecordsByEndTime(@Nonnull Instant endTime) {

        final LocalDateTime endDateTime = LocalDateTime.ofInstant(endTime, UTC_ZONE_ID);

        return dslContext.select(Tables.ENTITY_COMPUTE_TIER_ALLOCATION.asterisk())
                .from(Tables.ENTITY_COMPUTE_TIER_ALLOCATION)
                .where(Tables.ENTITY_COMPUTE_TIER_ALLOCATION.END_TIME.eq(endDateTime))
                .fetchStreamInto(EntityComputeTierAllocationRecord.class);

    }

    private void insertAllocations(@Nonnull Set<ComputeTierAllocationDatapoint> allocationDatapoints,
                                        @Nonnull LocalDateTime recordTimestamp) {

        Set<EntityCloudScopeRecord> cloudScopeRecords = allocationDatapoints.stream()
                .map(allocation -> createCloudScopeRecord(
                        allocation.entityOid(),
                        allocation.accountOid(),
                        allocation.regionOid(),
                        allocation.availabilityZoneOid(),
                        allocation.serviceProviderOid()))
                .collect(ImmutableSet.toImmutableSet());

        try {

            insertCloudScopeRecords(cloudScopeRecords);

            Set<EntityComputeTierAllocationRecord> allocationRecords = allocationDatapoints.stream()
                    .map(allocation -> createAllocationRecord(allocation, recordTimestamp))
                    .collect(ImmutableSet.toImmutableSet());

            dslContext.batchInsert(allocationRecords).execute();
        } catch (IOException e) {
            logger.error("Error inserting records into entity cloud scope table", e);
        }
    }

    /**
     * Extends any previously stored records matching the data points within {@code allocationsByEntityOid}.
     * First, the previous topology is queried from {@link TopologyInfoTracker}. If no prior topology can
     * be found, this method returns without extending any records.
     *
     * <p>If the prior topology is found, a query is performed for records in which the end time (last recorded
     * time) matches the prior topology's creation time. For any record from the prior topology, if the entity
     * exists in the current set of allocations to store (in {@code allocationsByEntityOid}) and the compute
     * tier demand remains unchanged, the record is "extended", meaning its end time is updated to the
     * creation time of {@code topologyInfo}.
     *
     * @param topologyInfo The {@link TopologyInfo} associated with the {@code allocationsByEntityOid}.
     * @param allocationsByEntityOid The allocation datapoints to store, indexed by the associated entity's OID.
     * @return An unmodifiable set containing the entity OIDs for records which were extended. If a prior
     * topology can not be found for {@code topologyInfo} through the {@link TopologyInfoTracker}, an
     * empty set is returned.
     */
    private Set<Long> extendEntityRecords(@Nonnull TopologyInfo topologyInfo,
                                          @Nonnull Map<Long, ComputeTierAllocationDatapoint> allocationsByEntityOid) {

        try (DataMetricTimer extensionTimer = EXTENSION_DURATION_SUMMARY_METRIC.startTimer()) {

            final Instant topologyCreationTime = Instant.ofEpochMilli(topologyInfo.getCreationTime());
            final Optional<TopologyInfo> previousTopologyInfo = topologyInfoTracker.getPriorTopologyInfo(topologyInfo);

            if (previousTopologyInfo.isPresent()) {

                final Instant previousCreationTime = Instant.ofEpochMilli(previousTopologyInfo.get().getCreationTime());
                final LocalDateTime creationTimestamp = LocalDateTime.ofInstant(topologyCreationTime, UTC_ZONE_ID);

                final Stream<EntityComputeTierAllocationRecord> updatedRecordsStream = streamRecordsByEndTime(previousCreationTime)
                        .map(entityAllocationRecord -> extendAllocationRecord(
                                entityAllocationRecord,
                                allocationsByEntityOid,
                                creationTimestamp))
                        .filter(Optional::isPresent)
                        .map(Optional::get);

                final Set<Long> updatedEntityOids = new HashSet<>();
                Iterators.partition(updatedRecordsStream.iterator(), batchExtensionSize)
                        .forEachRemaining(allocationRecordsBatch -> {
                            try {
                                dslContext.batchUpdate(allocationRecordsBatch).execute();
                                allocationRecordsBatch.forEach(r -> updatedEntityOids.add(r.getEntityOid()));
                            } catch (Exception e) {
                                logger.error("Error extending entity allocation records", e);
                            }

                        });

                return Collections.unmodifiableSet(updatedEntityOids);
            } else {

                logger.warn("Unable to find previous topology in extending allocation records "
                        + "(Context ID={}, Topology ID={}, Type={}, Creation Time={})",
                        topologyInfo.getTopologyContextId(),
                        topologyInfo.getTopologyId(),
                        topologyInfo.getTopologyType(),
                        topologyCreationTime);

                return Collections.EMPTY_SET;
            }
        }

    }

    /**
     * Extends a target {@code allocationRecord}, if the record is contained within {@code allocationsByEntityOid}.
     * The compute tier demand between the record and data point must match for a record to be extended.
     * The {@link EntityComputeTierAllocationRecord#getEndTime()} attribute of {@codeallocationRecord}
     * will be updated to {@code topologyCreationTime}, if a match is found.
     *
     * <p>Note: The {@code allocationRecord} is updated, instead of creating a new record, in order to optimize
     * the update query. Updating a record already attached to a context should allow jOOQ to create an UPDATE
     * query for only the updated attribute (and not the entire record).
     *
     * @param record The {@link EntityComputeTierAllocationRecord} to extend
     * @param allocationsByEntityOid The set of allocation datapoints, in which a matching allocation to
     *                               the target {@code allocationRecord} will be matched against.
     * @param topologyCreationTime The topology creation time associated with the allocation datapoints.
     *                             This timestamp will be used to extend the {@code allocationRecord}, if
     *                             a match is found.
     * @return The {@code allocationRecord} with an updated end time, if a match is found. If no match is
     * found and the record cannot be extended, {@link Optional#empty()} will be returned.
     */
    private Optional<EntityComputeTierAllocationRecord> extendAllocationRecord(
            @Nonnull EntityComputeTierAllocationRecord record,
            @Nonnull Map<Long, ComputeTierAllocationDatapoint> allocationsByEntityOid,
            @Nonnull LocalDateTime topologyCreationTime) {

        if (allocationsByEntityOid.containsKey(record.getEntityOid())) {
            final ComputeTierAllocationDatapoint allocationDatapoint = allocationsByEntityOid.get(
                    record.getEntityOid());

            final ComputeTierDemand computeTierDemand = allocationDatapoint.cloudTierDemand();

            if (computeTierDemand.osType().equals(OSType.forNumber(record.getOsType()))
                    && computeTierDemand.tenancy().equals(Tenancy.forNumber(record.getTenancy()))
                    && computeTierDemand.cloudTierOid() == record.getAllocatedComputeTierOid()) {

                record.setEndTime(topologyCreationTime);

                return Optional.of(record);
            }
        }

        return Optional.empty();
    }

    private Set<Condition> generateConditionsFromFilter(@Nonnull EntityComputeTierAllocationFilter filter) {

        final Set<Condition> conditions = new HashSet<>();

        filter.startTimeFilter().ifPresent(startTimeFilter ->
                conditions.add(
                        generateTimestampCondition(
                                Tables.ENTITY_COMPUTE_TIER_ALLOCATION.START_TIME,
                                startTimeFilter)));

        filter.endTimeFilter().ifPresent(endTimeFilter ->
                conditions.add(
                        generateTimestampCondition(
                                Tables.ENTITY_COMPUTE_TIER_ALLOCATION.END_TIME,
                                endTimeFilter)));

        if (!filter.entityOids().isEmpty()) {
            conditions.add(Tables.ENTITY_COMPUTE_TIER_ALLOCATION.ENTITY_OID.in(filter.entityOids()));
        }

        if (!filter.accountOids().isEmpty()) {
            conditions.add(Tables.ENTITY_CLOUD_SCOPE.ACCOUNT_OID.in(filter.accountOids()));
        }

        if (!filter.regionOids().isEmpty()) {
            conditions.add(Tables.ENTITY_CLOUD_SCOPE.REGION_OID.in(filter.regionOids()));
        }

        if (!filter.serviceProviderOids().isEmpty()) {
            conditions.add(Tables.ENTITY_CLOUD_SCOPE.SERVICE_PROVIDER_OID.in(filter.serviceProviderOids()));
        }

        if (!filter.platforms().isEmpty()) {
            conditions.add(Tables.ENTITY_COMPUTE_TIER_ALLOCATION.OS_TYPE.in(
                    filter.platforms()
                            .stream()
                            .map(OSType::getNumber)
                            .collect(Collectors.toSet())));
        }

        if (!filter.tenancies().isEmpty()) {
            conditions.add(Tables.ENTITY_COMPUTE_TIER_ALLOCATION.TENANCY.in(
                    filter.tenancies()
                            .stream()
                            .map(Tenancy::getNumber)
                            .collect(Collectors.toSet())));
        }

        if (!filter.computeTierOids().isEmpty()) {
            conditions.add(Tables.ENTITY_COMPUTE_TIER_ALLOCATION.ALLOCATED_COMPUTE_TIER_OID.in(
                    filter.computeTierOids()));
        }

        return conditions;
    }

    @Nonnull
    private Condition generateTimestampCondition(@Nonnull TableField<?, LocalDateTime> field,
                                                 @Nonnull TimeFilter timeFilter) {

        final LocalDateTime time = LocalDateTime.ofInstant(timeFilter.time(), UTC_ZONE_ID);


        final Condition timeCondition;
        switch (timeFilter.comparator()) {

            case BEFORE:
                timeCondition = field.le(time);
                break;
            case BEFORE_OR_EQUAL_TO:
                timeCondition = field.lessOrEqual(time);
                break;
            case EQUAL_TO:
                timeCondition = field.eq(time);
                break;
            case AFTER:
                timeCondition = field.ge(time);
                break;
            case AFTER_OR_EQUAL_TO:
                timeCondition = field.greaterOrEqual(time);
                break;
            default:
                throw new UnsupportedOperationException();

        }

        return timeCondition;
    }

    @Nonnull
    private EntityComputeTierAllocation createAllocationFromRecord(@Nonnull Record record) {

        final EntityComputeTierAllocationRecord allocationRecord = record.into(EntityComputeTierAllocationRecord.class);
        final EntityCloudScopeRecord entityCloudScopeRecord = record.into(EntityCloudScopeRecord.class);

        return ImmutableEntityComputeTierAllocation.builder()
                .timeInterval(ImmutableTimeInterval.builder()
                        .startTime(allocationRecord.getStartTime().toInstant(ZoneOffset.UTC))
                        .endTime(allocationRecord.getEndTime().toInstant(ZoneOffset.UTC))
                        .build())
                .entityOid(allocationRecord.getEntityOid())
                .accountOid(entityCloudScopeRecord.getAccountOid())
                .regionOid(entityCloudScopeRecord.getRegionOid())
                .availabilityZoneOid(Optional.ofNullable(entityCloudScopeRecord.getAvailabilityZoneOid()))
                .serviceProviderOid(entityCloudScopeRecord.getServiceProviderOid())
                .cloudTierDemand(ImmutableComputeTierDemand.builder()
                        .cloudTierOid(allocationRecord.getAllocatedComputeTierOid())
                        .osType(OSType.forNumber(allocationRecord.getOsType()))
                        .tenancy(Tenancy.forNumber(allocationRecord.getTenancy()))
                        .build())
                .build();
    }


    @Nonnull
    private EntityComputeTierAllocationRecord createAllocationRecord(@Nonnull ComputeTierAllocationDatapoint allocation,
                                                                   @Nonnull LocalDateTime timestamp) {

        final EntityComputeTierAllocationRecord allocationRecord = new EntityComputeTierAllocationRecord();

        allocationRecord.setStartTime(timestamp);
        allocationRecord.setEndTime(timestamp);
        allocationRecord.setEntityOid(allocation.entityOid());
        allocationRecord.setOsType(allocation.cloudTierDemand().osType().getNumber());
        allocationRecord.setTenancy(allocation.cloudTierDemand().tenancy().getNumber());
        allocationRecord.setAllocatedComputeTierOid(allocation.cloudTierDemand().cloudTierOid());

        return allocationRecord;
    }

    private String fetchDiagsForExport() {
        return dslContext.select(DSL.asterisk())
                .from(Tables.ENTITY_COMPUTE_TIER_ALLOCATION)
                .fetch().formatJSON();
    }

    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags, @Nullable Void context) throws DiagnosticsException {
        //TODO To be implemented as a part of OM-58627
        return;
    }

    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
        String records = fetchDiagsForExport();
        appender.appendString(records);

    }

    @Nonnull
    @Override
    public String getFileName() {
        return entityTierDemandDumpFile;
    }
}
