package com.vmturbo.cost.component.entity.scope;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Record1;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;
import org.springframework.scheduling.TaskScheduler;

import com.vmturbo.cloud.common.entity.scope.EntityCloudScope;
import com.vmturbo.cloud.common.entity.scope.EntityCloudScopeStore;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudScopeFilter;
import com.vmturbo.cost.component.TableDiagsRestorable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.EntityCloudScopeRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * An implementation of {@link EntityCloudScopeStore}, backed by a SQL table.
 */
public class SQLEntityCloudScopeStore implements EntityCloudScopeStore,
        TableDiagsRestorable<Object, EntityCloudScopeRecord> {

    /**
     * A summary metric collecting the total duration in cleaning up cloud scope records.
     */
    private static final DataMetricSummary TOTAL_CLEANUP_DURATION_SUMMARY_METRIC =
            DataMetricSummary.builder()
                    .withName("cost_cloud_scope_cleanup_duration_seconds")
                    .withHelp("Total time to cleanup entity cloud scope records.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();


    private final Logger logger = LogManager.getLogger();

    private static final String sqlCloudScopeStoreDump = "sqlCloudScopeStore_dump";

    private final DSLContext dslContext;

    private final int batchCleanupSize;

    private final int batchFetchSize;

    /**
     * Original number of DB columns in cloud scope table, before 2 columns were added in 8.1.6.
     */
    private final int pre816ColumnCount = 6;

    /**
     * Creates a {@link SQLEntityCloudScopeStore} instance.
     * @param dslContext The {@link DSLContext} to use in querying and deleting cloud scope records.
     * @param taskScheduler A {@link TaskScheduler}, used to schedule invocations of {@link #cleanupCloudScopeRecords()}.
     * @param cleanupInterval The interval at which {@link #cleanupCloudScopeRecords()} should be invoked.
     * @param batchCleanupSize The number of records that should be deleted in a single SQL query during cleanup.
     */
    public SQLEntityCloudScopeStore(@Nonnull DSLContext dslContext,
                                    @Nonnull TaskScheduler taskScheduler,
                                    @Nonnull Duration cleanupInterval,
                                    int batchCleanupSize,
                                    int batchFetchSize) {

        Preconditions.checkArgument(batchCleanupSize >= 0, "Batch cleanup size must be >= 0");
        Preconditions.checkArgument(batchFetchSize >= 0, "Batch fetch size must be >= 0");

        this.dslContext = dslContext;
        this.batchCleanupSize = batchCleanupSize;
        this.batchFetchSize = batchFetchSize;

        taskScheduler.scheduleWithFixedDelay(this::cleanupCloudScopeRecords, cleanupInterval);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long cleanupCloudScopeRecords() {

        logger.info("Cleaning up cloud scope records");

        final MutableLong numRecordsDelete = new MutableLong(0);
        try (DataMetricTimer cleanupTimer = TOTAL_CLEANUP_DURATION_SUMMARY_METRIC.startTimer()) {
            SelectJoinStep<Record1<Long>> selectJoinStep = dslContext.select(Tables.ENTITY_CLOUD_SCOPE.ENTITY_OID)
                    .from(Tables.ENTITY_CLOUD_SCOPE);


            for (Table foreignKeyTable : resolveForeignKeyTables()) {

                logger.debug("Adding foreign key join on cloud scope cleanup to {}", foreignKeyTable.getName());
                selectJoinStep = selectJoinStep.leftAntiJoin(foreignKeyTable).using(Tables.ENTITY_CLOUD_SCOPE.ENTITY_OID);
            }

            try (Stream<Long> entityOidStream = selectJoinStep.stream().map(Record1::component1)) {
                Iterators.partition(entityOidStream.iterator(), batchCleanupSize).forEachRemaining(entityOidsToDelete -> {
                    try {
                        final int batchRecordsDelete = dslContext.deleteFrom(Tables.ENTITY_CLOUD_SCOPE)
                                .where(Tables.ENTITY_CLOUD_SCOPE.ENTITY_OID.in(entityOidsToDelete))
                                .execute();

                        logger.info("Delete batch of {} cloud scope records", batchRecordsDelete);
                        numRecordsDelete.add(batchRecordsDelete);
                    } catch (Exception e) {
                        logger.error("Error delete batch of cloud scope records", e);
                    }
                });
            }

            logger.info("Cleaned up {} entity cloud scope records in {}",
                    numRecordsDelete.longValue(),
                    Duration.ofSeconds((long)cleanupTimer.getTimeElapsedSecs()));
        } catch (Exception e) {
            logger.error("Error during entity cloud scope cleanup", e);
        }

        return numRecordsDelete.longValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void streamAll(@Nonnull Consumer<EntityCloudScope> consumer) {
        streamByFilter(null, consumer);
    }

    @Override
    public void streamByFilter(@Nullable final CloudScopeFilter filter,
            @Nonnull Consumer<EntityCloudScope> consumer) {
        dslContext.connection(conn -> {
            conn.setAutoCommit(false);
            try (Stream<EntityCloudScopeRecord> stream = DSL.using(conn, dslContext.settings())
                    .selectFrom(Tables.ENTITY_CLOUD_SCOPE)
                    .where(filter == null ? Collections.emptySet() : generateConditionsFromFilter(filter))
                    .fetchSize(batchFetchSize)
                    .stream()) {
                stream.map(this::convertRecordToImmutable).forEach(consumer);
            }
        });
    }

    private Set<Condition> generateConditionsFromFilter(@Nonnull CloudScopeFilter filter) {

        final Set<Condition> conditions = new HashSet<>();

        if (filter.hasEntityFilter() && filter.getEntityFilter().getEntityIdCount() > 0) {
            conditions.add(Tables.ENTITY_CLOUD_SCOPE.ENTITY_OID.in(filter.getEntityFilter().getEntityIdList()));
        }

        if (filter.hasAccountFilter() && filter.getAccountFilter().getAccountIdCount() > 0) {
            conditions.add(Tables.ENTITY_CLOUD_SCOPE.ACCOUNT_OID.in(filter.getAccountFilter().getAccountIdList()));
        }

        if (filter.hasRegionFilter() && filter.getRegionFilter().getRegionIdCount() > 0) {
            conditions.add(Tables.ENTITY_CLOUD_SCOPE.REGION_OID.in(filter.getRegionFilter().getRegionIdList()));
        }

        if (filter.hasServiceProviderFilter() && filter.getServiceProviderFilter().getServiceProviderIdCount() > 0) {
            conditions.add(Tables.ENTITY_CLOUD_SCOPE.SERVICE_PROVIDER_OID.in(filter.getServiceProviderFilter().getServiceProviderIdList()));
        }

        return conditions;
    }

    private EntityCloudScope convertRecordToImmutable(@Nonnull EntityCloudScopeRecord record) {

        return EntityCloudScope.builder()
                .entityOid(record.getEntityOid())
                .accountOid(record.getAccountOid())
                .regionOid(record.getRegionOid())
                .availabilityZoneOid(Optional.ofNullable(record.getAvailabilityZoneOid()))
                .serviceProviderOid(record.getServiceProviderOid())
                .creationTime(record.getCreationTime().toInstant(ZoneOffset.UTC))
                .build();
    }


    /**
     * Dynamically determines all jOOQ tables with references (foreign keys) to the {@link Tables#ENTITY_CLOUD_SCOPE}
     * table.
     *
     * @return The set of tables with a foreign key referencing the {@link Tables#ENTITY_CLOUD_SCOPE} table.
     */
    @Nonnull
    private Set<Table<?>> resolveForeignKeyTables() {

        return Tables.ENTITY_CLOUD_SCOPE.getSchema().tableStream()
                .filter(Predicates.not(Tables.ENTITY_CLOUD_SCOPE::equals))
                .map(t -> t.getReferencesTo(Tables.ENTITY_CLOUD_SCOPE))
                .flatMap(List::stream)
                .map(ForeignKey::getTable)
                .collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public DSLContext getDSLContext() {
        return dslContext;
    }

    @Override
    public TableImpl<EntityCloudScopeRecord> getTable() {
        return Tables.ENTITY_CLOUD_SCOPE;
    }

    @Nonnull
    @Override
    public String getFileName() {
        return sqlCloudScopeStoreDump;
    }

    /**
     * If we are trying to load a diagnostic that is before 8.1.6 version, then it only has 6
     * data fields (missing the entityType and resourceGroupOid fields), so use default values
     * for those 2 fields and insert them in the right position, before this data record could
     * be loaded into the DB (of latest version).
     *
     * @param data Input data line (in JSON format) fields, could get updated.
     * @param fields DB fields for the table to which record is to be inserted.
     */
    @Override
    public void preProcessJsonData(@Nonnull List<Object> data, @Nonnull final Field<?>[] fields) {
        if (data.size() == pre816ColumnCount) {
            // Entity type for all was VM, before the scope table was expanded to support VV etc.
            data.add(1, EntityType.VIRTUAL_MACHINE_VALUE);
            // We don't know ResourceGroup, so set it to null. Data size should be 8 after this.
            data.add(6, null);
        }
    }
}
