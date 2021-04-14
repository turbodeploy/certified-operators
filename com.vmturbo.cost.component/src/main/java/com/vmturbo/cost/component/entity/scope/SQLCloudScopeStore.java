package com.vmturbo.cost.component.entity.scope;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.ForeignKey;
import org.jooq.Record1;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.jooq.impl.TableImpl;
import org.springframework.scheduling.TaskScheduler;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import com.vmturbo.cloud.common.entity.scope.CloudScopeStore;
import com.vmturbo.cloud.common.entity.scope.EntityCloudScope;
import com.vmturbo.common.protobuf.cost.EntityUptime.CloudScopeFilter;
import com.vmturbo.cost.component.TableDiagsRestorable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.EntityCloudScopeRecord;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * An implementation of {@link CloudScopeStore}, backed by a SQL table.
 */
public class SQLCloudScopeStore implements CloudScopeStore,
        TableDiagsRestorable<Void, EntityCloudScopeRecord> {

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
     * Creates a {@link SQLCloudScopeStore} instance.
     * @param dslContext The {@link DSLContext} to use in querying and deleting cloud scope records.
     * @param taskScheduler A {@link TaskScheduler}, used to schedule invocations of {@link #cleanupCloudScopeRecords()}.
     * @param cleanupInterval The interval at which {@link #cleanupCloudScopeRecords()} should be invoked.
     * @param batchCleanupSize The number of records that should be deleted in a single SQL query during cleanup.
     */
    public SQLCloudScopeStore(@Nonnull DSLContext dslContext,
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

            final Iterator<Long> entityOidIterator = selectJoinStep.stream()
                    .map(Record1::component1)
                    .iterator();

            Iterators.partition(entityOidIterator, batchCleanupSize).forEachRemaining(entityOidsToDelete -> {
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
    public Stream<EntityCloudScope> streamAll() {
        return dslContext.selectFrom(Tables.ENTITY_CLOUD_SCOPE)
                .fetchSize(batchFetchSize)
                .stream()
                .map(this::convertRecordToImmutable);
    }

    @Override
    public Stream<EntityCloudScope> streamByFilter(@Nonnull final CloudScopeFilter filter) {
        return dslContext.selectFrom(Tables.ENTITY_CLOUD_SCOPE)
                .where(generateConditionsFromFilter(filter))
                .fetchSize(1000)
                .stream()
                .map(this::convertRecordToImmutable);
    }

    private Set<Condition> generateConditionsFromFilter(@Nonnull CloudScopeFilter filter) {

        final Set<Condition> conditions = new HashSet<>();

        if (!filter.getEntityOidList().isEmpty()) {
            conditions.add(Tables.ENTITY_CLOUD_SCOPE.ENTITY_OID.in(filter.getEntityOidList()));
        }

        if (!filter.getAccountOidList().isEmpty()) {
            conditions.add(Tables.ENTITY_CLOUD_SCOPE.ACCOUNT_OID.in(filter.getAccountOidList()));
        }

        if (!filter.getRegionOidList().isEmpty()) {
            conditions.add(Tables.ENTITY_CLOUD_SCOPE.REGION_OID.in(filter.getRegionOidList()));
        }

        if (!filter.getServiceProviderOidList().isEmpty()) {
            conditions.add(Tables.ENTITY_CLOUD_SCOPE.SERVICE_PROVIDER_OID.in(filter.getServiceProviderOidList()));
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
}
