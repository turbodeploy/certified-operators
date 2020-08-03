package com.vmturbo.cost.component.entity.scope;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.ForeignKey;
import org.jooq.Record1;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.springframework.scheduling.TaskScheduler;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.gson.Gson;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.EntityCloudScopeRecord;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * An implementation of {@link CloudScopeStore}, backed by a SQL table.
 */
public class SQLCloudScopeStore implements CloudScopeStore {

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
                              int batchCleanupSize) {
        this.dslContext = dslContext;
        this.batchCleanupSize = batchCleanupSize;
        taskScheduler.scheduleWithFixedDelay(this::cleanupCloudScopeRecords, cleanupInterval);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long cleanupCloudScopeRecords() {

        final MutableLong numRecordsDelete = new MutableLong(0);

        try (DataMetricTimer cleanupTimer = TOTAL_CLEANUP_DURATION_SUMMARY_METRIC.startTimer()) {
            SelectJoinStep<Record1<Long>> selectJoinStep = dslContext.select(Tables.ENTITY_CLOUD_SCOPE.ENTITY_OID)
                    .from(Tables.ENTITY_CLOUD_SCOPE);


            for (Table foreignKeyTable : resolveForeignKeyTables()) {
                selectJoinStep = selectJoinStep.leftAntiJoin(foreignKeyTable).onKey();
            }


            final Iterator<Long> entityOidIterator = selectJoinStep.stream()
                    .map(Record1::component1)
                    .iterator();



            Iterators.partition(entityOidIterator, batchCleanupSize).forEachRemaining(entityOidsToDelete ->
                    numRecordsDelete.add(
                            dslContext.deleteFrom(Tables.ENTITY_CLOUD_SCOPE)
                                    .where(Tables.ENTITY_CLOUD_SCOPE.ENTITY_OID.in(entityOidsToDelete))
                                    .execute()));
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
                .stream()
                .map(this::convertRecordToImmutable);
    }

    private EntityCloudScope convertRecordToImmutable(@Nonnull EntityCloudScopeRecord record) {

        return ImmutableEntityCloudScope.builder()
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

    public List<EntityCloudScope> getDiagsForExport() {
        return dslContext.selectFrom(Tables.ENTITY_CLOUD_SCOPE)
                .fetch()
                .map(this::convertRecordToImmutable);
    }

    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException {
        //TODO To be implemented as a part of OM-58627
        return;
    }

    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
        List<EntityCloudScope> records = getDiagsForExport();
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        appender.appendString(gson.toJson(records));
    }

    @Nonnull
    @Override
    public String getFileName() {
        return sqlCloudScopeStoreDump;
    }
}
