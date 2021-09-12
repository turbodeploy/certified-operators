package com.vmturbo.cost.component.entity.scope;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.cloud.common.entity.scope.CloudScopeStore;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.EntityCloudScopeRecord;

/**
 * An abstract implementation of a SQL-backed store that is dependent on the {@link CloudScopeStore}.
 * As a SQL implementation, all subclasses extending {@link SQLCloudScopedStore} are expected to have
 * a foreign key relationship on the {@link com.vmturbo.cost.component.db.Tables#ENTITY_CLOUD_SCOPE}
 * table.
 */
public abstract class SQLCloudScopedStore implements CloudScopedStore {

    private final Logger logger = LogManager.getLogger();

    protected final DSLContext dslContext;

    protected final int batchInsertionSize;


    protected SQLCloudScopedStore(@Nonnull DSLContext dslContext,
                                  int batchInsertionSize) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.batchInsertionSize = batchInsertionSize;
    }


    protected EntityCloudScopeRecord createCloudScopeRecord(long entityOid,
                                                            int entityType,
                                                            long accountOid,
                                                            long regionOid,
                                                            Optional<Long> availabilityZoneOid,
                                                            long serviceProviderOid,
                                                            Optional<Long> resourceGroupOid,
                                                            @Nonnull LocalDateTime recordCreationTime) {

        final EntityCloudScopeRecord cloudScopeRecord = new EntityCloudScopeRecord();

        cloudScopeRecord.setEntityOid(entityOid);
        cloudScopeRecord.setEntityType(entityType);
        cloudScopeRecord.setAccountOid(accountOid);
        cloudScopeRecord.setRegionOid(regionOid);
        availabilityZoneOid.ifPresent(cloudScopeRecord::setAvailabilityZoneOid);
        cloudScopeRecord.setServiceProviderOid(serviceProviderOid);
        resourceGroupOid.ifPresent(cloudScopeRecord::setResourceGroupOid);
        cloudScopeRecord.setCreationTime(recordCreationTime);

        return cloudScopeRecord;
    }

    protected void insertCloudScopeRecords(
            @Nonnull Collection<EntityCloudScopeRecord> cloudScopeRecords) throws IOException {

        logger.info("Storing {} entity cloud scope records", cloudScopeRecords::size);

        final Stopwatch recordInsertionTimer = Stopwatch.createStarted();
        Iterables.partition(cloudScopeRecords, batchInsertionSize).forEach(scopeRecordsBatch ->
            dslContext.batch(scopeRecordsBatch.stream()
                    .map(record -> dslContext.insertInto(Tables.ENTITY_CLOUD_SCOPE)
                            .set(record)
                            .onDuplicateKeyUpdate()
                            .set(Tables.ENTITY_CLOUD_SCOPE.ENTITY_TYPE, record.getEntityType())
                            .set(Tables.ENTITY_CLOUD_SCOPE.ACCOUNT_OID, record.getAccountOid())
                            .set(Tables.ENTITY_CLOUD_SCOPE.REGION_OID, record.getRegionOid())
                            .set(Tables.ENTITY_CLOUD_SCOPE.AVAILABILITY_ZONE_OID, record.getAvailabilityZoneOid())
                            .set(Tables.ENTITY_CLOUD_SCOPE.SERVICE_PROVIDER_OID, record.getServiceProviderOid())
                            .set(Tables.ENTITY_CLOUD_SCOPE.RESOURCE_GROUP_OID, record.getResourceGroupOid()))
                    .collect(ImmutableSet.toImmutableSet())).execute());

        logger.info("Stored {} entity cloud scope records in {}", cloudScopeRecords::size, recordInsertionTimer::elapsed);
    }
}
