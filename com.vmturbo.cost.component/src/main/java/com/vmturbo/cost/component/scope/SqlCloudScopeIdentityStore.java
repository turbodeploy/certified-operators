package com.vmturbo.cost.component.scope;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Query;

import com.vmturbo.cloud.common.scope.CloudScopeIdentity;
import com.vmturbo.cloud.common.scope.CloudScopeIdentity.CloudScopeType;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityStore;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.CloudScopeRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.jooq.filter.JooqFilterMapper;

/**
 * SQL implementation of {@link CloudScopeIdentityStore}.
 */
public class SqlCloudScopeIdentityStore implements CloudScopeIdentityStore {

    private final Logger logger = LogManager.getLogger();

    private final JooqFilterMapper<CloudScopeIdentityFilter> filterMapper = JooqFilterMapper.<CloudScopeIdentityFilter>builder()
            .addInCollection(CloudScopeIdentityFilter::scopeIds, Tables.CLOUD_SCOPE.SCOPE_ID)
            .addEnumInShortCollection(CloudScopeIdentityFilter::scopeTypes, Tables.CLOUD_SCOPE.SCOPE_TYPE)
            .addInCollection(CloudScopeIdentityFilter::accountIds, Tables.CLOUD_SCOPE.ACCOUNT_ID)
            .addInCollection(CloudScopeIdentityFilter::regionIds, Tables.CLOUD_SCOPE.REGION_ID)
            .addInCollection(CloudScopeIdentityFilter::cloudServiceIds, Tables.CLOUD_SCOPE.CLOUD_SERVICE_ID)
            .addInCollection(CloudScopeIdentityFilter::resourceGroupIds, Tables.CLOUD_SCOPE.RESOURCE_GROUP_ID)
            .addInCollection(CloudScopeIdentityFilter::serviceProviderIds, Tables.CLOUD_SCOPE.SERVICE_PROVIDER_ID)
            .build();

    private final DSLContext dslContext;

    private final int batchStoreSize;

    /**
     * Constructs a new {@link SqlCloudScopeIdentityStore} instance.
     * @param dslContext The DSL context.
     * @param batchStoreSize The batch size for persisting scope identities.
     */
    public SqlCloudScopeIdentityStore(@Nonnull DSLContext dslContext,
                                      int batchStoreSize) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.batchStoreSize = batchStoreSize;
    }


    @Override
    public void saveScopeIdentities(@Nonnull List<CloudScopeIdentity> scopeIdentityList) {

        final Stopwatch stopwatch = Stopwatch.createStarted();

        Iterables.partition(scopeIdentityList, batchStoreSize).forEach(scopeIdentitiesBatch -> {

            final List<Query> recordInsertList = scopeIdentitiesBatch.stream()
                    .map(this::createRecordFromIdentity)
                    .map(cloudScopeRecord -> dslContext.insertInto(Tables.CLOUD_SCOPE)
                            .set(cloudScopeRecord)
                            .onDuplicateKeyUpdate()
                            .setNull(Tables.CLOUD_SCOPE.UPDATE_TS)
                            // Update all other records
                            .set(cloudScopeRecord))
                    .collect(ImmutableList.toImmutableList());

            dslContext.batch(recordInsertList).execute();
        });

        logger.info("Persisted {} cloud scope records in {}", scopeIdentityList.size(), stopwatch);
    }

    @Override
    public List<CloudScopeIdentity> getIdentitiesByFilter(@Nonnull CloudScopeIdentityFilter scopeIdentityFilter) {

        Preconditions.checkNotNull(scopeIdentityFilter, "Filter cannot be null");

        try (Stream<CloudScopeRecord> recordStream = dslContext.selectFrom(Tables.CLOUD_SCOPE)
                .where(filterMapper.generateConditions(scopeIdentityFilter))
                .stream()) {

            return recordStream.map(this::createIdentityFromRecord)
                    .collect(ImmutableList.toImmutableList());
        }
    }

    private CloudScopeRecord createRecordFromIdentity(@Nonnull CloudScopeIdentity scopeIdentity) {

        final CloudScopeRecord record = new CloudScopeRecord();
        record.setScopeId(scopeIdentity.scopeId());
        record.setScopeType((short)scopeIdentity.scopeType().ordinal());
        record.setResourceId(scopeIdentity.resourceId());
        record.setResourceType(scopeIdentity.hasResourceInfo() ? (short)scopeIdentity.resourceType().ordinal() : null);
        record.setAccountId(scopeIdentity.accountId());
        record.setRegionId(scopeIdentity.regionId());
        record.setCloudServiceId(scopeIdentity.cloudServiceId());
        record.setAvailabilityZoneId(scopeIdentity.zoneId());
        record.setResourceGroupId(scopeIdentity.resourceGroupId());
        record.setServiceProviderId(scopeIdentity.serviceProviderId());

        return record;
    }

    private CloudScopeIdentity createIdentityFromRecord(@Nonnull CloudScopeRecord scopeRecord) {
        return CloudScopeIdentity.builder()
                .scopeId(scopeRecord.getScopeId())
                .scopeType(CloudScopeType.values()[scopeRecord.getScopeType()])
                .resourceId(scopeRecord.getResourceId())
                .resourceType(scopeRecord.getResourceType() != null
                        ? EntityType.forNumber(scopeRecord.getResourceType())
                        : null)
                .accountId(scopeRecord.getAccountId())
                .regionId(scopeRecord.getRegionId())
                .cloudServiceId(scopeRecord.getCloudServiceId())
                .zoneId(scopeRecord.getAvailabilityZoneId())
                .resourceGroupId(scopeRecord.getResourceGroupId())
                .serviceProviderId(scopeRecord.getServiceProviderId())
                .build();
    }
}
