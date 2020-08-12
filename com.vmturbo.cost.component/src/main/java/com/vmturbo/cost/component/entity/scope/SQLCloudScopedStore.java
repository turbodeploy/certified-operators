package com.vmturbo.cost.component.entity.scope;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

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


    protected SQLCloudScopedStore(@Nonnull DSLContext dslContext) {
        this.dslContext = Objects.requireNonNull(dslContext);
    }


    protected EntityCloudScopeRecord createCloudScopeRecord(long entityOid,
                                                            long accountOid,
                                                            long regionOid,
                                                            Optional<Long> availabilityZoneOid,
                                                            long serviceProviderOid) {

        final EntityCloudScopeRecord cloudScopeRecord = new EntityCloudScopeRecord();

        cloudScopeRecord.setEntityOid(entityOid);
        cloudScopeRecord.setAccountOid(accountOid);
        cloudScopeRecord.setRegionOid(regionOid);
        availabilityZoneOid.ifPresent(cloudScopeRecord::setAvailabilityZoneOid);
        cloudScopeRecord.setServiceProviderOid(serviceProviderOid);

        return cloudScopeRecord;
    }

    protected void insertCloudScopeRecords(
            @Nonnull Collection<EntityCloudScopeRecord> cloudScopeRecords) throws IOException {

        logger.info("Storing {} entity cloud scope records", cloudScopeRecords.size());

        dslContext.loadInto(Tables.ENTITY_CLOUD_SCOPE)
                .batchAll()
                .onDuplicateKeyUpdate()
                .loadRecords(cloudScopeRecords)
                .fields(Tables.ENTITY_CLOUD_SCOPE.ENTITY_OID,
                        Tables.ENTITY_CLOUD_SCOPE.ACCOUNT_OID,
                        Tables.ENTITY_CLOUD_SCOPE.REGION_OID,
                        Tables.ENTITY_CLOUD_SCOPE.AVAILABILITY_ZONE_OID,
                        Tables.ENTITY_CLOUD_SCOPE.SERVICE_PROVIDER_OID)
                .execute();
    }
}
