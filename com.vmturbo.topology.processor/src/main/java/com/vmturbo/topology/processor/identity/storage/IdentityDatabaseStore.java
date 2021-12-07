package com.vmturbo.topology.processor.identity.storage;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.db.tables.AssignedIdentity;

/**
 * Responsible for mediating between the {@link IdentityServiceInMemoryUnderlyingStore} and
 * the database.
 */
public class IdentityDatabaseStore {
    /**
     * Batch size for records insertion in the database.
     */
    private static final int BATCH_SIZE = 100;

    private final Logger logger = LogManager.getLogger(IdentityDatabaseStore.class);

    private final DSLContext dsl;

    public IdentityDatabaseStore(@Nonnull final DSLContext dsl) {
        this.dsl = dsl;
    }

    void saveDescriptors(@Nonnull final Collection<IdentityRecord> records)
            throws IdentityDatabaseException {
        try {
            dsl.transaction(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);
                insert(transactionDsl, records);
            });
        } catch (DataAccessException e) {
            throw new IdentityDatabaseException(e);
        }
    }

    private void insert(@Nonnull final DSLContext context,
            @Nonnull final Collection<IdentityRecord> records) {
        Iterators.partition(records.iterator(), BATCH_SIZE).forEachRemaining( batch -> {
            for (IdentityRecord record: batch) {
                Collection<Query> queries = new HashSet<>();
                EntityInMemoryProxyDescriptor descriptor = record.getDescriptor();
                Query query = context.insertInto(AssignedIdentity.ASSIGNED_IDENTITY)
                        .columns(AssignedIdentity.ASSIGNED_IDENTITY.ID, AssignedIdentity.ASSIGNED_IDENTITY.PROBE_ID,
                                AssignedIdentity.ASSIGNED_IDENTITY.PROPERTIES, AssignedIdentity.ASSIGNED_IDENTITY.ENTITY_TYPE)
                        .values(descriptor.getOID(), record.getProbeId(), descriptor,
                                record.getEntityType().getNumber())
                        .onDuplicateKeyUpdate()
                        .set(AssignedIdentity.ASSIGNED_IDENTITY.PROPERTIES, descriptor)
                        .set(AssignedIdentity.ASSIGNED_IDENTITY.ENTITY_TYPE,
                                record.getEntityType().getNumber());
                queries.add(query);
                context.batch(queries).execute();
            }
        });
    }

    void removeDescriptor(final long oid) throws IdentityDatabaseException {
        try {
            dsl.deleteFrom(AssignedIdentity.ASSIGNED_IDENTITY)
                    .where(AssignedIdentity.ASSIGNED_IDENTITY.ID.eq(oid)).execute();
        } catch (DataAccessException e) {
            throw new IdentityDatabaseException(e);
        }
    }

    @Nonnull
    Set<IdentityRecord> getDescriptors() throws IdentityDatabaseException {
        try {
            final Result<Record3<EntityInMemoryProxyDescriptor, Long, Integer>> result =
                    dsl.select(AssignedIdentity.ASSIGNED_IDENTITY.PROPERTIES,
                        AssignedIdentity.ASSIGNED_IDENTITY.PROBE_ID,
                        AssignedIdentity.ASSIGNED_IDENTITY.ENTITY_TYPE)
                            .from(AssignedIdentity.ASSIGNED_IDENTITY)
                        .where(AssignedIdentity.ASSIGNED_IDENTITY.EXPIRED.isFalse())
                        .fetch();
            return result.stream()
                .map(record -> new IdentityRecord(EntityType.forNumber(record.get(AssignedIdentity.ASSIGNED_IDENTITY.ENTITY_TYPE)),
                        record.get(AssignedIdentity.ASSIGNED_IDENTITY.PROPERTIES),
                            record.get(AssignedIdentity.ASSIGNED_IDENTITY.PROBE_ID)))
                .collect(Collectors.toSet());
        } catch (DataAccessException e) {
            throw new IdentityDatabaseException(e);
        }
    }
}
