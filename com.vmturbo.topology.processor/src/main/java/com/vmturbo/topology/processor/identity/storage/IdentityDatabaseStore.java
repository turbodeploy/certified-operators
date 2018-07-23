package com.vmturbo.topology.processor.identity.storage;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.topology.processor.db.tables.AssignedIdentity;

/**
 * Responsible for mediating between the {@link IdentityServiceInMemoryUnderlyingStore} and
 * the database.
 */
public class IdentityDatabaseStore {

    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    public IdentityDatabaseStore(@Nonnull final DSLContext dsl) {
        this.dsl = dsl;
    }

    void saveDescriptors(@Nonnull final Collection<EntityInMemoryProxyDescriptor> descriptors)
            throws IdentityDatabaseException {
        try {
            dsl.transaction(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);
                descriptors.forEach(descriptor -> insert(transactionDsl, descriptor));
            });
        } catch (DataAccessException e) {
            throw new IdentityDatabaseException(e);
        }
    }

    private void insert(@Nonnull final DSLContext context,
                        @Nonnull final EntityInMemoryProxyDescriptor descriptor) {
        context.insertInto(AssignedIdentity.ASSIGNED_IDENTITY)
                .columns(AssignedIdentity.ASSIGNED_IDENTITY.ID, AssignedIdentity.ASSIGNED_IDENTITY.PROPERTIES)
                .values(descriptor.getOID(), descriptor)
                .onDuplicateKeyUpdate()
                .set(AssignedIdentity.ASSIGNED_IDENTITY.PROPERTIES, descriptor)
                .execute();
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
    Set<EntityInMemoryProxyDescriptor> getDescriptors() throws IdentityDatabaseException {
        try {
            final Result<Record1<EntityInMemoryProxyDescriptor>> result =
                    dsl.select(AssignedIdentity.ASSIGNED_IDENTITY.PROPERTIES)
                            .from(AssignedIdentity.ASSIGNED_IDENTITY)
                            .fetch();
            return result.stream()
                    .map(record -> record.get(AssignedIdentity.ASSIGNED_IDENTITY.PROPERTIES))
                    .collect(Collectors.toSet());
        } catch (DataAccessException e) {
            throw new IdentityDatabaseException(e);
        }
    }
}
