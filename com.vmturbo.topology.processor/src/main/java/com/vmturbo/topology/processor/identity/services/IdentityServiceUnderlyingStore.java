package com.vmturbo.topology.processor.identity.services;

import java.io.Reader;
import java.io.Writer;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.identity.EntityDescriptor;
import com.vmturbo.topology.processor.identity.EntityMetadataDescriptor;
import com.vmturbo.topology.processor.identity.EntryData;
import com.vmturbo.topology.processor.identity.IdentityServiceStoreOperationException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadataStore;
import com.vmturbo.topology.processor.identity.storage.EntityInMemoryProxyDescriptor;

/**
 * The IdentityServiceUnderlyingStore implements the Identity Service underlying service.
 */
public interface IdentityServiceUnderlyingStore extends RequiresDataInitialization {

    /**
     * Performs the lookup by the identifying set of properties.
     *
     * @param metadataDescriptor The metadata descriptor.
     * @param properties         The identifying set of properties.
     * @return The OID if found, {@code null} if not.
     * @throws IdentityServiceStoreOperationException In case of an error looking up the OID.
     * @throws IdentityUninitializedException If the store is not initialized yet.
     */
    long lookupByIdentifyingSet(@Nonnull EntityMetadataDescriptor metadataDescriptor,
                                @Nonnull List<PropertyDescriptor> properties)
            throws IdentityServiceStoreOperationException, IdentityUninitializedException;

    /**
     * Adds the entry to the underlying store.
     *
     * @param oid                The object id.
     * @param descriptor         The descriptor.
     * @param metadataDescriptor The metadata descriptor.
     * @param entityType         The type of the entity.
     * @param probeId            The ID of the probe of the target which discovered the entity.
     * @throws IdentityServiceStoreOperationException In case of an error adding the Entity.
     * @throws IdentityUninitializedException If the store is not initialized yet.
     */
    void addEntry(final long oid,
                  @Nonnull final EntityDescriptor descriptor,
                  @Nonnull final EntityMetadataDescriptor metadataDescriptor,
                  @Nonnull final EntityType entityType,
                  final long probeId)
            throws IdentityServiceStoreOperationException, IdentityUninitializedException;

    /**
     * A bulk insert of entities into the store. The insert will happen atomically.
     *
     * @param entryMap The map from OID to the entity-specific data for that entry.
     * @throws IdentityServiceStoreOperationException In case of an operational error.
     * @throws IdentityUninitializedException If the store is not initialized yet.
     */
    void upsertEntries(@Nonnull final Map<Long, EntryData> entryMap)
            throws IdentityServiceStoreOperationException, IdentityUninitializedException;

    /**
     * Updates the entry to the underlying store.
     *
     * @param oid                The object id.
     * @param descriptor         The descriptor.
     * @param metadataDescriptor The metadata descriptor.
     * @param entityType         The type of the entity.
     * @param probeId            The ID of the probe of the target which discovered the entity.
     * @throws IdentityServiceStoreOperationException In case of an error updating the Entity.
     * @throws IdentityUninitializedException If the store is not initialized yet.
     */
    void updateEntry(final long oid,
                     @Nonnull final EntityDescriptor descriptor,
                     @Nonnull final EntityMetadataDescriptor metadataDescriptor,
                     @Nonnull final EntityType entityType,
                     final long probeId)
            throws IdentityServiceStoreOperationException, IdentityUninitializedException;

    /**
     * Remove the entry to the underlying store.
     *
     * @param oid The object id.
     * @return {@code true} iff the entity existed and has been removed.
     * @throws IdentityServiceStoreOperationException In case of an error removing the DTO.
     * @throws IdentityUninitializedException If the store is not initialized yet.
     */
    boolean removeEntry(long oid) throws IdentityServiceStoreOperationException, IdentityUninitializedException;

    /**
     * Performs the search using the set of properties.
     * The optional metadata descriptor will be used to narrow the search.
     *
     * @param properties The non volatile properties.
     * @throws IdentityUninitializedException If the store is not initialized yet.
     */
    @Nonnull
    List<EntityInMemoryProxyDescriptor> getDtosByNonVolatileProperties(
        @Nonnull List<PropertyDescriptor> properties)
            throws IdentityUninitializedException;

    /**
     * Checks whether entity with such OID is already present.
     *
     * @param oid The OID.
     * @return {@code true} iff Entity with such OID is present.
     * @throws IdentityServiceStoreOperationException In case of an issue querying.
     * @throws IdentityUninitializedException If the store is not initialized yet.
     */
    boolean containsOID(long oid) throws IdentityServiceStoreOperationException, IdentityUninitializedException;

    /**
     * Checks whether entity with such set of identifying properties is already present.
     *
     * @param metadataDescriptor The metadata descriptor.
     * @param properties         The set of identifying properties.
     * @return {@code true} iff Entity with such set of identifying properties is present.
     * @throws IdentityServiceStoreOperationException In case of an issue querying.
     * @throws IdentityUninitializedException If the store is not initialized yet.
     */
    boolean containsWithIdentifyingProperties(
            @Nonnull EntityMetadataDescriptor metadataDescriptor,
            @Nonnull List<PropertyDescriptor> properties)
            throws IdentityServiceStoreOperationException, IdentityUninitializedException;

    /**
     * Makes the current thread wait until the store gets initialized or a timeout occurs.
     * @throws InterruptedException if any thread interrupted the current thread before
     * or while the current thread was waiting for a notification.
     */
    void waitForInitializedStore() throws InterruptedException;

    /**
     * Write out the contents of the store to the provided writer.
     *
     * @param writer The writer to backup to.
     */
    void backup(@Nonnull final Writer writer);

    /**
     * Replace the contents of the store from a backup as written by a previous call to
     * {@link IdentityServiceUnderlyingStore#backup(Writer)}. All existing contents will be
     * removed.
     *
     * @param input The reader to back up from.
     * @param perProbeMetadata probe metadata to distinguish volatile and non-volatile properties
     */
    void restore(@Nonnull Reader input, @Nonnull Map<Long,
        ServiceEntityIdentityMetadataStore> perProbeMetadata);

    /**
     * Reload the entity descriptors from the underlying backing store.
     * If the store is a pass-through, then this will be a no-op. If the
     * store is an in-memory store backed by a persistent store, this call
     * updates the in-memory store with the latest values from the
     * persistent store.
     */
    void reloadEntityDescriptors();

    /**
     * Restore diags with the older format containing {@link EntityInMemoryProxyDescriptor}.
     * @param input The reader to back up from.
     */
    void restoreOldDiags(Reader input);
}
