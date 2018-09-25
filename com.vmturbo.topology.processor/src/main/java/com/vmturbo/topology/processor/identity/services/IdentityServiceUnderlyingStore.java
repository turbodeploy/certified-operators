package com.vmturbo.topology.processor.identity.services;

import java.io.Reader;
import java.io.Writer;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.topology.processor.identity.EntityDescriptor;
import com.vmturbo.topology.processor.identity.EntityMetadataDescriptor;
import com.vmturbo.topology.processor.identity.EntryData;
import com.vmturbo.topology.processor.identity.IdentityServiceStoreOperationException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;

/**
 * The IdentityServiceUnderlyingStore implements the Identity Service underlying service.
 */
public interface IdentityServiceUnderlyingStore {

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
     * @param probeId            The ID of the probe of the target which discovered the entity.
     * @throws IdentityServiceStoreOperationException In case of an error adding the Entity.
     * @throws IdentityUninitializedException If the store is not initialized yet.
     */
    void addEntry(final long oid,
                  @Nonnull final EntityDescriptor descriptor,
                  @Nonnull final EntityMetadataDescriptor metadataDescriptor,
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
     * @param probeId            The ID of the probe of the target which discovered the entity.
     * @throws IdentityServiceStoreOperationException In case of an error updating the Entity.
     * @throws IdentityUninitializedException If the store is not initialized yet.
     */
    void updateEntry(final long oid,
                     @Nonnull final EntityDescriptor descriptor,
                     @Nonnull final EntityMetadataDescriptor metadataDescriptor,
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
     * @param metadataDescriptor The metadata descriptor. if {@code null}, will be ignored.
     * @param properties         The set of properties.
     * @return The collection of identifying and heuristic properties. It is in the form of:
     * {@code Iterable<VMTEntityProxyDescriptor>}
     * @throws IdentityServiceStoreOperationException In case of an error querying the DTOs.
     * @throws IdentityUninitializedException If the store is not initialized yet.
     */
    @Nonnull
    Iterable<EntityProxyDescriptor> query(
            @Nullable EntityMetadataDescriptor metadataDescriptor,
            @Nonnull Iterable<PropertyDescriptor> properties)
            throws IdentityServiceStoreOperationException, IdentityUninitializedException;

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
     */
    void restore(@Nonnull final Reader input);
}
